use crate::execution::agent_lifecycle::{
    BackendPreparedLifecycle, PolymarketLifecycleBackend, PolymarketLifecycleBackendError,
    PolymarketLifecycleBalances, PolymarketLifecycleOperation, PolymarketLifecycleReadiness,
    PolymarketLifecycleReconciliation, PolymarketLifecycleSubmission, ScopedApprovalReadiness,
};
use async_trait::async_trait;
use nautilus_core::UnixNanos;

use super::*;

#[derive(Debug)]
struct PreparedRelayerLifecycle {
    operation: PolymarketLifecycleOperation,
    request: DepositWalletBatchRequest,
    balances_before: CtfBalances,
}

pub(super) fn backend_error(error: CtfTransportError) -> PolymarketLifecycleBackendError {
    match error {
        CtfTransportError::Retryable(reason) => PolymarketLifecycleBackendError::NotReady(reason),
        CtfTransportError::Failed(reason) => PolymarketLifecycleBackendError::Rejected(reason),
        CtfTransportError::Ambiguous(reason) => PolymarketLifecycleBackendError::Unknown(reason),
    }
}

fn command_from_operation(operation: &PolymarketLifecycleOperation) -> CtfCommand {
    let (idempotency_key, condition_id, operation) = match operation {
        PolymarketLifecycleOperation::Split {
            idempotency_key,
            condition_id,
            quantity,
            neg_risk,
        } => (
            idempotency_key,
            condition_id,
            CtfOperation::Split {
                condition_id: condition_id.clone(),
                quantity: *quantity,
                neg_risk: *neg_risk,
            },
        ),
        PolymarketLifecycleOperation::Merge {
            idempotency_key,
            condition_id,
            quantity,
            neg_risk,
        } => (
            idempotency_key,
            condition_id,
            CtfOperation::Merge {
                condition_id: condition_id.clone(),
                quantity: *quantity,
                neg_risk: *neg_risk,
            },
        ),
        PolymarketLifecycleOperation::Redeem {
            idempotency_key,
            condition_id,
            neg_risk,
        } => (
            idempotency_key,
            condition_id,
            CtfOperation::Redeem {
                condition_id: condition_id.clone(),
                neg_risk: *neg_risk,
            },
        ),
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
        });
    CtfCommand {
        command_id: idempotency_key.clone(),
        lifecycle_id: condition_id.clone(),
        operation,
        ts_event: UnixNanos::from(now),
        ts_init: UnixNanos::from(now),
    }
}

fn lifecycle_balances(balances: &CtfBalances) -> PolymarketLifecycleBalances {
    PolymarketLifecycleBalances {
        collateral: balances.pusd,
        up: balances.up,
        down: balances.down,
    }
}

fn ctf_balances(balances: &PolymarketLifecycleBalances) -> CtfBalances {
    CtfBalances {
        pusd: balances.collateral,
        up: balances.up,
        down: balances.down,
    }
}

impl RelayerCtfTransport {
    async fn prepare_for_agent(
        &self,
        operation: &PolymarketLifecycleOperation,
    ) -> Result<BackendPreparedLifecycle, PolymarketLifecycleBackendError> {
        self.ensure_wallet_lock()
            .map_err(|error| PolymarketLifecycleBackendError::NotReady(error.to_string()))?;
        let command = command_from_operation(operation);
        let context = self.context(&command).map_err(backend_error)?;
        if let CtfOperation::Split { quantity, .. } = &command.operation {
            let cap = self
                .config
                .required_wallet_cap()
                .map_err(|error| PolymarketLifecycleBackendError::NotReady(error.to_string()))?;
            if *quantity > cap {
                return Err(PolymarketLifecycleBackendError::Rejected(format!(
                    "split quantity {quantity} exceeds live wallet cap {cap}"
                )));
            }
        }
        self.ensure_wallet_deployed()
            .await
            .map_err(|error| PolymarketLifecycleBackendError::NotReady(error.to_string()))?;
        self.ensure_operator_approvals(context)
            .await
            .map_err(|error| PolymarketLifecycleBackendError::NotReady(error.to_string()))?;
        self.ensure_operation_funding(&command, context)
            .await
            .map_err(|error| PolymarketLifecycleBackendError::NotReady(error.to_string()))?;
        let balances_before = self
            .read_balances(context)
            .await
            .map_err(|error| PolymarketLifecycleBackendError::NotReady(error.to_string()))?;
        let call = self
            .operation_call(&command, context)
            .map_err(|error| PolymarketLifecycleBackendError::Rejected(error.to_string()))?;
        let nonce = self
            .relayer_nonce()
            .await
            .map_err(|error| PolymarketLifecycleBackendError::NotReady(error.to_string()))?;
        let deadline = unix_seconds()
            .and_then(|now| {
                now.checked_add(self.config.deadline.as_secs())
                    .ok_or_else(|| anyhow!("deadline overflow"))
            })
            .map_err(|error| PolymarketLifecycleBackendError::Rejected(error.to_string()))?;
        let request = self
            .signed_batch_request(&command, nonce, deadline, call)
            .map_err(|error| PolymarketLifecycleBackendError::Rejected(error.to_string()))?;
        if matches!(command.operation, CtfOperation::Split { .. }) {
            self.ensure_split_geo_allowed()
                .await
                .map_err(backend_error)?;
            self.ensure_wallet_scope_is_clean()
                .await
                .map_err(|error| PolymarketLifecycleBackendError::NotReady(error.to_string()))?;
        }
        let encoded = serde_json::to_vec(&request)
            .map_err(|error| PolymarketLifecycleBackendError::Rejected(error.to_string()))?;
        let native_hash = format!("{:#x}", keccak256(encoded));
        Ok(BackendPreparedLifecycle::new(
            native_hash,
            PreparedRelayerLifecycle {
                operation: operation.clone(),
                request,
                balances_before,
            },
        ))
    }
}

#[async_trait]
impl PolymarketLifecycleBackend for RelayerCtfTransport {
    async fn readiness(
        &self,
    ) -> Result<PolymarketLifecycleReadiness, PolymarketLifecycleBackendError> {
        self.preflight()
            .await
            .map_err(|error| PolymarketLifecycleBackendError::NotReady(error.to_string()))?;
        self.relayer_nonce()
            .await
            .map_err(|error| PolymarketLifecycleBackendError::NotReady(error.to_string()))?;
        let mut scoped_approvals = Vec::new();
        for context in self.markets.values() {
            for spender in [adapter_for(context), exchange_for(context)] {
                scoped_approvals.push(ScopedApprovalReadiness {
                    token: format!("{CONDITIONAL_TOKENS:#x}"),
                    spender: format!("{spender:#x}"),
                    ready: true,
                });
            }
            scoped_approvals.push(ScopedApprovalReadiness {
                token: format!("{PUSD:#x}"),
                spender: format!("{:#x}", adapter_for(context)),
                ready: true,
            });
        }
        Ok(PolymarketLifecycleReadiness {
            chain_id: POLYGON_CHAIN_ID,
            signer_address: format!("{:#x}", self.signer.address()),
            funder_address: format!("{:#x}", self.config.wallet_address),
            wallet_deployed: true,
            relayer_authenticated: true,
            scoped_approvals,
        })
    }

    async fn prepare(
        &self,
        operation: &PolymarketLifecycleOperation,
    ) -> Result<BackendPreparedLifecycle, PolymarketLifecycleBackendError> {
        self.prepare_for_agent(operation).await
    }

    async fn submit_prepared(
        &self,
        prepared: BackendPreparedLifecycle,
    ) -> Result<PolymarketLifecycleSubmission, PolymarketLifecycleBackendError> {
        let native_hash = prepared.native_hash().to_string();
        let prepared = prepared
            .downcast::<PreparedRelayerLifecycle>()
            .map_err(|_| {
                PolymarketLifecycleBackendError::Rejected(
                    "prepared lifecycle payload belongs to a different backend".to_string(),
                )
            })?;
        let response = self
            .submit(&prepared.request)
            .await
            .map_err(backend_error)?;
        Ok(PolymarketLifecycleSubmission {
            native_hash,
            transaction_id: response.transaction_id,
            operation: prepared.operation,
            balances_before: lifecycle_balances(&prepared.balances_before),
        })
    }

    async fn reconcile(
        &self,
        submission: &PolymarketLifecycleSubmission,
    ) -> Result<PolymarketLifecycleReconciliation, PolymarketLifecycleBackendError> {
        let command = command_from_operation(&submission.operation);
        match self
            .poll_async(
                &command,
                &submission.transaction_id,
                &ctf_balances(&submission.balances_before),
            )
            .await
        {
            Ok(CtfPollResult::Pending) => Ok(PolymarketLifecycleReconciliation::Pending),
            Ok(CtfPollResult::Completed(receipt)) => {
                Ok(PolymarketLifecycleReconciliation::Confirmed {
                    transaction_hash: receipt.tx_hash,
                })
            }
            Err(CtfTransportError::Failed(reason)) => {
                Ok(PolymarketLifecycleReconciliation::Rejected { reason })
            }
            Err(CtfTransportError::Ambiguous(reason)) => {
                Ok(PolymarketLifecycleReconciliation::Unknown { reason })
            }
            Err(error @ CtfTransportError::Retryable(_)) => Err(backend_error(error)),
        }
    }
}
