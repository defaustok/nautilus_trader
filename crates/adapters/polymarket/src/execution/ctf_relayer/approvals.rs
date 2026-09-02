// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::*;
use crate::execution::agent_lifecycle::{
    PolymarketLifecycleBackendError, PolymarketLifecycleReconciliation,
};

/// Exact phrase required before any approval request can be signed.
pub const APPROVAL_WRITE_CONFIRMATION: &str = "CONFIRM_POLYMARKET_SCOPED_APPROVALS";
const APPROVAL_WRITE_CONFIRMATION_ENV: &str = "POLYMARKET_APPROVAL_WRITE_CONFIRMATION";

/// A dry-run description of one narrowly scoped approval mutation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolymarketApprovalPlan {
    pub token: String,
    pub spender: String,
    pub approval_kind: String,
}

/// A signed approval batch retained inside the execution agent until consumed.
pub struct PreparedPolymarketApprovals {
    native_hash: String,
    request: DepositWalletBatchRequest,
    plan: Vec<PolymarketApprovalPlan>,
}

impl Debug for PreparedPolymarketApprovals {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedPolymarketApprovals")
            .field("native_hash", &self.native_hash)
            .field("request", &"<redacted>")
            .field("plan", &self.plan)
            .finish()
    }
}

impl PreparedPolymarketApprovals {
    #[must_use]
    pub fn native_hash(&self) -> &str {
        &self.native_hash
    }

    #[must_use]
    pub fn plan(&self) -> &[PolymarketApprovalPlan] {
        &self.plan
    }
}

/// Durable evidence from exactly one relayer approval submission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolymarketApprovalSubmission {
    pub native_hash: String,
    pub transaction_id: String,
    pub plan: Vec<PolymarketApprovalPlan>,
}

impl RelayerCtfTransport {
    /// Returns only missing approvals. This method is always read-only.
    pub async fn plan_scoped_approvals(
        &self,
    ) -> Result<Vec<PolymarketApprovalPlan>, PolymarketLifecycleBackendError> {
        self.ensure_wallet_lock()
            .map_err(|error| PolymarketLifecycleBackendError::NotReady(error.to_string()))?;
        self.ensure_wallet_deployed()
            .await
            .map_err(|error| PolymarketLifecycleBackendError::NotReady(error.to_string()))?;

        let mut plan = Vec::new();
        let mut seen = BTreeSet::new();
        for context in self.markets.values() {
            let adapter = adapter_for(context);
            let required = decimal_to_atomic(context.planned_split_quantity)
                .map_err(|error| PolymarketLifecycleBackendError::Rejected(error.to_string()))?;
            if self
                .pusd_allowance(adapter)
                .await
                .map_err(|error| PolymarketLifecycleBackendError::NotReady(error.to_string()))?
                < required
                && seen.insert((PUSD, adapter))
            {
                plan.push(PolymarketApprovalPlan {
                    token: format!("{PUSD:#x}"),
                    spender: format!("{adapter:#x}"),
                    approval_kind: "erc20_max_allowance".to_string(),
                });
            }
            for operator in [adapter, exchange_for(context)] {
                if !self
                    .ctf_approved(operator)
                    .await
                    .map_err(|error| PolymarketLifecycleBackendError::NotReady(error.to_string()))?
                    && seen.insert((CONDITIONAL_TOKENS, operator))
                {
                    plan.push(PolymarketApprovalPlan {
                        token: format!("{CONDITIONAL_TOKENS:#x}"),
                        spender: format!("{operator:#x}"),
                        approval_kind: "erc1155_operator".to_string(),
                    });
                }
            }
        }
        Ok(plan)
    }

    /// Rechecks and signs the current approval plan without sending it.
    ///
    /// `POLYMARKET_APPROVAL_WRITE_CONFIRMATION` must exactly match
    /// [`APPROVAL_WRITE_CONFIRMATION`]. A dry run calls [`Self::plan_scoped_approvals`] and never
    /// invokes this method.
    pub async fn prepare_scoped_approvals(
        &self,
    ) -> Result<PreparedPolymarketApprovals, PolymarketLifecycleBackendError> {
        let confirmation = env::var(APPROVAL_WRITE_CONFIRMATION_ENV).unwrap_or_default();
        self.prepare_scoped_approvals_confirmed(&confirmation).await
    }

    pub(super) async fn prepare_scoped_approvals_confirmed(
        &self,
        confirmation: &str,
    ) -> Result<PreparedPolymarketApprovals, PolymarketLifecycleBackendError> {
        if confirmation != APPROVAL_WRITE_CONFIRMATION {
            return Err(PolymarketLifecycleBackendError::Rejected(
                "scoped approval write confirmation is absent or incorrect".to_string(),
            ));
        }
        let plan = self.plan_scoped_approvals().await?;
        if plan.is_empty() {
            return Err(PolymarketLifecycleBackendError::Rejected(
                "all scoped approvals are already ready".to_string(),
            ));
        }
        let calls = approval_calls(&plan)?;
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
            .signed_batch_request_with_metadata(
                nonce,
                deadline,
                calls,
                "polymarket-scoped-approvals".to_string(),
            )
            .map_err(|error| PolymarketLifecycleBackendError::Rejected(error.to_string()))?;
        let encoded = serde_json::to_vec(&request)
            .map_err(|error| PolymarketLifecycleBackendError::Rejected(error.to_string()))?;
        Ok(PreparedPolymarketApprovals {
            native_hash: format!("{:#x}", keccak256(encoded)),
            request,
            plan,
        })
    }

    /// Consumes a prepared approval and performs exactly one relayer POST.
    pub async fn submit_prepared_approvals(
        &self,
        prepared: PreparedPolymarketApprovals,
    ) -> Result<PolymarketApprovalSubmission, PolymarketLifecycleBackendError> {
        let response = self
            .submit(&prepared.request)
            .await
            .map_err(agent::backend_error)?;
        Ok(PolymarketApprovalSubmission {
            native_hash: prepared.native_hash,
            transaction_id: response.transaction_id,
            plan: prepared.plan,
        })
    }

    /// Reconciles an approval transaction by durable relayer ID and never resubmits it.
    pub async fn reconcile_approval(
        &self,
        submission: &PolymarketApprovalSubmission,
    ) -> Result<PolymarketLifecycleReconciliation, PolymarketLifecycleBackendError> {
        match self
            .poll_once(&submission.transaction_id)
            .await
            .map_err(agent::backend_error)?
        {
            None => Ok(PolymarketLifecycleReconciliation::Pending),
            Some(transaction)
                if matches!(transaction.state.as_str(), "STATE_FAILED" | "STATE_INVALID") =>
            {
                Ok(PolymarketLifecycleReconciliation::Rejected {
                    reason: transaction
                        .error_msg
                        .unwrap_or_else(|| "relayer rejected approval transaction".to_string()),
                })
            }
            Some(transaction) => {
                let Some(hash) = transaction.transaction_hash else {
                    return Ok(PolymarketLifecycleReconciliation::Pending);
                };
                if self
                    .transaction_is_final(&hash)
                    .await
                    .map_err(agent::backend_error)?
                {
                    Ok(PolymarketLifecycleReconciliation::Confirmed {
                        transaction_hash: hash,
                    })
                } else {
                    Ok(PolymarketLifecycleReconciliation::Pending)
                }
            }
        }
    }
}

fn approval_calls(
    plan: &[PolymarketApprovalPlan],
) -> Result<Vec<Call>, PolymarketLifecycleBackendError> {
    plan.iter()
        .map(|item| {
            let token = Address::from_str(&item.token)
                .map_err(|error| PolymarketLifecycleBackendError::Rejected(error.to_string()))?;
            let spender = Address::from_str(&item.spender)
                .map_err(|error| PolymarketLifecycleBackendError::Rejected(error.to_string()))?;
            let data = match item.approval_kind.as_str() {
                "erc20_max_allowance" if token == PUSD => approveCall {
                    spender,
                    amount: U256::MAX,
                }
                .abi_encode(),
                "erc1155_operator" if token == CONDITIONAL_TOKENS => setApprovalForAllCall {
                    operator: spender,
                    approved: true,
                }
                .abi_encode(),
                _ => {
                    return Err(PolymarketLifecycleBackendError::Rejected(
                        "approval plan contains a non-scoped token or kind".to_string(),
                    ));
                }
            };
            Ok(Call {
                target: token,
                value: U256::ZERO,
                data: Bytes::from(data),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_scoped_approval_calls_are_pinned() {
        let plan = vec![
            PolymarketApprovalPlan {
                token: format!("{PUSD:#x}"),
                spender: format!("{CTF_COLLATERAL_ADAPTER:#x}"),
                approval_kind: "erc20_max_allowance".to_string(),
            },
            PolymarketApprovalPlan {
                token: format!("{CONDITIONAL_TOKENS:#x}"),
                spender: format!("{CTF_EXCHANGE:#x}"),
                approval_kind: "erc1155_operator".to_string(),
            },
        ];
        let calls = approval_calls(&plan).unwrap();

        assert_eq!(calls[0].target, PUSD);
        assert_eq!(calls[1].target, CONDITIONAL_TOKENS);
        assert_eq!(
            approveCall::abi_decode(&calls[0].data).unwrap().amount,
            U256::MAX
        );
        assert!(
            setApprovalForAllCall::abi_decode(&calls[1].data)
                .unwrap()
                .approved
        );
    }

    #[test]
    fn foreign_approval_kind_fails_closed() {
        let plan = [PolymarketApprovalPlan {
            token: format!("{PUSD:#x}"),
            spender: format!("{CTF_EXCHANGE:#x}"),
            approval_kind: "arbitrary".to_string(),
        }];
        assert!(approval_calls(&plan).is_err());
    }
}
