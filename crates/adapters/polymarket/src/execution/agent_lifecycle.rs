// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Provider-neutral facade contract for Deposit Wallet CTF lifecycle backends.

use std::{any::Any, fmt};

use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A native Conditional Token Framework operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum PolymarketLifecycleOperation {
    Split {
        idempotency_key: String,
        condition_id: String,
        quantity: Decimal,
        neg_risk: bool,
    },
    Merge {
        idempotency_key: String,
        condition_id: String,
        quantity: Decimal,
        neg_risk: bool,
    },
    Redeem {
        idempotency_key: String,
        condition_id: String,
        neg_risk: bool,
    },
}

/// One exact approval required by a native lifecycle backend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopedApprovalReadiness {
    pub token: String,
    pub spender: String,
    pub ready: bool,
}

/// Read-only lifecycle readiness evidence returned by the configured backend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolymarketLifecycleReadiness {
    pub chain_id: u64,
    pub signer_address: String,
    pub funder_address: String,
    pub wallet_deployed: bool,
    pub relayer_authenticated: bool,
    pub scoped_approvals: Vec<ScopedApprovalReadiness>,
}

/// Opaque signed relayer payload retained only inside the execution process.
pub struct BackendPreparedLifecycle {
    native_hash: String,
    payload: Box<dyn Any + Send + Sync>,
}

impl BackendPreparedLifecycle {
    /// Wraps a backend-specific signed payload and its safe native hash.
    #[must_use]
    pub fn new<T>(native_hash: String, payload: T) -> Self
    where
        T: Any + Send + Sync,
    {
        Self {
            native_hash,
            payload: Box::new(payload),
        }
    }

    /// Returns the safe native hash without exposing the signed payload.
    #[must_use]
    pub fn native_hash(&self) -> &str {
        &self.native_hash
    }

    /// Consumes and downcasts the opaque payload inside its originating backend.
    ///
    /// # Errors
    ///
    /// Returns the original wrapper when the requested backend type does not match.
    pub fn downcast<T>(self) -> Result<T, Self>
    where
        T: Any + Send + Sync,
    {
        let Self {
            native_hash,
            payload,
        } = self;
        match payload.downcast::<T>() {
            Ok(payload) => Ok(*payload),
            Err(payload) => Err(Self {
                native_hash,
                payload,
            }),
        }
    }
}

impl fmt::Debug for BackendPreparedLifecycle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackendPreparedLifecycle")
            .field("native_hash", &self.native_hash)
            .field("payload", &"<redacted>")
            .finish()
    }
}

/// Public prepared lifecycle request whose payload is neither cloneable nor serializable.
#[derive(Debug)]
pub struct PreparedPolymarketLifecycle {
    operation: PolymarketLifecycleOperation,
    backend: BackendPreparedLifecycle,
}

impl PreparedPolymarketLifecycle {
    /// Returns the typed lifecycle operation.
    #[must_use]
    pub const fn operation(&self) -> &PolymarketLifecycleOperation {
        &self.operation
    }

    /// Returns the safe native relayer hash.
    #[must_use]
    pub fn native_hash(&self) -> &str {
        self.backend.native_hash()
    }
}

/// Wallet balances required to verify a lifecycle operation after finality.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolymarketLifecycleBalances {
    pub collateral: Decimal,
    pub up: Decimal,
    pub down: Decimal,
}

/// Durable evidence returned after one relayer submission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolymarketLifecycleSubmission {
    pub native_hash: String,
    pub transaction_id: String,
    pub operation: PolymarketLifecycleOperation,
    pub balances_before: PolymarketLifecycleBalances,
}

/// Read-only reconciliation state for a previously accepted transaction ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PolymarketLifecycleReconciliation {
    Pending,
    Confirmed { transaction_hash: String },
    Rejected { reason: String },
    Unknown { reason: String },
}

/// Backend failure classified around the external mutation boundary.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PolymarketLifecycleBackendError {
    #[error("lifecycle preflight is not ready: {0}")]
    NotReady(String),
    #[error("lifecycle request was rejected: {0}")]
    Rejected(String),
    #[error("lifecycle submission outcome is unknown: {0}")]
    Unknown(String),
}

/// Facade-level lifecycle failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PolymarketLifecycleError {
    #[error("native Polymarket lifecycle backend is not configured")]
    Unsupported,
    #[error("native Polymarket lifecycle readiness failed: {0}")]
    NotReady(String),
    #[error("native Polymarket lifecycle request was rejected: {0}")]
    Rejected(String),
    #[error("native Polymarket lifecycle submission is UNKNOWN: {0}")]
    Unknown(String),
}

impl From<PolymarketLifecycleBackendError> for PolymarketLifecycleError {
    fn from(error: PolymarketLifecycleBackendError) -> Self {
        match error {
            PolymarketLifecycleBackendError::NotReady(reason) => Self::NotReady(reason),
            PolymarketLifecycleBackendError::Rejected(reason) => Self::Rejected(reason),
            PolymarketLifecycleBackendError::Unknown(reason) => Self::Unknown(reason),
        }
    }
}

/// Concrete relayer integration contract implemented outside the CLOB adapter.
#[async_trait]
pub trait PolymarketLifecycleBackend: fmt::Debug + Send + Sync {
    /// Returns current wallet deployment, identity, and scoped approval evidence.
    async fn readiness(
        &self,
    ) -> Result<PolymarketLifecycleReadiness, PolymarketLifecycleBackendError>;

    /// Performs read-only preflight and signs without crossing the Relayer POST boundary.
    async fn prepare(
        &self,
        operation: &PolymarketLifecycleOperation,
    ) -> Result<BackendPreparedLifecycle, PolymarketLifecycleBackendError>;

    /// Consumes a prepared payload and performs exactly one Relayer POST attempt.
    async fn submit_prepared(
        &self,
        prepared: BackendPreparedLifecycle,
    ) -> Result<PolymarketLifecycleSubmission, PolymarketLifecycleBackendError>;

    /// Reconciles durable accepted evidence without submitting another operation.
    async fn reconcile(
        &self,
        submission: &PolymarketLifecycleSubmission,
    ) -> Result<PolymarketLifecycleReconciliation, PolymarketLifecycleBackendError>;
}

/// Validates lifecycle readiness against the configured type-3 identity.
///
/// # Errors
///
/// Returns an error for wrong chain/account identity, absent deployment/auth, or any missing
/// scoped approval.
pub fn validate_lifecycle_readiness(
    readiness: &PolymarketLifecycleReadiness,
    signer_address: &str,
    funder_address: &str,
) -> Result<(), PolymarketLifecycleError> {
    if readiness.chain_id != 137 {
        return Err(PolymarketLifecycleError::NotReady(format!(
            "Polygon chain ID mismatch: expected 137, received {}",
            readiness.chain_id
        )));
    }
    if !readiness
        .signer_address
        .eq_ignore_ascii_case(signer_address)
        || !readiness
            .funder_address
            .eq_ignore_ascii_case(funder_address)
    {
        return Err(PolymarketLifecycleError::NotReady(
            "Deposit Wallet signer/funder identity mismatch".to_string(),
        ));
    }
    if !readiness.wallet_deployed {
        return Err(PolymarketLifecycleError::NotReady(
            "Deposit Wallet is not deployed".to_string(),
        ));
    }
    if !readiness.relayer_authenticated {
        return Err(PolymarketLifecycleError::NotReady(
            "Builder Relayer credentials are not ready".to_string(),
        ));
    }
    if readiness.scoped_approvals.is_empty()
        || readiness.scoped_approvals.iter().any(|approval| {
            approval.token.trim().is_empty()
                || approval.spender.trim().is_empty()
                || !approval.ready
        })
    {
        return Err(PolymarketLifecycleError::NotReady(
            "one or more scoped CTF approvals are not ready".to_string(),
        ));
    }
    Ok(())
}

/// Creates a prepared wrapper after backend preflight succeeds.
pub(crate) fn prepared_lifecycle(
    operation: PolymarketLifecycleOperation,
    backend: BackendPreparedLifecycle,
) -> PreparedPolymarketLifecycle {
    PreparedPolymarketLifecycle { operation, backend }
}

/// Splits a prepared wrapper for the consuming submit boundary.
pub(crate) fn into_backend_prepared(
    prepared: PreparedPolymarketLifecycle,
) -> BackendPreparedLifecycle {
    prepared.backend
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn ready() -> PolymarketLifecycleReadiness {
        PolymarketLifecycleReadiness {
            chain_id: 137,
            signer_address: "0x1111111111111111111111111111111111111111".to_string(),
            funder_address: "0x2222222222222222222222222222222222222222".to_string(),
            wallet_deployed: true,
            relayer_authenticated: true,
            scoped_approvals: vec![ScopedApprovalReadiness {
                token: "0x3333333333333333333333333333333333333333".to_string(),
                spender: "0x4444444444444444444444444444444444444444".to_string(),
                ready: true,
            }],
        }
    }

    #[rstest]
    fn lifecycle_readiness_accepts_exact_type3_identity() {
        assert_eq!(
            validate_lifecycle_readiness(
                &ready(),
                "0x1111111111111111111111111111111111111111",
                "0x2222222222222222222222222222222222222222",
            ),
            Ok(())
        );
    }

    #[rstest]
    fn lifecycle_readiness_rejects_incomplete_scoped_approval() {
        let mut readiness = ready();
        readiness.scoped_approvals[0].ready = false;
        assert!(matches!(
            validate_lifecycle_readiness(
                &readiness,
                "0x1111111111111111111111111111111111111111",
                "0x2222222222222222222222222222222222222222",
            ),
            Err(PolymarketLifecycleError::NotReady(_))
        ));
    }

    #[rstest]
    fn prepared_payload_debug_is_redacted() {
        let prepared = BackendPreparedLifecycle::new(
            "0xsafe-hash".to_string(),
            "signed-secret-payload".to_string(),
        );
        let debug = format!("{prepared:?}");
        assert!(debug.contains("0xsafe-hash"));
        assert!(!debug.contains("signed-secret-payload"));
    }
}
