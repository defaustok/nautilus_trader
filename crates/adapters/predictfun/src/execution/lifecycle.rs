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
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
//  the specific language governing permissions and limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Predict.fun account readiness, scoped approvals and position lifecycle transactions.

use std::{collections::HashSet, fmt};

use alloy::{
    primitives::{Address, B256, Bytes, U256, address, keccak256},
    sol_types::SolValue,
};
use thiserror::Error;

use crate::common::{
    consts::{BNB_MAINNET_CHAIN_ID, BNB_TESTNET_CHAIN_ID},
    enums::{PredictFunEnvironment, PredictFunSide},
};

/// Exact opt-in required before an approval runner may send a transaction.
pub const APPROVAL_WRITE_CONFIRM_ENV: &str = "PREDICTFUN_APPROVAL_WRITE_CONFIRM";
pub const APPROVAL_WRITE_CONFIRM_VALUE: &str = "I_CONFIRM_PREDICTFUN_APPROVAL_WRITES";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PredictFunContractAddresses {
    pub yield_bearing_ctf_exchange: Address,
    pub yield_bearing_neg_risk_ctf_exchange: Address,
    pub yield_bearing_neg_risk_adapter: Address,
    pub yield_bearing_conditional_tokens: Address,
    pub yield_bearing_neg_risk_conditional_tokens: Address,
    pub ctf_exchange: Address,
    pub neg_risk_ctf_exchange: Address,
    pub neg_risk_adapter: Address,
    pub conditional_tokens: Address,
    pub neg_risk_conditional_tokens: Address,
    pub usdt: Address,
    pub kernel_implementation: Address,
    pub ecdsa_validator: Address,
}

#[must_use]
pub const fn contract_addresses(environment: PredictFunEnvironment) -> PredictFunContractAddresses {
    match environment {
        PredictFunEnvironment::Mainnet => PredictFunContractAddresses {
            yield_bearing_ctf_exchange: address!("6bEb5a40C032AFc305961162d8204CDA16DECFa5"),
            yield_bearing_neg_risk_ctf_exchange: address!(
                "8A289d458f5a134bA40015085A8F50Ffb681B41d"
            ),
            yield_bearing_neg_risk_adapter: address!("41dCe1A4B8FB5e6327701750aF6231B7CD0B2A40"),
            yield_bearing_conditional_tokens: address!("9400F8Ad57e9e0F352345935d6D3175975eb1d9F"),
            yield_bearing_neg_risk_conditional_tokens: address!(
                "F64b0b318AAf83BD9071110af24D24445719A07F"
            ),
            ctf_exchange: address!("8BC070BEdAB741406F4B1Eb65A72bee27894B689"),
            neg_risk_ctf_exchange: address!("365fb81bd4A24D6303cd2F19c349dE6894D8d58A"),
            neg_risk_adapter: address!("c3Cf7c252f65E0d8D88537dF96569AE94a7F1A6E"),
            conditional_tokens: address!("22DA1810B194ca018378464a58f6Ac2B10C9d244"),
            neg_risk_conditional_tokens: address!("22DA1810B194ca018378464a58f6Ac2B10C9d244"),
            usdt: address!("55d398326f99059fF775485246999027B3197955"),
            kernel_implementation: address!("BAC849bB641841b44E965fB01A4Bf5F074f84b4D"),
            ecdsa_validator: address!("845ADb2C711129d4f3966735eD98a9F09fC4cE57"),
        },
        PredictFunEnvironment::Testnet => PredictFunContractAddresses {
            yield_bearing_ctf_exchange: address!("8a6B4Fa700A1e310b106E7a48bAFa29111f66e89"),
            yield_bearing_neg_risk_ctf_exchange: address!(
                "95D5113bc50eD201e319101bbca3e0E250662fCC"
            ),
            yield_bearing_neg_risk_adapter: address!("b74aea04bdeBE912Aa425bC9173F9668e6f11F99"),
            yield_bearing_conditional_tokens: address!("38BF1cbD66d174bb5F3037d7068E708861D68D7f"),
            yield_bearing_neg_risk_conditional_tokens: address!(
                "26e865CbaAe99b62fbF9D18B55c25B5E079A93D5"
            ),
            ctf_exchange: address!("2A6413639BD3d73a20ed8C95F634Ce198ABbd2d7"),
            neg_risk_ctf_exchange: address!("d690b2bd441bE36431F6F6639D7Ad351e7B29680"),
            neg_risk_adapter: address!("285c1B939380B130D7EBd09467b93faD4BA623Ed"),
            conditional_tokens: address!("2827AAef52D71910E8FBad2FfeBC1B6C2DA37743"),
            neg_risk_conditional_tokens: address!("2827AAef52D71910E8FBad2FfeBC1B6C2DA37743"),
            usdt: address!("B32171ecD878607FFc4F8FC0bCcE6852BB3149E0"),
            kernel_implementation: address!("BAC849bB641841b44E965fB01A4Bf5F074f84b4D"),
            ecdsa_validator: address!("845ADb2C711129d4f3966735eD98a9F09fC4cE57"),
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PredictFunApprovalOperation {
    Trade,
    Split,
    Merge,
    Redeem,
    Convert,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PredictFunApprovalScope {
    pub operation: PredictFunApprovalOperation,
    pub is_neg_risk: bool,
    pub is_yield_bearing: bool,
    pub side: Option<PredictFunSide>,
    /// Exact minimum allowance required by this operation. Required for ERC-20 steps.
    pub required_allowance: U256,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PredictFunApprovalKind {
    Erc20Allowance,
    Erc1155Operator,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PredictFunApprovalStep {
    pub id: String,
    pub kind: PredictFunApprovalKind,
    pub token: Address,
    pub spender: Address,
    pub required_allowance: U256,
}

pub fn approval_plan(
    environment: PredictFunEnvironment,
    scope: PredictFunApprovalScope,
) -> Result<Vec<PredictFunApprovalStep>, PredictFunLifecycleError> {
    if scope.operation == PredictFunApprovalOperation::Convert && !scope.is_neg_risk {
        return Err(PredictFunLifecycleError::Invalid(
            "convert is only valid for NegRisk markets".to_string(),
        ));
    }
    let addresses = contract_addresses(environment);
    let exchange = match (scope.is_neg_risk, scope.is_yield_bearing) {
        (false, false) => addresses.ctf_exchange,
        (false, true) => addresses.yield_bearing_ctf_exchange,
        (true, false) => addresses.neg_risk_ctf_exchange,
        (true, true) => addresses.yield_bearing_neg_risk_ctf_exchange,
    };
    let exchange_key = match (scope.is_neg_risk, scope.is_yield_bearing) {
        (false, false) => "CTF_EXCHANGE",
        (false, true) => "YIELD_BEARING_CTF_EXCHANGE",
        (true, false) => "NEG_RISK_CTF_EXCHANGE",
        (true, true) => "YIELD_BEARING_NEG_RISK_CTF_EXCHANGE",
    };
    let conditional_tokens = match (scope.is_neg_risk, scope.is_yield_bearing) {
        (false, false) => addresses.conditional_tokens,
        (false, true) => addresses.yield_bearing_conditional_tokens,
        (true, false) => addresses.neg_risk_conditional_tokens,
        (true, true) => addresses.yield_bearing_neg_risk_conditional_tokens,
    };
    let conditional_tokens_key = if scope.is_yield_bearing {
        "YIELD_BEARING_CONDITIONAL_TOKENS"
    } else {
        "CONDITIONAL_TOKENS"
    };
    let adapter = if scope.is_yield_bearing {
        addresses.yield_bearing_neg_risk_adapter
    } else {
        addresses.neg_risk_adapter
    };
    let adapter_key = if scope.is_yield_bearing {
        "YIELD_BEARING_NEG_RISK_ADAPTER"
    } else {
        "NEG_RISK_ADAPTER"
    };
    let erc20 = |spender: Address, role: &str| PredictFunApprovalStep {
        id: format!("ERC20_ALLOWANCE:{role}"),
        kind: PredictFunApprovalKind::Erc20Allowance,
        token: addresses.usdt,
        spender,
        required_allowance: scope.required_allowance,
    };
    let erc1155 = |spender: Address, role: &str| PredictFunApprovalStep {
        id: format!("ERC1155_APPROVAL:{role}"),
        kind: PredictFunApprovalKind::Erc1155Operator,
        token: conditional_tokens,
        spender,
        required_allowance: U256::ZERO,
    };

    let mut steps = Vec::new();
    match scope.operation {
        PredictFunApprovalOperation::Trade => {
            if scope.side != Some(PredictFunSide::Buy) {
                steps.push(erc1155(exchange, exchange_key));
            }
            if scope.is_neg_risk {
                steps.push(erc1155(adapter, adapter_key));
            }
            if scope.side != Some(PredictFunSide::Sell) {
                if scope.required_allowance.is_zero() {
                    return Err(PredictFunLifecycleError::Invalid(
                        "trade BUY approval requires a positive exact allowance".to_string(),
                    ));
                }
                steps.push(erc20(exchange, exchange_key));
            }
        }
        PredictFunApprovalOperation::Split => {
            if scope.required_allowance.is_zero() {
                return Err(PredictFunLifecycleError::Invalid(
                    "split approval requires a positive exact allowance".to_string(),
                ));
            }
            steps.push(erc20(
                if scope.is_neg_risk {
                    adapter
                } else {
                    conditional_tokens
                },
                if scope.is_neg_risk {
                    adapter_key
                } else {
                    conditional_tokens_key
                },
            ));
        }
        PredictFunApprovalOperation::Merge | PredictFunApprovalOperation::Redeem => {
            if scope.is_neg_risk {
                steps.push(erc1155(adapter, adapter_key));
            }
        }
        PredictFunApprovalOperation::Convert => {
            steps.push(erc1155(adapter, adapter_key));
        }
    }
    Ok(steps)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictFunStartupRequirements {
    pub environment: PredictFunEnvironment,
    pub signer: Address,
    pub account: Address,
    pub predict_account: bool,
    pub minimum_gas_balance: U256,
    pub minimum_collateral_balance: U256,
    /// Pin this after observing the official Predict Account implementation in shadow mode.
    pub expected_kernel_account_id: Option<String>,
    pub required_approvals: Vec<PredictFunApprovalStep>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictFunStartupReadiness {
    pub chain_id: u64,
    pub gas_balance: U256,
    pub collateral_balance: U256,
    pub kernel_account_id: Option<String>,
    pub kernel_implementation: Option<Address>,
    pub approvals: Vec<PredictFunApprovalCheck>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictFunApprovalCheck {
    pub step: PredictFunApprovalStep,
    pub satisfied: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictFunCall {
    pub target: Address,
    pub calldata: Bytes,
}

pub struct PreparedPredictFunLifecycleTransaction {
    operation: &'static str,
    call: PredictFunCall,
}

impl fmt::Debug for PreparedPredictFunLifecycleTransaction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct(stringify!(PreparedPredictFunLifecycleTransaction))
            .field("operation", &self.operation)
            .field("target", &self.call.target)
            .field("calldata", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictFunLifecycleReceipt {
    pub operation: String,
    pub transaction_hash: B256,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredictFunTransactionEvidence {
    Confirmed,
    Reverted,
    Pending,
    NotFound,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PredictFunBackendError {
    #[error("read failed: {0}")]
    Read(String),
    #[error("transaction was definitively rejected before dispatch: {0}")]
    DefinitiveRejected(String),
    #[error("transaction outcome is ambiguous after dispatch: {0}")]
    AmbiguousAfterDispatch(String),
}

#[derive(Debug, Error)]
pub enum PredictFunLifecycleError {
    #[error("invalid PredictFun lifecycle request: {0}")]
    Invalid(String),
    #[error("PredictFun account is not ready: {0}")]
    NotReady(String),
    #[error("PredictFun transaction was definitively rejected: {0}")]
    DefinitiveRejected(String),
    #[error("PredictFun transaction outcome is unknown after dispatch: {0}")]
    UnknownAfterDispatch(String),
    #[error("PredictFun account read failed: {0}")]
    Read(String),
}

impl PredictFunLifecycleError {
    #[must_use]
    pub const fn is_unknown_after_dispatch(&self) -> bool {
        matches!(self, Self::UnknownAfterDispatch(_))
    }
}

#[allow(async_fn_in_trait)]
pub trait PredictFunLifecycleBackend: Send + Sync {
    fn signer_address(&self) -> Result<Address, PredictFunBackendError>;
    async fn chain_id(&self) -> Result<u64, PredictFunBackendError>;
    async fn gas_balance(&self, owner: Address) -> Result<U256, PredictFunBackendError>;
    async fn collateral_balance(&self, owner: Address) -> Result<U256, PredictFunBackendError>;
    async fn account_has_code(&self, account: Address) -> Result<bool, PredictFunBackendError>;
    async fn proxy_implementation(
        &self,
        account: Address,
    ) -> Result<Address, PredictFunBackendError>;
    async fn validator_owner(&self, account: Address) -> Result<Address, PredictFunBackendError>;
    async fn kernel_account_id(&self, account: Address) -> Result<String, PredictFunBackendError>;
    async fn kernel_supports_default_execute(
        &self,
        account: Address,
    ) -> Result<bool, PredictFunBackendError>;
    async fn allowance(
        &self,
        token: Address,
        owner: Address,
        spender: Address,
    ) -> Result<U256, PredictFunBackendError>;
    async fn is_approved_for_all(
        &self,
        token: Address,
        owner: Address,
        spender: Address,
    ) -> Result<bool, PredictFunBackendError>;
    async fn submit(&self, call: PredictFunCall) -> Result<B256, PredictFunBackendError>;
    async fn estimate_gas(&self, call: PredictFunCall) -> Result<u64, PredictFunBackendError>;
    async fn transaction_evidence(
        &self,
        transaction_hash: B256,
    ) -> Result<PredictFunTransactionEvidence, PredictFunBackendError>;
}

pub struct PredictFunLifecycle<B> {
    backend: B,
    environment: PredictFunEnvironment,
    account: Address,
    predict_account: bool,
}

impl<B: PredictFunLifecycleBackend> PredictFunLifecycle<B> {
    #[must_use]
    pub const fn new(
        backend: B,
        environment: PredictFunEnvironment,
        account: Address,
        predict_account: bool,
    ) -> Self {
        Self {
            backend,
            environment,
            account,
            predict_account,
        }
    }

    pub async fn startup_readiness(
        &self,
        requirements: &PredictFunStartupRequirements,
    ) -> Result<PredictFunStartupReadiness, PredictFunLifecycleError> {
        if requirements.environment != self.environment || requirements.account != self.account {
            return Err(PredictFunLifecycleError::Invalid(
                "startup requirements do not match lifecycle account/environment".to_string(),
            ));
        }
        if requirements.predict_account != self.predict_account {
            return Err(PredictFunLifecycleError::Invalid(
                "startup account type does not match lifecycle configuration".to_string(),
            ));
        }
        let backend_signer = self.backend.signer_address().map_err(read_error)?;
        if backend_signer != requirements.signer {
            return Err(PredictFunLifecycleError::NotReady(format!(
                "backend signer {backend_signer:#x} does not match configured signer {:#x}",
                requirements.signer
            )));
        }
        let expected_chain = chain_id(self.environment);
        let actual_chain = self.backend.chain_id().await.map_err(read_error)?;
        if actual_chain != expected_chain {
            return Err(PredictFunLifecycleError::NotReady(format!(
                "chain ID mismatch: expected {expected_chain}, received {actual_chain}"
            )));
        }
        let gas_balance = self
            .backend
            .gas_balance(requirements.signer)
            .await
            .map_err(read_error)?;
        if gas_balance < requirements.minimum_gas_balance {
            return Err(PredictFunLifecycleError::NotReady(format!(
                "signer gas balance {gas_balance} is below required {}",
                requirements.minimum_gas_balance
            )));
        }
        let collateral_balance = self
            .backend
            .collateral_balance(self.account)
            .await
            .map_err(read_error)?;
        if collateral_balance < requirements.minimum_collateral_balance {
            return Err(PredictFunLifecycleError::NotReady(format!(
                "collateral balance {collateral_balance} is below required {}",
                requirements.minimum_collateral_balance
            )));
        }

        let (kernel_account_id, kernel_implementation) = if requirements.predict_account {
            if !self
                .backend
                .account_has_code(self.account)
                .await
                .map_err(read_error)?
            {
                return Err(PredictFunLifecycleError::NotReady(
                    "Predict Account has no deployed code".to_string(),
                ));
            }
            let implementation = self
                .backend
                .proxy_implementation(self.account)
                .await
                .map_err(read_error)?;
            let expected_implementation =
                contract_addresses(self.environment).kernel_implementation;
            if implementation != expected_implementation {
                return Err(PredictFunLifecycleError::NotReady(format!(
                    "Kernel implementation mismatch: expected {expected_implementation:#x}, received {implementation:#x}"
                )));
            }
            let owner = self
                .backend
                .validator_owner(self.account)
                .await
                .map_err(read_error)?;
            if owner != requirements.signer {
                return Err(PredictFunLifecycleError::NotReady(format!(
                    "ECDSA validator owner {owner:#x} does not match signer {:#x}",
                    requirements.signer
                )));
            }
            if !self
                .backend
                .kernel_supports_default_execute(self.account)
                .await
                .map_err(read_error)?
            {
                return Err(PredictFunLifecycleError::NotReady(
                    "Predict Account does not support Kernel default execute mode".to_string(),
                ));
            }
            let account_id = self
                .backend
                .kernel_account_id(self.account)
                .await
                .map_err(read_error)?;
            if account_id.trim().is_empty() {
                return Err(PredictFunLifecycleError::NotReady(
                    "Kernel accountId is empty".to_string(),
                ));
            }
            if let Some(expected) = &requirements.expected_kernel_account_id
                && &account_id != expected
            {
                return Err(PredictFunLifecycleError::NotReady(format!(
                    "Kernel accountId mismatch: expected {expected}, received {account_id}"
                )));
            }
            (Some(account_id), Some(implementation))
        } else {
            if requirements.signer != self.account {
                return Err(PredictFunLifecycleError::NotReady(
                    "EOA account does not match signer".to_string(),
                ));
            }
            (None, None)
        };

        let approvals = self
            .check_approvals(&requirements.required_approvals)
            .await?;
        if let Some(missing) = approvals.iter().find(|check| !check.satisfied) {
            return Err(PredictFunLifecycleError::NotReady(format!(
                "approval {} is not ready",
                missing.step.id
            )));
        }
        Ok(PredictFunStartupReadiness {
            chain_id: actual_chain,
            gas_balance,
            collateral_balance,
            kernel_account_id,
            kernel_implementation,
            approvals,
        })
    }

    pub async fn check_approvals(
        &self,
        steps: &[PredictFunApprovalStep],
    ) -> Result<Vec<PredictFunApprovalCheck>, PredictFunLifecycleError> {
        let mut checks = Vec::with_capacity(steps.len());
        for step in steps {
            let satisfied = match step.kind {
                PredictFunApprovalKind::Erc20Allowance => {
                    self.backend
                        .allowance(step.token, self.account, step.spender)
                        .await
                        .map_err(read_error)?
                        >= step.required_allowance
                }
                PredictFunApprovalKind::Erc1155Operator => self
                    .backend
                    .is_approved_for_all(step.token, self.account, step.spender)
                    .await
                    .map_err(read_error)?,
            };
            checks.push(PredictFunApprovalCheck {
                step: step.clone(),
                satisfied,
            });
        }
        Ok(checks)
    }

    pub async fn run_approvals_from_env(
        &self,
        steps: &[PredictFunApprovalStep],
    ) -> Result<PredictFunApprovalRunReport, PredictFunLifecycleError> {
        let confirmation = std::env::var(APPROVAL_WRITE_CONFIRM_ENV).ok();
        self.run_approvals(steps, confirmation.as_deref()).await
    }

    async fn run_approvals(
        &self,
        steps: &[PredictFunApprovalStep],
        confirmation: Option<&str>,
    ) -> Result<PredictFunApprovalRunReport, PredictFunLifecycleError> {
        let writes_enabled = approval_writes_enabled(confirmation);
        let checks = self.check_approvals(steps).await?;
        let mut seen = HashSet::new();
        let mut results = Vec::new();
        for check in checks {
            if !seen.insert(check.step.id.clone()) {
                continue;
            }
            if check.satisfied {
                results.push(PredictFunApprovalRunStep::Satisfied(check.step));
            } else if !writes_enabled {
                let prepared = self.prepare_approval(check.step.clone())?;
                let estimated_gas = self
                    .backend
                    .estimate_gas(prepared.call.clone())
                    .await
                    .map_err(write_error)?;
                results.push(PredictFunApprovalRunStep::DryRun {
                    step: check.step,
                    estimated_gas,
                });
            } else {
                let prepared = self.prepare_approval(check.step.clone())?;
                let receipt = self.submit_prepared(prepared).await?;
                results.push(PredictFunApprovalRunStep::Confirmed {
                    step: check.step,
                    transaction_hash: receipt.transaction_hash,
                });
            }
        }
        Ok(PredictFunApprovalRunReport {
            dry_run: !writes_enabled,
            steps: results,
        })
    }

    pub fn prepare_approval(
        &self,
        step: PredictFunApprovalStep,
    ) -> Result<PreparedPredictFunLifecycleTransaction, PredictFunLifecycleError> {
        let calldata = match step.kind {
            PredictFunApprovalKind::Erc20Allowance => encode_call(
                "approve(address,uint256)",
                (step.spender, step.required_allowance).abi_encode_params(),
            ),
            PredictFunApprovalKind::Erc1155Operator => encode_call(
                "setApprovalForAll(address,bool)",
                (step.spender, true).abi_encode_params(),
            ),
        };
        Ok(self.prepare_call("approval", step.token, calldata))
    }

    pub fn prepare_redeem(
        &self,
        request: PredictFunRedeem,
    ) -> Result<PreparedPredictFunLifecycleTransaction, PredictFunLifecycleError> {
        ensure_index_set(request.index_set)?;
        let addresses = contract_addresses(self.environment);
        let (target, calldata) = if request.is_neg_risk {
            let amount = request.amount.ok_or_else(|| {
                PredictFunLifecycleError::Invalid(
                    "amount is required to redeem NegRisk positions".to_string(),
                )
            })?;
            ensure_positive(amount)?;
            let amounts = if request.index_set == U256::from(1) {
                vec![amount, U256::ZERO]
            } else {
                vec![U256::ZERO, amount]
            };
            (
                neg_risk_adapter(addresses, request.is_yield_bearing),
                encode_call(
                    "redeemPositions(bytes32,uint256[])",
                    (request.condition_id, amounts).abi_encode_params(),
                ),
            )
        } else {
            (
                conditional_tokens(addresses, request.is_yield_bearing),
                encode_call(
                    "redeemPositions(address,bytes32,bytes32,uint256[])",
                    (
                        addresses.usdt,
                        B256::ZERO,
                        request.condition_id,
                        vec![request.index_set],
                    )
                        .abi_encode_params(),
                ),
            )
        };
        Ok(self.prepare_call("redeem", target, calldata))
    }

    pub fn prepare_merge(
        &self,
        request: PredictFunMerge,
    ) -> Result<PreparedPredictFunLifecycleTransaction, PredictFunLifecycleError> {
        ensure_positive(request.amount)?;
        let addresses = contract_addresses(self.environment);
        let (target, calldata) = if request.is_neg_risk {
            (
                neg_risk_adapter(addresses, request.is_yield_bearing),
                encode_call(
                    "mergePositions(bytes32,uint256)",
                    (request.condition_id, request.amount).abi_encode_params(),
                ),
            )
        } else {
            (
                conditional_tokens(addresses, request.is_yield_bearing),
                encode_call(
                    "mergePositions(address,bytes32,bytes32,uint256[],uint256)",
                    (
                        addresses.usdt,
                        B256::ZERO,
                        request.condition_id,
                        vec![U256::from(1), U256::from(2)],
                        request.amount,
                    )
                        .abi_encode_params(),
                ),
            )
        };
        Ok(self.prepare_call("merge", target, calldata))
    }

    pub fn prepare_split(
        &self,
        request: PredictFunSplit,
    ) -> Result<PreparedPredictFunLifecycleTransaction, PredictFunLifecycleError> {
        ensure_positive(request.amount)?;
        let addresses = contract_addresses(self.environment);
        let (target, calldata) = if request.is_neg_risk {
            (
                neg_risk_adapter(addresses, request.is_yield_bearing),
                encode_call(
                    "splitPosition(bytes32,uint256)",
                    (request.condition_id, request.amount).abi_encode_params(),
                ),
            )
        } else {
            (
                conditional_tokens(addresses, request.is_yield_bearing),
                encode_call(
                    "splitPosition(address,bytes32,bytes32,uint256[],uint256)",
                    (
                        addresses.usdt,
                        B256::ZERO,
                        request.condition_id,
                        vec![U256::from(1), U256::from(2)],
                        request.amount,
                    )
                        .abi_encode_params(),
                ),
            )
        };
        Ok(self.prepare_call("split", target, calldata))
    }

    pub fn prepare_convert(
        &self,
        request: PredictFunConvert,
    ) -> Result<PreparedPredictFunLifecycleTransaction, PredictFunLifecycleError> {
        ensure_positive(request.index_set)?;
        ensure_positive(request.amount)?;
        let addresses = contract_addresses(self.environment);
        Ok(self.prepare_call(
            "convert",
            neg_risk_adapter(addresses, request.is_yield_bearing),
            encode_call(
                "convertPositions(bytes32,uint256,uint256)",
                (
                    request.neg_risk_on_chain_id,
                    request.index_set,
                    request.amount,
                )
                    .abi_encode_params(),
            ),
        ))
    }

    pub async fn submit_prepared(
        &self,
        prepared: PreparedPredictFunLifecycleTransaction,
    ) -> Result<PredictFunLifecycleReceipt, PredictFunLifecycleError> {
        let PreparedPredictFunLifecycleTransaction { operation, call } = prepared;
        let transaction_hash = self.backend.submit(call).await.map_err(write_error)?;
        Ok(PredictFunLifecycleReceipt {
            operation: operation.to_string(),
            transaction_hash,
        })
    }

    pub async fn reconcile_transaction(
        &self,
        transaction_hash: B256,
    ) -> Result<PredictFunTransactionEvidence, PredictFunLifecycleError> {
        self.backend
            .transaction_evidence(transaction_hash)
            .await
            .map_err(read_error)
    }

    fn prepare_call(
        &self,
        operation: &'static str,
        target: Address,
        calldata: Vec<u8>,
    ) -> PreparedPredictFunLifecycleTransaction {
        let direct = PredictFunCall {
            target,
            calldata: Bytes::from(calldata),
        };
        let call = if self.predict_account {
            PredictFunCall {
                target: self.account,
                calldata: Bytes::from(encode_call(
                    "execute(bytes32,bytes)",
                    (
                        B256::ZERO,
                        execution_calldata(direct.target, &direct.calldata),
                    )
                        .abi_encode_params(),
                )),
            }
        } else {
            direct
        };
        PreparedPredictFunLifecycleTransaction { operation, call }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredictFunApprovalRunStep {
    Satisfied(PredictFunApprovalStep),
    DryRun {
        step: PredictFunApprovalStep,
        estimated_gas: u64,
    },
    Confirmed {
        step: PredictFunApprovalStep,
        transaction_hash: B256,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictFunApprovalRunReport {
    pub dry_run: bool,
    pub steps: Vec<PredictFunApprovalRunStep>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PredictFunRedeem {
    pub condition_id: B256,
    pub index_set: U256,
    pub amount: Option<U256>,
    pub is_neg_risk: bool,
    pub is_yield_bearing: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PredictFunMerge {
    pub condition_id: B256,
    pub amount: U256,
    pub is_neg_risk: bool,
    pub is_yield_bearing: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PredictFunSplit {
    pub condition_id: B256,
    pub amount: U256,
    pub is_neg_risk: bool,
    pub is_yield_bearing: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PredictFunConvert {
    pub neg_risk_on_chain_id: B256,
    pub index_set: U256,
    pub amount: U256,
    pub is_yield_bearing: bool,
}

fn conditional_tokens(addresses: PredictFunContractAddresses, is_yield_bearing: bool) -> Address {
    if is_yield_bearing {
        addresses.yield_bearing_conditional_tokens
    } else {
        addresses.conditional_tokens
    }
}

fn neg_risk_adapter(addresses: PredictFunContractAddresses, is_yield_bearing: bool) -> Address {
    if is_yield_bearing {
        addresses.yield_bearing_neg_risk_adapter
    } else {
        addresses.neg_risk_adapter
    }
}

const fn chain_id(environment: PredictFunEnvironment) -> u64 {
    match environment {
        PredictFunEnvironment::Mainnet => BNB_MAINNET_CHAIN_ID,
        PredictFunEnvironment::Testnet => BNB_TESTNET_CHAIN_ID,
    }
}

fn encode_call(signature: &str, params: Vec<u8>) -> Vec<u8> {
    let selector = keccak256(signature.as_bytes());
    let mut calldata = Vec::with_capacity(4 + params.len());
    calldata.extend_from_slice(&selector[..4]);
    calldata.extend_from_slice(&params);
    calldata
}

fn execution_calldata(target: Address, calldata: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(20 + 32 + calldata.len());
    encoded.extend_from_slice(target.as_slice());
    encoded.extend_from_slice(&[0; 32]);
    encoded.extend_from_slice(calldata);
    encoded
}

fn approval_writes_enabled(value: Option<&str>) -> bool {
    value == Some(APPROVAL_WRITE_CONFIRM_VALUE)
}

fn ensure_positive(value: U256) -> Result<(), PredictFunLifecycleError> {
    if value.is_zero() {
        Err(PredictFunLifecycleError::Invalid(
            "amount must be positive".to_string(),
        ))
    } else {
        Ok(())
    }
}

fn ensure_index_set(value: U256) -> Result<(), PredictFunLifecycleError> {
    if value == U256::from(1) || value == U256::from(2) {
        Ok(())
    } else {
        Err(PredictFunLifecycleError::Invalid(
            "redeem index_set must be 1 or 2".to_string(),
        ))
    }
}

fn read_error(error: PredictFunBackendError) -> PredictFunLifecycleError {
    PredictFunLifecycleError::Read(error.to_string())
}

fn write_error(error: PredictFunBackendError) -> PredictFunLifecycleError {
    match error {
        PredictFunBackendError::DefinitiveRejected(message) => {
            PredictFunLifecycleError::DefinitiveRejected(message)
        }
        PredictFunBackendError::AmbiguousAfterDispatch(message) => {
            PredictFunLifecycleError::UnknownAfterDispatch(message)
        }
        PredictFunBackendError::Read(message) => PredictFunLifecycleError::Read(message),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use rstest::rstest;

    use super::*;

    const SIGNER: Address = address!("1111111111111111111111111111111111111111");
    const ACCOUNT: Address = address!("2222222222222222222222222222222222222222");

    #[derive(Debug, Clone)]
    struct MockBackend {
        chain_id: u64,
        gas: U256,
        collateral: U256,
        code: bool,
        implementation: Address,
        owner: Address,
        account_id: String,
        supports_execute: bool,
        allowance: U256,
        operator_approved: bool,
        submit_result: Result<B256, PredictFunBackendError>,
        estimated_gas: u64,
        submissions: Arc<AtomicUsize>,
        calls: Arc<Mutex<Vec<PredictFunCall>>>,
    }

    impl Default for MockBackend {
        fn default() -> Self {
            Self {
                chain_id: BNB_TESTNET_CHAIN_ID,
                gas: U256::from(1),
                collateral: U256::from(1),
                code: true,
                implementation: contract_addresses(PredictFunEnvironment::Testnet)
                    .kernel_implementation,
                owner: SIGNER,
                account_id: "kernel.advanced.v0.3.1".to_string(),
                supports_execute: true,
                allowance: U256::MAX,
                operator_approved: true,
                submit_result: Ok(B256::repeat_byte(0xaa)),
                estimated_gas: 21_000,
                submissions: Arc::new(AtomicUsize::new(0)),
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl PredictFunLifecycleBackend for MockBackend {
        fn signer_address(&self) -> Result<Address, PredictFunBackendError> {
            Ok(SIGNER)
        }

        async fn chain_id(&self) -> Result<u64, PredictFunBackendError> {
            Ok(self.chain_id)
        }

        async fn gas_balance(&self, _owner: Address) -> Result<U256, PredictFunBackendError> {
            Ok(self.gas)
        }

        async fn collateral_balance(
            &self,
            _owner: Address,
        ) -> Result<U256, PredictFunBackendError> {
            Ok(self.collateral)
        }

        async fn account_has_code(
            &self,
            _account: Address,
        ) -> Result<bool, PredictFunBackendError> {
            Ok(self.code)
        }

        async fn proxy_implementation(
            &self,
            _account: Address,
        ) -> Result<Address, PredictFunBackendError> {
            Ok(self.implementation)
        }

        async fn validator_owner(
            &self,
            _account: Address,
        ) -> Result<Address, PredictFunBackendError> {
            Ok(self.owner)
        }

        async fn kernel_account_id(
            &self,
            _account: Address,
        ) -> Result<String, PredictFunBackendError> {
            Ok(self.account_id.clone())
        }

        async fn kernel_supports_default_execute(
            &self,
            _account: Address,
        ) -> Result<bool, PredictFunBackendError> {
            Ok(self.supports_execute)
        }

        async fn allowance(
            &self,
            _token: Address,
            _owner: Address,
            _spender: Address,
        ) -> Result<U256, PredictFunBackendError> {
            Ok(self.allowance)
        }

        async fn is_approved_for_all(
            &self,
            _token: Address,
            _owner: Address,
            _spender: Address,
        ) -> Result<bool, PredictFunBackendError> {
            Ok(self.operator_approved)
        }

        async fn submit(&self, call: PredictFunCall) -> Result<B256, PredictFunBackendError> {
            self.submissions.fetch_add(1, Ordering::Relaxed);
            self.calls.lock().expect("mock lock").push(call);
            self.submit_result.clone()
        }

        async fn estimate_gas(&self, _call: PredictFunCall) -> Result<u64, PredictFunBackendError> {
            Ok(self.estimated_gas)
        }

        async fn transaction_evidence(
            &self,
            _transaction_hash: B256,
        ) -> Result<PredictFunTransactionEvidence, PredictFunBackendError> {
            Ok(PredictFunTransactionEvidence::Confirmed)
        }
    }

    fn lifecycle(backend: MockBackend, predict_account: bool) -> PredictFunLifecycle<MockBackend> {
        PredictFunLifecycle::new(
            backend,
            PredictFunEnvironment::Testnet,
            if predict_account { ACCOUNT } else { SIGNER },
            predict_account,
        )
    }

    fn requirements(step: PredictFunApprovalStep) -> PredictFunStartupRequirements {
        PredictFunStartupRequirements {
            environment: PredictFunEnvironment::Testnet,
            signer: SIGNER,
            account: ACCOUNT,
            predict_account: true,
            minimum_gas_balance: U256::from(1),
            minimum_collateral_balance: U256::from(1),
            expected_kernel_account_id: Some("kernel.advanced.v0.3.1".to_string()),
            required_approvals: vec![step],
        }
    }

    fn buy_step() -> PredictFunApprovalStep {
        approval_plan(
            PredictFunEnvironment::Testnet,
            PredictFunApprovalScope {
                operation: PredictFunApprovalOperation::Trade,
                is_neg_risk: false,
                is_yield_bearing: false,
                side: Some(PredictFunSide::Buy),
                required_allowance: U256::from(5),
            },
        )
        .unwrap()
        .remove(0)
    }

    #[rstest]
    fn official_addresses_and_scoped_trade_plan_are_exact() {
        let addresses = contract_addresses(PredictFunEnvironment::Mainnet);
        assert_eq!(
            addresses.ctf_exchange,
            address!("8BC070BEdAB741406F4B1Eb65A72bee27894B689")
        );
        assert_eq!(
            addresses.ecdsa_validator,
            address!("845ADb2C711129d4f3966735eD98a9F09fC4cE57")
        );
        let step = buy_step();
        assert_eq!(step.kind, PredictFunApprovalKind::Erc20Allowance);
        assert_eq!(step.required_allowance, U256::from(5));
        assert_eq!(
            step.token,
            contract_addresses(PredictFunEnvironment::Testnet).usdt
        );
    }

    #[rstest]
    fn approval_runner_is_dry_run_without_exact_confirmation() {
        assert!(!approval_writes_enabled(None));
        assert!(!approval_writes_enabled(Some("true")));
        assert!(approval_writes_enabled(Some(APPROVAL_WRITE_CONFIRM_VALUE)));
    }

    #[rstest]
    #[tokio::test]
    async fn approval_dry_run_estimates_gas_without_dispatch() {
        let backend = MockBackend {
            allowance: U256::ZERO,
            ..MockBackend::default()
        };
        let submissions = Arc::clone(&backend.submissions);
        let report = lifecycle(backend, true)
            .run_approvals(&[buy_step()], None)
            .await
            .unwrap();

        assert!(report.dry_run);
        assert!(matches!(
            report.steps.as_slice(),
            [PredictFunApprovalRunStep::DryRun {
                estimated_gas: 21_000,
                ..
            }]
        ));
        assert_eq!(submissions.load(Ordering::Relaxed), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn startup_rejects_wrong_owner_and_zero_gas() {
        let mut backend = MockBackend {
            gas: U256::ZERO,
            ..MockBackend::default()
        };
        let error = lifecycle(backend.clone(), true)
            .startup_readiness(&requirements(buy_step()))
            .await
            .unwrap_err();
        assert!(matches!(error, PredictFunLifecycleError::NotReady(_)));

        backend.gas = U256::from(1);
        backend.owner = Address::ZERO;
        let error = lifecycle(backend, true)
            .startup_readiness(&requirements(buy_step()))
            .await
            .unwrap_err();
        assert!(matches!(error, PredictFunLifecycleError::NotReady(_)));
    }

    #[rstest]
    #[tokio::test]
    async fn startup_rejects_zero_allowance_and_accepts_ready_account() {
        let backend = MockBackend {
            allowance: U256::ZERO,
            ..MockBackend::default()
        };
        let error = lifecycle(backend, true)
            .startup_readiness(&requirements(buy_step()))
            .await
            .unwrap_err();
        assert!(matches!(error, PredictFunLifecycleError::NotReady(_)));

        let ready = lifecycle(MockBackend::default(), true)
            .startup_readiness(&requirements(buy_step()))
            .await
            .unwrap();
        assert_eq!(ready.chain_id, BNB_TESTNET_CHAIN_ID);
    }

    #[rstest]
    #[tokio::test]
    async fn startup_rejects_wrong_kernel_implementation() {
        let backend = MockBackend {
            implementation: Address::ZERO,
            ..MockBackend::default()
        };
        let error = lifecycle(backend, true)
            .startup_readiness(&requirements(buy_step()))
            .await
            .unwrap_err();

        assert!(matches!(error, PredictFunLifecycleError::NotReady(_)));
    }

    #[rstest]
    fn all_predict_account_writes_route_through_kernel_execute() {
        let lifecycle = lifecycle(MockBackend::default(), true);
        let condition_id = B256::repeat_byte(0x33);
        let prepared = [
            lifecycle.prepare_approval(buy_step()).unwrap(),
            lifecycle
                .prepare_redeem(PredictFunRedeem {
                    condition_id,
                    index_set: U256::from(1),
                    amount: None,
                    is_neg_risk: false,
                    is_yield_bearing: false,
                })
                .unwrap(),
            lifecycle
                .prepare_merge(PredictFunMerge {
                    condition_id,
                    amount: U256::from(1),
                    is_neg_risk: false,
                    is_yield_bearing: false,
                })
                .unwrap(),
            lifecycle
                .prepare_split(PredictFunSplit {
                    condition_id,
                    amount: U256::from(1),
                    is_neg_risk: false,
                    is_yield_bearing: false,
                })
                .unwrap(),
            lifecycle
                .prepare_convert(PredictFunConvert {
                    neg_risk_on_chain_id: condition_id,
                    index_set: U256::from(1),
                    amount: U256::from(1),
                    is_yield_bearing: false,
                })
                .unwrap(),
        ];
        let execute_selector = &keccak256("execute(bytes32,bytes)".as_bytes())[..4];
        for transaction in prepared {
            assert_eq!(transaction.call.target, ACCOUNT);
            assert_eq!(&transaction.call.calldata[..4], execute_selector);
        }
    }

    #[rstest]
    #[tokio::test]
    async fn submit_success_is_awaited_once() {
        let backend = MockBackend::default();
        let submissions = Arc::clone(&backend.submissions);
        let lifecycle = lifecycle(backend, false);
        let prepared = lifecycle.prepare_approval(buy_step()).unwrap();

        let receipt = lifecycle.submit_prepared(prepared).await.unwrap();

        assert_eq!(receipt.transaction_hash, B256::repeat_byte(0xaa));
        assert_eq!(submissions.load(Ordering::Relaxed), 1);
    }

    #[rstest]
    #[case(
        PredictFunBackendError::DefinitiveRejected("estimate reverted".to_string()),
        false
    )]
    #[case(
        PredictFunBackendError::AmbiguousAfterDispatch("receipt timeout".to_string()),
        true
    )]
    #[tokio::test]
    async fn submit_classifies_reject_and_ambiguity_without_retry(
        #[case] backend_error: PredictFunBackendError,
        #[case] unknown: bool,
    ) {
        let backend = MockBackend {
            submit_result: Err(backend_error),
            ..MockBackend::default()
        };
        let submissions = Arc::clone(&backend.submissions);
        let lifecycle = lifecycle(backend, false);
        let prepared = lifecycle.prepare_approval(buy_step()).unwrap();

        let error = lifecycle.submit_prepared(prepared).await.unwrap_err();

        assert_eq!(error.is_unknown_after_dispatch(), unknown);
        assert_eq!(submissions.load(Ordering::Relaxed), 1);
    }
}
