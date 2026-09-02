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

use std::{str::FromStr, time::Duration};

use alloy::{
    network::{Ethereum, EthereumWallet, Network, TransactionBuilder},
    primitives::{Address, B256, U256},
    providers::{Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
};

use super::lifecycle::{
    PredictFunBackendError, PredictFunCall, PredictFunLifecycleBackend,
    PredictFunTransactionEvidence, contract_addresses,
};
use crate::{
    common::{
        consts::{BNB_MAINNET_CHAIN_ID, BNB_TESTNET_CHAIN_ID},
        enums::PredictFunEnvironment,
    },
    config::SecretString,
};

alloy::sol! {
    #[sol(rpc)]
    interface LifecycleErc20 {
        function balanceOf(address owner) external view returns (uint256 amount);
        function allowance(address owner, address spender) external view returns (uint256 amount);
    }

    #[sol(rpc)]
    interface LifecycleErc1155 {
        function isApprovedForAll(address owner, address operator) external view returns (bool approved);
    }

    #[sol(rpc)]
    interface LifecycleEcdsaValidator {
        function ecdsaValidatorStorage(address account) external view returns (address owner);
    }

    #[sol(rpc)]
    interface LifecycleKernel {
        function accountId() external view returns (string id);
        function supportsExecutionMode(bytes32 mode) external view returns (bool supported);
    }
}

pub struct AlloyPredictFunLifecycleBackend {
    rpc_url: SecretString,
    private_key: SecretString,
    environment: PredictFunEnvironment,
    receipt_timeout: Duration,
}

impl std::fmt::Debug for AlloyPredictFunLifecycleBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct(stringify!(AlloyPredictFunLifecycleBackend))
            .field("rpc_url", &"<redacted>")
            .field("private_key", &"<redacted>")
            .field("environment", &self.environment)
            .field("receipt_timeout", &self.receipt_timeout)
            .finish()
    }
}

impl AlloyPredictFunLifecycleBackend {
    #[must_use]
    pub const fn new(
        rpc_url: SecretString,
        private_key: SecretString,
        environment: PredictFunEnvironment,
        receipt_timeout: Duration,
    ) -> Self {
        Self {
            rpc_url,
            private_key,
            environment,
            receipt_timeout,
        }
    }

    pub fn signer_address(&self) -> Result<Address, PredictFunBackendError> {
        self.signer().map(|signer| signer.address())
    }

    fn signer(&self) -> Result<PrivateKeySigner, PredictFunBackendError> {
        let key = self
            .private_key
            .expose()
            .strip_prefix("0x")
            .unwrap_or(self.private_key.expose());
        PrivateKeySigner::from_str(key)
            .map_err(|error| PredictFunBackendError::Read(error.to_string()))
    }

    const fn expected_chain_id(&self) -> u64 {
        match self.environment {
            PredictFunEnvironment::Mainnet => BNB_MAINNET_CHAIN_ID,
            PredictFunEnvironment::Testnet => BNB_TESTNET_CHAIN_ID,
        }
    }

    fn rpc_url(&self) -> Result<reqwest::Url, PredictFunBackendError> {
        self.rpc_url
            .expose()
            .parse::<reqwest::Url>()
            .map_err(|error| PredictFunBackendError::Read(error.to_string()))
    }
}

impl PredictFunLifecycleBackend for AlloyPredictFunLifecycleBackend {
    fn signer_address(&self) -> Result<Address, PredictFunBackendError> {
        AlloyPredictFunLifecycleBackend::signer_address(self)
    }

    async fn chain_id(&self) -> Result<u64, PredictFunBackendError> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url()?);
        provider
            .get_chain_id()
            .await
            .map_err(|error| PredictFunBackendError::Read(error.to_string()))
    }

    async fn gas_balance(&self, owner: Address) -> Result<U256, PredictFunBackendError> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url()?);
        provider
            .get_balance(owner)
            .await
            .map_err(|error| PredictFunBackendError::Read(error.to_string()))
    }

    async fn collateral_balance(&self, owner: Address) -> Result<U256, PredictFunBackendError> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url()?);
        LifecycleErc20::new(contract_addresses(self.environment).usdt, provider)
            .balanceOf(owner)
            .call()
            .await
            .map_err(|error| PredictFunBackendError::Read(error.to_string()))
    }

    async fn account_has_code(&self, account: Address) -> Result<bool, PredictFunBackendError> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url()?);
        provider
            .get_code_at(account)
            .await
            .map(|code| !code.is_empty())
            .map_err(|error| PredictFunBackendError::Read(error.to_string()))
    }

    async fn proxy_implementation(
        &self,
        account: Address,
    ) -> Result<Address, PredictFunBackendError> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url()?);
        // keccak256("eip1967.proxy.implementation") - 1.
        let slot = U256::from_be_bytes([
            0x36, 0x08, 0x94, 0xa1, 0x3b, 0xa1, 0xa3, 0x21, 0x06, 0x67, 0xc8, 0x28, 0x49, 0x2d,
            0xb9, 0x8d, 0xca, 0x3e, 0x20, 0x76, 0xcc, 0x37, 0x35, 0xa9, 0x20, 0xa3, 0xca, 0x50,
            0x5d, 0x38, 0x2b, 0xbc,
        ]);
        let stored = provider
            .get_storage_at(account, slot)
            .await
            .map_err(|error| PredictFunBackendError::Read(error.to_string()))?;
        let bytes = stored.to_be_bytes::<32>();
        Ok(Address::from_slice(&bytes[12..]))
    }

    async fn validator_owner(&self, account: Address) -> Result<Address, PredictFunBackendError> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url()?);
        LifecycleEcdsaValidator::new(
            contract_addresses(self.environment).ecdsa_validator,
            provider,
        )
        .ecdsaValidatorStorage(account)
        .call()
        .await
        .map_err(|error| PredictFunBackendError::Read(error.to_string()))
    }

    async fn kernel_account_id(&self, account: Address) -> Result<String, PredictFunBackendError> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url()?);
        LifecycleKernel::new(account, provider)
            .accountId()
            .call()
            .await
            .map_err(|error| PredictFunBackendError::Read(error.to_string()))
    }

    async fn kernel_supports_default_execute(
        &self,
        account: Address,
    ) -> Result<bool, PredictFunBackendError> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url()?);
        LifecycleKernel::new(account, provider)
            .supportsExecutionMode(B256::ZERO)
            .call()
            .await
            .map_err(|error| PredictFunBackendError::Read(error.to_string()))
    }

    async fn allowance(
        &self,
        token: Address,
        owner: Address,
        spender: Address,
    ) -> Result<U256, PredictFunBackendError> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url()?);
        LifecycleErc20::new(token, provider)
            .allowance(owner, spender)
            .call()
            .await
            .map_err(|error| PredictFunBackendError::Read(error.to_string()))
    }

    async fn is_approved_for_all(
        &self,
        token: Address,
        owner: Address,
        spender: Address,
    ) -> Result<bool, PredictFunBackendError> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url()?);
        LifecycleErc1155::new(token, provider)
            .isApprovedForAll(owner, spender)
            .call()
            .await
            .map_err(|error| PredictFunBackendError::Read(error.to_string()))
    }

    async fn submit(&self, call: PredictFunCall) -> Result<B256, PredictFunBackendError> {
        let signer = self.signer()?;
        let provider = ProviderBuilder::new()
            .with_chain_id(self.expected_chain_id())
            .wallet(EthereumWallet::from(signer))
            .connect_http(self.rpc_url()?);
        let actual_chain = provider
            .get_chain_id()
            .await
            .map_err(|error| PredictFunBackendError::Read(error.to_string()))?;
        if actual_chain != self.expected_chain_id() {
            return Err(PredictFunBackendError::DefinitiveRejected(format!(
                "chain ID changed before dispatch: expected {}, received {actual_chain}",
                self.expected_chain_id()
            )));
        }
        let transaction: <Ethereum as Network>::TransactionRequest = Default::default();
        let transaction = transaction.with_to(call.target).with_input(call.calldata);
        let gas = provider
            .estimate_gas(transaction.clone())
            .await
            .map_err(|error| PredictFunBackendError::DefinitiveRejected(error.to_string()))?;
        let pending = provider
            .send_transaction(transaction.with_gas_limit(gas.saturating_mul(125) / 100))
            .await
            .map_err(|error| PredictFunBackendError::AmbiguousAfterDispatch(error.to_string()))?;
        let transaction_hash = *pending.tx_hash();
        let receipt = tokio::time::timeout(self.receipt_timeout, pending.get_receipt())
            .await
            .map_err(|_| {
                PredictFunBackendError::AmbiguousAfterDispatch(format!(
                    "receipt timed out for {transaction_hash:#x}"
                ))
            })?
            .map_err(|error| {
                PredictFunBackendError::AmbiguousAfterDispatch(format!(
                    "receipt failed for {transaction_hash:#x}: {error}"
                ))
            })?;
        if !receipt.status() {
            return Err(PredictFunBackendError::DefinitiveRejected(format!(
                "transaction {transaction_hash:#x} reverted on-chain"
            )));
        }
        Ok(transaction_hash)
    }

    async fn estimate_gas(&self, call: PredictFunCall) -> Result<u64, PredictFunBackendError> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url()?);
        let transaction: <Ethereum as Network>::TransactionRequest = Default::default();
        let transaction = transaction
            .with_from(self.signer_address()?)
            .with_to(call.target)
            .with_input(call.calldata);
        provider
            .estimate_gas(transaction)
            .await
            .map_err(|error| PredictFunBackendError::DefinitiveRejected(error.to_string()))
    }

    async fn transaction_evidence(
        &self,
        transaction_hash: B256,
    ) -> Result<PredictFunTransactionEvidence, PredictFunBackendError> {
        let provider = ProviderBuilder::new().connect_http(self.rpc_url()?);
        if let Some(receipt) = provider
            .get_transaction_receipt(transaction_hash)
            .await
            .map_err(|error| PredictFunBackendError::Read(error.to_string()))?
        {
            return Ok(if receipt.status() {
                PredictFunTransactionEvidence::Confirmed
            } else {
                PredictFunTransactionEvidence::Reverted
            });
        }
        let pending = provider
            .get_transaction_by_hash(transaction_hash)
            .await
            .map_err(|error| PredictFunBackendError::Read(error.to_string()))?;
        Ok(if pending.is_some() {
            PredictFunTransactionEvidence::Pending
        } else {
            PredictFunTransactionEvidence::NotFound
        })
    }
}
