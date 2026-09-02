//! Relayer client operations.

use super::*;

impl RelayerCtfTransport {
    pub(super) async fn poll_once(
        &self,
        transaction_id: &str,
    ) -> Result<Option<RelayerTransaction>, CtfTransportError> {
        let response = self
            .client
            .get(format!(
                "{}/v1/account/transactions/{transaction_id}",
                self.config.relayer_url
            ))
            .header("RELAYER_API_KEY", &self.config.relayer_api_key)
            .header(
                "RELAYER_API_KEY_ADDRESS",
                format!("{:#x}", self.config.relayer_api_key_address),
            )
            .send()
            .await
            .map_err(|error| {
                CtfTransportError::Retryable(format!("Relayer poll transport error: {error}"))
            })?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if response.status() == StatusCode::TOO_MANY_REQUESTS || response.status().is_server_error()
        {
            return Err(CtfTransportError::Retryable(format!(
                "Relayer poll returned retryable HTTP {}",
                response.status()
            )));
        }
        let transaction = response
            .error_for_status()
            .map_err(|error| CtfTransportError::Failed(format!("Relayer poll rejected: {error}")))?
            .json::<RelayerTransaction>()
            .await
            .map_err(|error| {
                CtfTransportError::Retryable(format!("Relayer poll body was unreadable: {error}"))
            })?;
        if transaction.transaction_id != transaction_id {
            return Err(CtfTransportError::Failed(
                "Relayer poll returned a different transaction ID".to_string(),
            ));
        }
        Ok(Some(transaction))
    }

    pub(super) async fn ensure_wallet_deployed(&self) -> anyhow::Result<()> {
        let expected = derive_beacon_deposit_wallet(self.signer.address());
        ensure!(
            expected == self.config.wallet_address,
            "POLYMARKET_FUNDER does not equal the deterministic Deposit Wallet for the configured signer"
        );
        let code = self
            .rpc(
                "eth_getCode",
                serde_json::json!([format!("{:#x}", self.config.wallet_address), "latest"]),
            )
            .await?;
        let code = code
            .as_str()
            .ok_or_else(|| anyhow!("eth_getCode returned a non-string"))?;
        ensure!(
            code != "0x" && code != "0x0",
            "Deposit Wallet is not deployed"
        );
        Ok(())
    }

    pub(super) async fn ensure_pusd_allowance(
        &self,
        spender: Address,
        required: Decimal,
    ) -> anyhow::Result<()> {
        let allowance = self.pusd_allowance(spender).await?;
        let required_atomic = decimal_to_atomic(required)?;
        ensure!(
            allowance >= required_atomic,
            "pUSD allowance for {spender:#x} is below required {required} pUSD"
        );
        Ok(())
    }

    pub(super) async fn pusd_allowance(&self, spender: Address) -> anyhow::Result<U256> {
        let allowance_data = allowanceCall {
            owner: self.config.wallet_address,
            spender,
        }
        .abi_encode();
        let allowance_bytes = self.eth_call(PUSD, &allowance_data).await?;
        Ok(allowanceCall::abi_decode_returns(&allowance_bytes)?)
    }

    pub(super) async fn ensure_ctf_approval(
        &self,
        operator: Address,
        label: &str,
    ) -> anyhow::Result<()> {
        ensure!(
            self.ctf_approved(operator).await?,
            "CTF approval for {label} {operator:#x} is missing"
        );
        Ok(())
    }

    pub(super) async fn ctf_approved(&self, operator: Address) -> anyhow::Result<bool> {
        let data = isApprovedForAllCall {
            account: self.config.wallet_address,
            operator,
        }
        .abi_encode();
        Ok(isApprovedForAllCall::abi_decode_returns(
            &self.eth_call(CONDITIONAL_TOKENS, &data).await?,
        )?)
    }

    pub(super) async fn ensure_operator_approvals(
        &self,
        context: &RelayerMarketContext,
    ) -> anyhow::Result<()> {
        self.ensure_ctf_approval(adapter_for(context), "collateral adapter")
            .await?;
        self.ensure_ctf_approval(exchange_for(context), "CLOB exchange")
            .await
    }

    pub(super) async fn ensure_operation_funding(
        &self,
        command: &CtfCommand,
        context: &RelayerMarketContext,
    ) -> anyhow::Result<()> {
        let balances = self.read_balances(context).await?;
        match &command.operation {
            CtfOperation::Split { quantity, .. } => {
                ensure!(
                    balances.pusd >= *quantity,
                    "pUSD balance {} is below split quantity {quantity}",
                    balances.pusd
                );
                self.ensure_pusd_allowance(adapter_for(context), *quantity)
                    .await?;
            }
            CtfOperation::Merge { quantity, .. } => {
                ensure!(
                    balances.up >= *quantity && balances.down >= *quantity,
                    "merge quantity {quantity} exceeds outcome balances UP={} DOWN={}",
                    balances.up,
                    balances.down
                );
            }
            CtfOperation::Redeem { .. } => {
                ensure!(
                    balances.up > Decimal::ZERO || balances.down > Decimal::ZERO,
                    "redeem requested with zero UP and DOWN balances"
                );
            }
        }
        Ok(())
    }

    pub(super) async fn read_balances(
        &self,
        context: &RelayerMarketContext,
    ) -> anyhow::Result<CtfBalances> {
        let pusd_data = balanceOf_0Call {
            account: self.config.wallet_address,
        }
        .abi_encode();
        let pusd = balanceOf_0Call::abi_decode_returns(&self.eth_call(PUSD, &pusd_data).await?)?;

        let up_data = balanceOf_1Call {
            account: self.config.wallet_address,
            id: context.up_token_id,
        }
        .abi_encode();
        let up = balanceOf_1Call::abi_decode_returns(
            &self.eth_call(CONDITIONAL_TOKENS, &up_data).await?,
        )?;

        let down_data = balanceOf_1Call {
            account: self.config.wallet_address,
            id: context.down_token_id,
        }
        .abi_encode();
        let down = balanceOf_1Call::abi_decode_returns(
            &self.eth_call(CONDITIONAL_TOKENS, &down_data).await?,
        )?;
        Ok(CtfBalances {
            pusd: atomic_to_decimal(pusd)?,
            up: atomic_to_decimal(up)?,
            down: atomic_to_decimal(down)?,
        })
    }

    pub(super) async fn eth_call(&self, to: Address, data: &[u8]) -> anyhow::Result<Vec<u8>> {
        let value = self
            .rpc(
                "eth_call",
                serde_json::json!([{
                    "to": format!("{to:#x}"),
                    "data": format!("0x{}", hex::encode(data)),
                }, "latest"]),
            )
            .await?;
        let encoded = value
            .as_str()
            .ok_or_else(|| anyhow!("eth_call returned a non-string result"))?
            .strip_prefix("0x")
            .ok_or_else(|| anyhow!("eth_call result is not 0x-prefixed"))?;
        Ok(hex::decode(encoded)?)
    }

    pub(super) async fn transaction_is_final(
        &self,
        transaction_hash: &str,
    ) -> Result<bool, CtfTransportError> {
        let Some(receipt) = self
            .rpc_optional(
                "eth_getTransactionReceipt",
                serde_json::json!([transaction_hash]),
            )
            .await
            .map_err(|error| {
                CtfTransportError::Retryable(format!("cannot fetch transaction receipt: {error}"))
            })?
        else {
            return Ok(false);
        };
        let status = receipt
            .get("status")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                CtfTransportError::Retryable("transaction receipt has no status".to_string())
            })?;
        match parse_hex_u64(status) {
            Ok(1) => {}
            Ok(_) => {
                return Err(CtfTransportError::Failed(
                    "confirmed Relayer transaction reverted on Polygon".to_string(),
                ));
            }
            Err(error) => {
                return Err(CtfTransportError::Retryable(format!(
                    "transaction receipt has invalid status: {error}"
                )));
            }
        }
        let transaction_block = receipt
            .get("blockNumber")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                CtfTransportError::Retryable("transaction receipt has no block number".to_string())
            })
            .and_then(|value| {
                parse_hex_u64(value).map_err(|error| {
                    CtfTransportError::Retryable(format!(
                        "transaction receipt has invalid block number: {error}"
                    ))
                })
            })?;
        let latest_block = self
            .rpc("eth_blockNumber", serde_json::json!([]))
            .await
            .map_err(|error| {
                CtfTransportError::Retryable(format!("cannot fetch Polygon head: {error}"))
            })?
            .as_str()
            .ok_or_else(|| {
                CtfTransportError::Retryable("Polygon head is not a hex string".to_string())
            })
            .and_then(|value| {
                parse_hex_u64(value).map_err(|error| {
                    CtfTransportError::Retryable(format!("Polygon head is invalid: {error}"))
                })
            })?;
        Ok(latest_block.saturating_add(1)
            >= transaction_block.saturating_add(REQUIRED_CONFIRMATIONS))
    }

    pub(super) async fn rpc(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let response = self
            .client
            .post(&self.config.polygon_rpc_url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }))
            .send()
            .await
            .map_err(|_| anyhow!("Polygon RPC transport request failed"))?;
        let status = response.status();
        let response = response
            .error_for_status()
            .map_err(|_| anyhow!("Polygon RPC returned HTTP {status}"))?
            .json::<RpcResponse>()
            .await
            .map_err(|_| anyhow!("Polygon RPC response was unreadable"))?;
        if let Some(error) = response.error {
            bail!("Polygon RPC error {}: {}", error.code, error.message);
        }
        response
            .result
            .ok_or_else(|| anyhow!("Polygon RPC response has no result"))
    }

    pub(super) async fn rpc_optional(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let response = self
            .client
            .post(&self.config.polygon_rpc_url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }))
            .send()
            .await
            .map_err(|_| anyhow!("Polygon RPC transport request failed"))?;
        let status = response.status();
        let response = response
            .error_for_status()
            .map_err(|_| anyhow!("Polygon RPC returned HTTP {status}"))?
            .json::<RpcResponse>()
            .await
            .map_err(|_| anyhow!("Polygon RPC response was unreadable"))?;
        if let Some(error) = response.error {
            bail!("Polygon RPC error {}: {}", error.code, error.message);
        }
        Ok(response.result.filter(|result| !result.is_null()))
    }
}
