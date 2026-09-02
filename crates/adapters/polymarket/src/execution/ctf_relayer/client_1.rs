//! Relayer client operations.

use super::*;

impl RelayerCtfTransport {
    pub fn register_market_context(&mut self, context: RelayerMarketContext) -> anyhow::Result<()> {
        ensure!(
            !context.neg_risk,
            "negative-risk markets are not approved for the BTC binary live strategy"
        );
        let key = format!("{:#x}", context.condition_id);
        if let Some(existing) = self.markets.get(&key) {
            ensure!(
                existing == &context,
                "relayer market context is already registered with different policy data"
            );
            return Ok(());
        }
        self.markets.insert(key, context);
        Ok(())
    }

    pub fn new(
        config: RelayerConfig,
        contexts: impl IntoIterator<Item = RelayerMarketContext>,
    ) -> anyhow::Result<Self> {
        let key = config
            .private_key
            .strip_prefix("0x")
            .unwrap_or(&config.private_key);
        let signer = PrivateKeySigner::from_str(key).context("invalid POLYMARKET_PRIVATE_KEY")?;
        ensure!(
            signer.address() == config.relayer_api_key_address,
            "relayer API key address must equal the local EOA signer"
        );
        let mut markets = BTreeMap::new();
        for context in contexts {
            let key = format!("{:#x}", context.condition_id);
            ensure!(
                !context.neg_risk,
                "negative-risk markets are not approved for the BTC binary live strategy"
            );
            ensure!(
                markets.insert(key.clone(), context).is_none(),
                "duplicate relayer market context {key}"
            );
        }
        ensure!(
            !markets.is_empty(),
            "at least one relayer market context is required"
        );
        ensure!(
            signer.address() != config.wallet_address,
            "POLYMARKET_FUNDER must be the Deposit Wallet and must differ from the EOA signer"
        );
        let mut client = Client::builder()
            .connect_timeout(Duration::from_secs(HTTP_CONNECT_TIMEOUT_SECS))
            .timeout(Duration::from_secs(HTTP_REQUEST_TIMEOUT_SECS));
        #[cfg(not(test))]
        {
            client = client.https_only(true);
        }
        let nautilus_proxy = config
            .proxy_url
            .clone()
            .map(ProxyUrl::parse)
            .transpose()
            .map_err(|_| anyhow!("could not configure the approved Polymarket proxy"))?;
        if let Some(proxy_url) = &config.proxy_url {
            let proxy = reqwest::Proxy::all(proxy_url)
                .map_err(|_| anyhow!("could not configure the approved Polymarket proxy"))?;
            client = client.proxy(proxy);
        }
        let client = client.build()?;
        let credential = Credential::new(
            &config.clob_api_key,
            &config.clob_api_secret,
            config.clob_passphrase.clone(),
        )
        .context("invalid Polymarket CLOB L2 credentials")?;
        let clob_client = PolymarketClobHttpClient::new_with_proxy(
            credential,
            format!("{:#x}", signer.address()),
            Some(config.clob_url.clone()),
            HTTP_REQUEST_TIMEOUT_SECS,
            nautilus_proxy.clone(),
        )
        .context("cannot construct bounded Polymarket CLOB client")?;
        let data_api_client = PolymarketDataApiHttpClient::new_with_proxy(
            Some(config.data_api_url.clone()),
            HTTP_REQUEST_TIMEOUT_SECS,
            nautilus_proxy,
        )
        .context("cannot construct bounded Polymarket Data API client")?;
        Ok(Self {
            config,
            signer,
            client,
            clob_client,
            data_api_client,
            wallet_lock: Mutex::new(None),
            markets,
        })
    }

    /// Acquires the fixed, wallet-derived inter-process guard before any network preflight or
    /// submission. The file descriptor remains open for this transport's entire lifetime, so a
    /// crash automatically releases the advisory lock while a second healthy process fails
    /// closed instead of sharing CTF collateral.
    pub(super) fn ensure_wallet_lock(&self) -> anyhow::Result<()> {
        #[cfg(test)]
        return Ok(());

        #[cfg(not(test))]
        {
            let mut held = self
                .wallet_lock
                .lock()
                .map_err(|_| anyhow!("wallet lock state mutex is poisoned"))?;
            if held.is_none() {
                *held = Some(acquire_wallet_lock(self.config.wallet_address)?);
            }
            Ok(())
        }
    }

    /// Runs read-only deployment, allowance and balance checks without submitting a transaction.
    pub(super) async fn preflight(&self) -> anyhow::Result<BTreeMap<String, CtfBalances>> {
        self.ensure_wallet_lock()?;
        self.ensure_wallet_deployed().await?;
        self.ensure_clob_l2_identity().await?;
        self.ensure_wallet_scope_is_clean().await?;
        let mut balances = BTreeMap::new();
        let mut required_by_adapter = BTreeMap::<Address, Decimal>::new();
        // Contexts represent future, mutually-exclusive strategy lifecycles. The CTF actor
        // serializes operations, so startup must prove one admitted operation can execute, not
        // reserve the sum of every future period's collateral.
        let wallet_cap = self.config.required_wallet_cap()?;
        let mut required_total = Decimal::ZERO;
        for (condition, context) in &self.markets {
            self.ensure_operator_approvals(context).await?;
            required_total = required_total.max(context.planned_split_quantity);
            let adapter_required = required_by_adapter.entry(adapter_for(context)).or_default();
            *adapter_required = (*adapter_required).max(context.planned_split_quantity);
            balances.insert(condition.clone(), self.read_balances(context).await?);
        }
        let pusd = balances
            .values()
            .next()
            .map_or(Decimal::ZERO, |balance| balance.pusd);
        ensure!(
            required_total <= wallet_cap,
            "configured lifecycle requires {required_total} pUSD but live wallet cap is {wallet_cap}"
        );
        ensure!(
            pusd >= wallet_cap,
            "pUSD balance {pusd} is below live wallet cap {wallet_cap}"
        );
        for (adapter, required) in required_by_adapter {
            self.ensure_pusd_allowance(adapter, required).await?;
        }
        Ok(balances)
    }

    /// Performs one safe GET poll. `STATE_CONFIRMED` is not terminal for strategy accounting
    /// until the Polygon receipt is finalized and the operation-specific balance delta is proved.
    pub(super) async fn poll_async(
        &self,
        command: &CtfCommand,
        transaction_id: &str,
        balances_before: &CtfBalances,
    ) -> Result<CtfPollResult, CtfTransportError> {
        // The deadline forbids waiting indefinitely or ever resubmitting an accepted operation;
        // it must not forbid a final read-only status query. A transaction can become confirmed
        // while the process is restarting, and observing that confirmation is the only safe way
        // to reconcile its already-created wallet inventory.
        let deadline_expired = self.poll_deadline_expired(command)?;
        let transaction = match self.poll_once(transaction_id).await {
            Ok(Some(transaction)) => transaction,
            Ok(None) if !deadline_expired => return Ok(CtfPollResult::Pending),
            Ok(None) => {
                return Err(CtfTransportError::Ambiguous(format!(
                    "Relayer transaction {transaction_id} exceeded the durable polling deadline without a terminal status; automatic resubmission is forbidden"
                )));
            }
            Err(error) if deadline_expired => {
                return Err(CtfTransportError::Ambiguous(format!(
                    "Relayer transaction {transaction_id} exceeded the durable polling deadline and its final status query failed; automatic resubmission is forbidden: {error}"
                )));
            }
            Err(error) => return Err(error),
        };
        match transaction.state.as_str() {
            "STATE_NEW" | "STATE_EXECUTED" | "STATE_MINED" if deadline_expired => {
                Err(CtfTransportError::Ambiguous(format!(
                    "Relayer transaction {transaction_id} exceeded the durable polling deadline in {}; automatic resubmission is forbidden",
                    transaction.state
                )))
            }
            "STATE_NEW" | "STATE_EXECUTED" | "STATE_MINED" => Ok(CtfPollResult::Pending),
            "STATE_FAILED" | "STATE_INVALID" => Err(CtfTransportError::Failed(format!(
                "relayer transaction {transaction_id} reached {}{}",
                transaction.state,
                transaction
                    .error_msg
                    .as_deref()
                    .filter(|message| !message.trim().is_empty())
                    .map(|message| format!(": {message}"))
                    .unwrap_or_default()
            ))),
            "STATE_CONFIRMED" => {
                let tx_hash = transaction.transaction_hash.ok_or_else(|| {
                    CtfTransportError::Retryable(
                        "confirmed relayer transaction has no on-chain hash yet".to_string(),
                    )
                })?;
                if !self.transaction_is_final(&tx_hash).await? {
                    return Ok(CtfPollResult::Pending);
                }
                let context = self.context(command)?;
                // Polygon finality alone is insufficient: CLOB keeps its own balance/allowance
                // cache. Refresh it before reading/publishing the receipt that lets strategy
                // execution create a sell order. A refresh failure is retryable and never emits
                // `BalancesVerified`.
                self.refresh_clob_balance_allowance(command, context)
                    .await?;
                let balances_after = self.read_balances(context).await.map_err(|_| {
                    CtfTransportError::Retryable("post-operation balance read failed".to_string())
                })?;
                verify_balance_delta(command, balances_before, &balances_after)?;
                Ok(CtfPollResult::Completed(CtfReceipt {
                    tx_hash,
                    balances: balances_after,
                }))
            }
            other => Err(CtfTransportError::Retryable(format!(
                "unknown non-terminal relayer transaction state {other}"
            ))),
        }
    }

    pub(super) fn poll_deadline_expired(
        &self,
        command: &CtfCommand,
    ) -> Result<bool, CtfTransportError> {
        let started_at_ns = command.ts_init.as_u64();
        let budget_ns = u64::from(self.config.max_polls)
            .checked_mul(u64::try_from(self.config.poll_interval.as_nanos()).unwrap_or(u64::MAX))
            .ok_or_else(|| {
                CtfTransportError::Failed("Relayer poll deadline overflows u64".to_string())
            })?;
        let deadline_ns = started_at_ns.checked_add(budget_ns).ok_or_else(|| {
            CtfTransportError::Failed("Relayer poll deadline overflows UnixNanos".to_string())
        })?;
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| {
                CtfTransportError::Failed(format!("system clock is before Unix epoch: {error}"))
            })?
            .as_nanos();
        let now_ns = u64::try_from(now_ns).map_err(|_| {
            CtfTransportError::Failed("system clock exceeds UnixNanos range".to_string())
        })?;
        Ok(now_ns >= deadline_ns)
    }

    pub(super) async fn refresh_clob_balance_allowance(
        &self,
        command: &CtfCommand,
        context: &RelayerMarketContext,
    ) -> Result<(), CtfTransportError> {
        for params in clob_refresh_params(command, context) {
            self.clob_client
                .update_balance_allowance(params)
                .await
                .map_err(|error| {
                    CtfTransportError::Retryable(format!(
                        "CLOB balance/allowance cache refresh failed: {error}"
                    ))
                })?;
        }
        Ok(())
    }

    /// A read-only authenticated request validates that the L2 credentials, signer and
    /// Poly1271/funder account can be resolved by CLOB before any Relayer submission.
    pub(super) async fn ensure_clob_l2_identity(&self) -> anyhow::Result<()> {
        self.clob_client
            .get_balance_allowance(GetBalanceAllowanceParams {
                asset_type: Some(AssetType::Collateral),
                signature_type: Some(SignatureType::Poly1271),
                ..Default::default()
            })
            .await
            .context("CLOB L2 credential/funder identity check failed")?;
        Ok(())
    }

    pub(super) async fn ensure_split_geo_allowed(&self) -> Result<(), CtfTransportError> {
        let response = self
            .client
            .get(&self.config.geoblock_url)
            .send()
            .await
            .map_err(|error| retryable_pre_submit("Polymarket geoblock transport check", error))?;
        let status = response.status();
        if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            return Err(CtfTransportError::Retryable(format!(
                "Polymarket geoblock check returned retryable HTTP {status}"
            )));
        }
        let response = response.error_for_status().map_err(|_| {
            CtfTransportError::Failed(format!(
                "Polymarket geoblock check was rejected with HTTP {status}"
            ))
        })?;
        let response = response
            .bytes()
            .await
            .map_err(|error| retryable_pre_submit("Polymarket geoblock response read", error))?;
        parse_split_geoblock_response(&response)
    }

    /// A complete-set wallet must not be shared with another trading process or strategy.  The
    /// Data API identifies CTF inventory that would otherwise make pUSD alone look safe, while
    /// authenticated CLOB orders identify reserved inventory.  Both queries are read-only.
    pub(super) async fn ensure_wallet_scope_is_clean(&self) -> anyhow::Result<()> {
        let wallet_cap = self.config.required_wallet_cap()?;
        let positions = self
            .data_api_client
            .get_positions(&format!("{:#x}", self.config.wallet_address))
            .await
            .context("Polymarket Data API wallet-position check failed")?;
        validate_wallet_positions(&positions, &self.markets, wallet_cap)?;

        let orders = self
            .clob_client
            .get_orders(GetOrdersParams::default())
            .await
            .context("CLOB open-order scope check failed")?;
        for order in orders {
            validate_open_order_scope(
                order.market.as_str(),
                order.asset_id.as_str(),
                &self.markets,
            )?;
        }
        Ok(())
    }

    pub(super) fn context(
        &self,
        command: &CtfCommand,
    ) -> Result<&RelayerMarketContext, CtfTransportError> {
        let condition_id = match &command.operation {
            CtfOperation::Split { condition_id, .. }
            | CtfOperation::Merge { condition_id, .. }
            | CtfOperation::Redeem { condition_id, .. } => condition_id,
        };
        self.markets
            .get(&condition_id.to_ascii_lowercase())
            .ok_or_else(|| CtfTransportError::Failed("unknown condition id".to_string()))
    }

    pub(super) fn operation_call(
        &self,
        command: &CtfCommand,
        context: &RelayerMarketContext,
    ) -> anyhow::Result<Call> {
        let binary_partition = vec![U256::from(1), U256::from(2)];
        let data = match &command.operation {
            CtfOperation::Split {
                condition_id,
                quantity,
                neg_risk,
            } => {
                validate_command_context(condition_id, *neg_risk, context)?;
                splitPositionCall {
                    collateralToken: PUSD,
                    parentCollectionId: B256::ZERO,
                    conditionId: context.condition_id,
                    partition: binary_partition,
                    amount: decimal_to_atomic(*quantity)?,
                }
                .abi_encode()
            }
            CtfOperation::Merge {
                condition_id,
                quantity,
                neg_risk,
            } => {
                validate_command_context(condition_id, *neg_risk, context)?;
                mergePositionsCall {
                    collateralToken: PUSD,
                    parentCollectionId: B256::ZERO,
                    conditionId: context.condition_id,
                    partition: binary_partition,
                    amount: decimal_to_atomic(*quantity)?,
                }
                .abi_encode()
            }
            CtfOperation::Redeem {
                condition_id,
                neg_risk,
                ..
            } => {
                validate_command_context(condition_id, *neg_risk, context)?;
                redeemPositionsCall {
                    collateralToken: PUSD,
                    parentCollectionId: B256::ZERO,
                    conditionId: context.condition_id,
                    indexSets: binary_partition,
                }
                .abi_encode()
            }
        };
        Ok(Call {
            target: adapter_for(context),
            value: U256::ZERO,
            data: Bytes::from(data),
        })
    }

    pub(super) fn signed_batch_request(
        &self,
        command: &CtfCommand,
        nonce: U256,
        deadline: u64,
        call: Call,
    ) -> anyhow::Result<DepositWalletBatchRequest> {
        self.signed_batch_request_with_metadata(
            nonce,
            deadline,
            vec![call],
            operation_metadata(command),
        )
    }

    pub(super) fn signed_batch_request_with_metadata(
        &self,
        nonce: U256,
        deadline: u64,
        calls: Vec<Call>,
        metadata: String,
    ) -> anyhow::Result<DepositWalletBatchRequest> {
        let batch = Batch {
            wallet: self.config.wallet_address,
            nonce,
            deadline: U256::from(deadline),
            calls: calls.clone(),
        };
        let domain = eip712_domain! {
            name: "DepositWallet",
            version: "1",
            chain_id: POLYGON_CHAIN_ID,
            verifying_contract: self.config.wallet_address,
        };
        let hash = batch.eip712_signing_hash(&domain);
        let signature = self
            .signer
            .sign_hash_sync(&hash)
            .context("failed to sign Deposit Wallet batch")?;
        Ok(DepositWalletBatchRequest {
            transaction_type: "WALLET".to_string(),
            from: self.signer.address(),
            to: DEPOSIT_WALLET_FACTORY,
            nonce: nonce.to_string(),
            signature: format!("0x{}", hex::encode(signature.as_bytes())),
            metadata,
            deposit_wallet_params: DepositWalletParams {
                deposit_wallet: self.config.wallet_address,
                deadline: deadline.to_string(),
                calls: calls.into_iter().map(DepositWalletCallJson::from).collect(),
            },
        })
    }

    pub(super) async fn relayer_nonce(&self) -> anyhow::Result<U256> {
        let response = self
            .client
            .get(format!(
                "{}/v1/account/transactions/params",
                self.config.relayer_url
            ))
            .header("RELAYER_API_KEY", &self.config.relayer_api_key)
            .header(
                "RELAYER_API_KEY_ADDRESS",
                format!("{:#x}", self.config.relayer_api_key_address),
            )
            .query(&[
                ("address", format!("{:#x}", self.signer.address())),
                ("type", "WALLET".to_string()),
            ])
            .send()
            .await?
            .error_for_status()?
            .json::<NonceResponse>()
            .await?;
        ensure!(
            !response.relayer_address.is_zero(),
            "relayer nonce response returned the zero relayer address"
        );
        U256::from_str(&response.nonce).context("relayer returned an invalid wallet nonce")
    }

    pub(super) async fn submit(
        &self,
        request: &DepositWalletBatchRequest,
    ) -> Result<SubmitResponse, CtfTransportError> {
        let response = self
            .client
            .post(format!("{}/submit", self.config.relayer_url))
            .header("RELAYER_API_KEY", &self.config.relayer_api_key)
            .header(
                "RELAYER_API_KEY_ADDRESS",
                format!("{:#x}", self.config.relayer_api_key_address),
            )
            .json(request)
            .send()
            .await
            .map_err(|error| {
                CtfTransportError::Ambiguous(format!(
                    "relayer submission transport failed; request may have been accepted: {error}"
                ))
            })?;
        let status = response.status();
        if !status.is_success() {
            // The HTTP status proves the request was rejected, so reading the diagnostic cannot
            // turn this into an ambiguous submission.  Only documented message fields are
            // retained; never echo arbitrary bodies which could reflect signed request data.
            let detail = response
                .json::<serde_json::Value>()
                .await
                .ok()
                .as_ref()
                .and_then(relayer_rejection_detail)
                .unwrap_or_else(|| "no structured error detail".to_string());
            return Err(CtfTransportError::Failed(format!(
                "relayer rejected submission with HTTP {status}: {detail}"
            )));
        }
        response.json::<SubmitResponse>().await.map_err(|error| {
            CtfTransportError::Ambiguous(format!(
                "relayer accepted submission but response was unreadable: {error}"
            ))
        })
    }
}

pub(super) fn relayer_rejection_detail(value: &serde_json::Value) -> Option<String> {
    ["error", "message", "errorMsg", "error_msg"]
        .into_iter()
        .find_map(|field| value.get(field).and_then(serde_json::Value::as_str))
        .map(|detail| detail.chars().take(256).collect())
}
