//! Provider-owned Predict.fun authentication, readiness and normalized private evidence.

use std::{
    collections::HashSet,
    fmt,
    str::FromStr,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

const MAX_PREPARE_BOOK_AGE_MS: u64 = 5_000;

use alloy_primitives::{Address, U256};
use nautilus_network::websocket::TransportBackend;
use rust_decimal::Decimal;

use super::{
    agent::{
        PredictFunAgentError, PredictFunAgentFacade, PredictFunAgentReconciliation,
        PredictFunAgentSubmitResult, PredictFunCheckedFokOrder, PreparedPredictFunOrder,
    },
    lifecycle::{
        PredictFunApprovalCheck, PredictFunApprovalStep, PredictFunLifecycle,
        PredictFunStartupReadiness, PredictFunStartupRequirements,
    },
    lifecycle_rpc::AlloyPredictFunLifecycleBackend,
    session::PredictFunExecutionSession,
};
use crate::{
    common::{
        enums::{PredictFunAccountType, PredictFunEnvironment, PredictFunQuoteType},
        parse::wei_to_decimal,
    },
    config::SecretString,
    http::{
        PredictFunHttpClient,
        models::{
            PredictFunAccountActivity, PredictFunFeeAsset, PredictFunMatch, PredictFunMatchOrder,
            PredictFunOrderRecord, PredictFunPosition,
        },
    },
    signing::eip712::PredictFunOrderSigner,
    websocket::{
        PredictFunWsEvent,
        messages::{PredictFunWalletEvent, PredictFunWsMessage},
    },
};

#[derive(Clone)]
pub struct PredictFunAgentRuntimeConfig {
    pub rest_url: String,
    pub websocket_url: String,
    pub api_key: SecretString,
    pub private_key: SecretString,
    pub rpc_url: SecretString,
    pub environment: PredictFunEnvironment,
    pub account_address: Address,
    pub startup: PredictFunStartupRequirements,
    pub transport_backend: TransportBackend,
    pub http_timeout_secs: u64,
    pub transaction_timeout_secs: u64,
    /// Enables the OSS SDK-compatible raw `Kernel.execute` cancellation fallback. Ordinary CLOB
    /// BUY/SELL never require this or a BNB balance.
    pub allow_raw_transaction_fallback: bool,
}

impl fmt::Debug for PredictFunAgentRuntimeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct(stringify!(PredictFunAgentRuntimeConfig))
            .field("rest_url", &self.rest_url)
            .field("websocket_url", &self.websocket_url)
            .field("api_key", &"<redacted>")
            .field("private_key", &"<redacted>")
            .field("rpc_url", &"<redacted>")
            .field("environment", &self.environment)
            .field("account_address", &self.account_address)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictFunNormalizedFill {
    pub event_id: String,
    pub settlement_id: String,
    pub venue_order_id: Option<String>,
    pub native_order_hash: String,
    pub market_id: u64,
    pub side: PredictFunQuoteType,
    pub gross_quantity: String,
    pub net_quantity: String,
    pub price: String,
    pub native_fee_amount: String,
    pub fee_asset: PredictFunFeeAsset,
    pub timestamp_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredictFunRuntimeEvent {
    Order {
        event_type: String,
        venue_order_id: String,
        native_order_hash: String,
        market_id: u64,
        side: PredictFunQuoteType,
        quantity: String,
        price: String,
        reason: Option<String>,
        timestamp_ms: u64,
    },
    Fill(PredictFunNormalizedFill),
    Reconciled,
    Error(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct PredictFunRuntimeReconciliation {
    pub orders: Vec<PredictFunOrderRecord>,
    pub fills: Vec<PredictFunNormalizedFill>,
    pub order_evidence: Vec<PredictFunNormalizedOrderEvidence>,
    pub positions: Vec<PredictFunPosition>,
    pub activity: Vec<PredictFunAccountActivity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictFunNormalizedOrderEvidence {
    pub event_id: String,
    pub status: String,
    pub reason: Option<String>,
    pub venue_order_id: Option<String>,
    pub native_order_hash: Option<String>,
    pub market_id: u64,
    pub token_id: String,
    pub side: PredictFunQuoteType,
    pub quantity: String,
    pub price: String,
    pub occurred_at: String,
    pub transaction_hash: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PredictFunRuntimeReadiness {
    pub authenticated: bool,
    pub private_stream_ready: bool,
    pub reconciliation_ready: bool,
    pub execution_ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictFunRuntimeBalances {
    pub chain_id: u64,
    /// Venue collateral in human USDT units (18 decimals on BNB Chain).
    pub collateral_balance: String,
    pub collateral_balance_wei: U256,
    pub gas_balance_wei: U256,
}

pub struct PredictFunAgentRuntime {
    facade: Arc<PredictFunAgentFacade<PredictFunHttpClient>>,
    lifecycle: Arc<PredictFunLifecycle<AlloyPredictFunLifecycleBackend>>,
    startup: PredictFunStartupRequirements,
    rpc_url: SecretString,
    transaction_timeout_secs: u64,
    allow_raw_transaction_fallback: bool,
    account_address: Address,
    token: Arc<Mutex<Option<SecretString>>>,
    transport_ready: Arc<AtomicBool>,
    ready: Arc<AtomicBool>,
    account_readiness: Arc<RwLock<Option<PredictFunStartupReadiness>>>,
    session: PredictFunExecutionSession,
    events: Option<tokio::sync::mpsc::UnboundedReceiver<PredictFunRuntimeEvent>>,
    event_task: tokio::task::JoinHandle<()>,
}

impl fmt::Debug for PredictFunAgentRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct(stringify!(PredictFunAgentRuntime))
            .field("account_address", &self.account_address)
            .field("ready", &self.is_ready())
            .field("token", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl PredictFunAgentRuntime {
    pub async fn connect(
        config: PredictFunAgentRuntimeConfig,
    ) -> Result<Self, PredictFunAgentError> {
        validate_config(&config)?;
        let http = PredictFunHttpClient::new(
            config.rest_url,
            Some(&config.api_key),
            config.http_timeout_secs,
        )
        .map_err(|error| PredictFunAgentError::Read(error.to_string()))?;
        let signer = Arc::new(
            PredictFunOrderSigner::new(config.private_key.expose())
                .map_err(|error| PredictFunAgentError::Invalid(error.to_string()))?,
        );
        let facade = Arc::new(PredictFunAgentFacade::new(
            http.clone(),
            config.private_key.clone(),
            config.environment,
            PredictFunAccountType::PredictAccount,
            config.account_address,
        )?);
        let lifecycle = Arc::new(PredictFunLifecycle::new(
            AlloyPredictFunLifecycleBackend::new(
                config.rpc_url.clone(),
                config.private_key,
                config.environment,
                Duration::from_secs(config.transaction_timeout_secs),
            ),
            config.environment,
            config.account_address,
            true,
        ));
        let token = Arc::new(Mutex::new(None));
        let transport_ready = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(AtomicBool::new(false));
        let account_readiness = Arc::new(RwLock::new(None));
        let mut session = PredictFunExecutionSession::connect(
            http,
            &config.websocket_url,
            Some(config.api_key),
            signer,
            config.environment,
            PredictFunAccountType::PredictAccount,
            Some(&format!("{:#x}", config.account_address)),
            config.transport_backend,
            Arc::clone(&token),
            Arc::clone(&transport_ready),
        )
        .await
        .map_err(|error| PredictFunAgentError::Read(error.to_string()))?;
        let private_events = session
            .take_events()
            .map_err(|error| PredictFunAgentError::Read(error.to_string()))?;

        let mut trading_startup = config.startup.clone();
        trading_startup.minimum_gas_balance = U256::ZERO;
        let initial_readiness = verify_account_and_reconcile(
            lifecycle.as_ref(),
            &trading_startup,
            facade.as_ref(),
            &current_token(&token)?,
            config.account_address,
        )
        .await;
        let startup_readiness = match initial_readiness {
            Ok((startup_readiness, _)) => startup_readiness,
            Err(error) => {
                session.disconnect().await;
                return Err(error);
            }
        };
        account_readiness
            .write()
            .map_err(|_| PredictFunAgentError::Read("PredictFun readiness lock poisoned".into()))?
            .replace(startup_readiness);
        ready.store(true, Ordering::Release);

        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
        let event_task = spawn_private_event_task(
            private_events,
            Arc::clone(&facade),
            Arc::clone(&token),
            Arc::clone(&transport_ready),
            Arc::clone(&ready),
            Arc::clone(&lifecycle),
            trading_startup.clone(),
            Arc::clone(&account_readiness),
            config.account_address,
            event_tx,
        );
        Ok(Self {
            facade,
            lifecycle,
            startup: trading_startup,
            rpc_url: config.rpc_url,
            transaction_timeout_secs: config.transaction_timeout_secs,
            allow_raw_transaction_fallback: config.allow_raw_transaction_fallback,
            account_address: config.account_address,
            token,
            transport_ready,
            ready,
            account_readiness,
            session,
            events: Some(event_rx),
            event_task,
        })
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.readiness().execution_ready
    }

    #[must_use]
    pub fn readiness(&self) -> PredictFunRuntimeReadiness {
        let authenticated = self.token.lock().is_ok_and(|token| token.is_some());
        let private_stream_ready = self.transport_ready.load(Ordering::Acquire);
        let reconciliation_ready = self.ready.load(Ordering::Acquire)
            && self
                .account_readiness
                .read()
                .is_ok_and(|readiness| readiness.is_some());
        PredictFunRuntimeReadiness {
            authenticated,
            private_stream_ready,
            reconciliation_ready,
            execution_ready: authenticated && private_stream_ready && reconciliation_ready,
        }
    }

    pub fn take_events(
        &mut self,
    ) -> Result<tokio::sync::mpsc::UnboundedReceiver<PredictFunRuntimeEvent>, PredictFunAgentError>
    {
        self.events.take().ok_or_else(|| {
            PredictFunAgentError::Invalid("PredictFun runtime event receiver already taken".into())
        })
    }

    pub async fn prepare_order(
        &self,
        request: PredictFunCheckedFokOrder,
    ) -> Result<PreparedPredictFunOrder, PredictFunAgentError> {
        self.ensure_ready()?;
        if request.max_book_age_ms == 0 || request.max_book_age_ms > MAX_PREPARE_BOOK_AGE_MS {
            return Err(PredictFunAgentError::Invalid(format!(
                "max book age must be between 1 and {MAX_PREPARE_BOOK_AGE_MS} ms"
            )));
        }
        self.facade
            .prepare_fok_order_checked(
                &current_token(&self.token)?,
                self.lifecycle.as_ref(),
                &self.startup,
                request,
            )
            .await
    }

    pub async fn submit_prepared(
        &self,
        prepared: PreparedPredictFunOrder,
    ) -> Result<PredictFunAgentSubmitResult, PredictFunAgentError> {
        self.ensure_ready()?;
        self.facade
            .submit_prepared(&current_token(&self.token)?, prepared)
            .await
    }

    pub async fn cancel_order(&self, venue_order_id: &str) -> Result<(), PredictFunAgentError> {
        if !self.allow_raw_transaction_fallback {
            return Err(PredictFunAgentError::Invalid(
                "sponsored Predict Account cancellation is not available; raw transaction fallback is disabled"
                    .into(),
            ));
        }
        self.facade
            .cancel_order(
                &current_token(&self.token)?,
                venue_order_id,
                &self.rpc_url,
                self.transaction_timeout_secs,
            )
            .await
    }

    pub async fn list_orders(
        &self,
        active_only: bool,
    ) -> Result<Vec<PredictFunOrderRecord>, PredictFunAgentError> {
        self.facade
            .list_orders(&current_token(&self.token)?, active_only)
            .await
    }

    pub async fn list_fills(&self) -> Result<Vec<PredictFunNormalizedFill>, PredictFunAgentError> {
        normalize_matches(
            &self.facade.list_fills(self.account_address).await?,
            self.account_address,
        )
    }

    pub async fn list_positions(
        &self,
        market_id: Option<u64>,
    ) -> Result<Vec<PredictFunPosition>, PredictFunAgentError> {
        self.facade
            .list_positions(&current_token(&self.token)?, market_id)
            .await
    }

    pub async fn get_market(
        &self,
        market_id: u64,
    ) -> Result<crate::http::models::PredictFunMarket, PredictFunAgentError> {
        self.facade.get_market(market_id).await
    }

    pub async fn reconcile(&self) -> Result<PredictFunRuntimeReconciliation, PredictFunAgentError> {
        self.ready.store(false, Ordering::Release);
        clear_account_readiness(&self.account_readiness)?;
        let result = verify_account_and_reconcile(
            self.lifecycle.as_ref(),
            &self.startup,
            self.facade.as_ref(),
            &current_token(&self.token)?,
            self.account_address,
        )
        .await;
        match result {
            Ok((startup_readiness, reconciliation)) => {
                self.account_readiness
                    .write()
                    .map_err(|_| {
                        PredictFunAgentError::Read("PredictFun readiness lock poisoned".into())
                    })?
                    .replace(startup_readiness);
                self.ready.store(
                    self.transport_ready.load(Ordering::Acquire),
                    Ordering::Release,
                );
                Ok(reconciliation)
            }
            Err(error) => Err(error),
        }
    }

    pub fn verified_account_readiness(
        &self,
    ) -> Result<PredictFunStartupReadiness, PredictFunAgentError> {
        read_account_readiness(&self.account_readiness)
    }

    pub fn verified_normalized_balances(
        &self,
    ) -> Result<PredictFunRuntimeBalances, PredictFunAgentError> {
        let readiness = self.verified_account_readiness()?;
        let collateral_balance = normalized_wei_string(readiness.collateral_balance)?;
        Ok(PredictFunRuntimeBalances {
            chain_id: readiness.chain_id,
            collateral_balance,
            collateral_balance_wei: readiness.collateral_balance,
            gas_balance_wei: readiness.gas_balance,
        })
    }

    pub fn verified_allowances(
        &self,
    ) -> Result<Vec<PredictFunApprovalCheck>, PredictFunAgentError> {
        self.verified_account_readiness()
            .map(|readiness| readiness.approvals)
    }

    pub async fn balances(&self) -> Result<PredictFunStartupReadiness, PredictFunAgentError> {
        self.lifecycle
            .startup_readiness(&self.startup)
            .await
            .map_err(|error| PredictFunAgentError::Read(error.to_string()))
    }

    pub async fn normalized_balances(
        &self,
    ) -> Result<PredictFunRuntimeBalances, PredictFunAgentError> {
        let readiness = self.balances().await?;
        let collateral_balance = normalized_wei_string(readiness.collateral_balance)?;
        Ok(PredictFunRuntimeBalances {
            chain_id: readiness.chain_id,
            collateral_balance,
            collateral_balance_wei: readiness.collateral_balance,
            gas_balance_wei: readiness.gas_balance,
        })
    }

    pub async fn allowances(&self) -> Result<Vec<PredictFunApprovalCheck>, PredictFunAgentError> {
        self.allowances_for(&self.startup.required_approvals).await
    }

    pub async fn allowances_for(
        &self,
        steps: &[PredictFunApprovalStep],
    ) -> Result<Vec<PredictFunApprovalCheck>, PredictFunAgentError> {
        self.lifecycle
            .check_approvals(steps)
            .await
            .map_err(|error| PredictFunAgentError::Read(error.to_string()))
    }

    pub async fn disconnect(&mut self) {
        self.ready.store(false, Ordering::Release);
        let _ = clear_account_readiness(&self.account_readiness);
        self.event_task.abort();
        self.session.disconnect().await;
        if let Ok(mut token) = self.token.lock() {
            *token = None;
        }
    }

    fn ensure_ready(&self) -> Result<(), PredictFunAgentError> {
        if !self.is_ready() {
            return Err(PredictFunAgentError::Read(
                "PredictFun private authentication or reconciliation is not ready".into(),
            ));
        }
        Ok(())
    }
}

fn validate_config(config: &PredictFunAgentRuntimeConfig) -> Result<(), PredictFunAgentError> {
    if !config.startup.predict_account
        || config.startup.account != config.account_address
        || config.startup.environment != config.environment
    {
        return Err(PredictFunAgentError::Invalid(
            "runtime requires matching Predict Account startup identity".into(),
        ));
    }
    if config.http_timeout_secs == 0 || config.transaction_timeout_secs == 0 {
        return Err(PredictFunAgentError::Invalid(
            "runtime timeouts must be positive".into(),
        ));
    }
    Ok(())
}

fn current_token(
    token: &Arc<Mutex<Option<SecretString>>>,
) -> Result<SecretString, PredictFunAgentError> {
    token
        .lock()
        .map_err(|_| PredictFunAgentError::Read("PredictFun token lock poisoned".into()))?
        .clone()
        .ok_or_else(|| PredictFunAgentError::Read("PredictFun authentication is not ready".into()))
}

#[allow(clippy::too_many_arguments)]
fn spawn_private_event_task(
    mut source: tokio::sync::mpsc::UnboundedReceiver<PredictFunWsEvent>,
    facade: Arc<PredictFunAgentFacade<PredictFunHttpClient>>,
    token: Arc<Mutex<Option<SecretString>>>,
    transport_ready: Arc<AtomicBool>,
    ready: Arc<AtomicBool>,
    lifecycle: Arc<PredictFunLifecycle<AlloyPredictFunLifecycleBackend>>,
    startup: PredictFunStartupRequirements,
    account_readiness: Arc<RwLock<Option<PredictFunStartupReadiness>>>,
    account: Address,
    output: tokio::sync::mpsc::UnboundedSender<PredictFunRuntimeEvent>,
) -> tokio::task::JoinHandle<()> {
    nautilus_common::live::get_runtime().spawn(async move {
        while let Some(event) = source.recv().await {
            match event {
                PredictFunWsEvent::Message(PredictFunWsMessage::Wallet { event }) => {
                    match normalize_wallet_event(&event, account) {
                        Ok(event) => {
                            if output.send(event).is_err() {
                                break;
                            }
                        }
                        Err(error) => {
                            ready.store(false, Ordering::Release);
                            let _ = output.send(PredictFunRuntimeEvent::Error(error.to_string()));
                        }
                    }
                }
                PredictFunWsEvent::Reconnected => {
                    ready.store(false, Ordering::Release);
                    let _ = clear_account_readiness(&account_readiness);
                    let result = match current_token(&token) {
                        Ok(token) => {
                            verify_account_and_reconcile(
                                lifecycle.as_ref(),
                                &startup,
                                facade.as_ref(),
                                &token,
                                account,
                            )
                            .await
                        }
                        Err(error) => Err(error),
                    };
                    match result {
                        Ok((startup_readiness, _)) => {
                            if let Ok(mut cached) = account_readiness.write() {
                                cached.replace(startup_readiness);
                            } else {
                                let _ = output.send(PredictFunRuntimeEvent::Error(
                                    "PredictFun readiness lock poisoned".into(),
                                ));
                                continue;
                            }
                            if transport_ready.load(Ordering::Acquire) {
                                ready.store(true, Ordering::Release);
                                let _ = output.send(PredictFunRuntimeEvent::Reconciled);
                            }
                        }
                        Err(error) => {
                            let _ = output.send(PredictFunRuntimeEvent::Error(error.to_string()));
                        }
                    }
                }
                PredictFunWsEvent::Error(error) => {
                    ready.store(false, Ordering::Release);
                    let _ = clear_account_readiness(&account_readiness);
                    let _ = output.send(PredictFunRuntimeEvent::Error(error));
                }
                _ => {}
            }
        }
        ready.store(false, Ordering::Release);
        let _ = clear_account_readiness(&account_readiness);
    })
}

fn clear_account_readiness(
    readiness: &RwLock<Option<PredictFunStartupReadiness>>,
) -> Result<(), PredictFunAgentError> {
    *readiness
        .write()
        .map_err(|_| PredictFunAgentError::Read("PredictFun readiness lock poisoned".into()))? =
        None;
    Ok(())
}

fn read_account_readiness(
    readiness: &RwLock<Option<PredictFunStartupReadiness>>,
) -> Result<PredictFunStartupReadiness, PredictFunAgentError> {
    readiness
        .read()
        .map_err(|_| PredictFunAgentError::Read("PredictFun readiness lock poisoned".into()))?
        .clone()
        .ok_or_else(|| PredictFunAgentError::Read("PredictFun account readiness is stale".into()))
}

async fn verify_account_and_reconcile(
    lifecycle: &PredictFunLifecycle<AlloyPredictFunLifecycleBackend>,
    startup: &PredictFunStartupRequirements,
    facade: &PredictFunAgentFacade<PredictFunHttpClient>,
    token: &SecretString,
    account: Address,
) -> Result<(PredictFunStartupReadiness, PredictFunRuntimeReconciliation), PredictFunAgentError> {
    let readiness = lifecycle
        .startup_readiness(startup)
        .await
        .map_err(|error| {
            PredictFunAgentError::Read(format!("PredictFun startup readiness failed: {error}"))
        })?;
    let reconciliation = reconcile_normalized(facade, token, account)
        .await
        .map_err(|error| {
            PredictFunAgentError::Read(format!("PredictFun REST reconciliation failed: {error}"))
        })?;
    Ok((readiness, reconciliation))
}

async fn reconcile_normalized(
    facade: &PredictFunAgentFacade<PredictFunHttpClient>,
    token: &SecretString,
    account: Address,
) -> Result<PredictFunRuntimeReconciliation, PredictFunAgentError> {
    let PredictFunAgentReconciliation {
        orders,
        fills,
        positions,
        activity,
    } = facade.reconcile(token).await?;
    let normalized = normalize_matches(&fills, account)?;
    let order_evidence = normalize_activity(&activity)?;
    Ok(PredictFunRuntimeReconciliation {
        orders,
        fills: normalized,
        order_evidence,
        positions,
        activity,
    })
}

fn normalize_matches(
    matches: &[PredictFunMatch],
    account: Address,
) -> Result<Vec<PredictFunNormalizedFill>, PredictFunAgentError> {
    let expected = format!("{account:#x}");
    let mut fills = Vec::new();
    let mut seen = HashSet::new();
    for matched in matches {
        let own_legs = std::iter::once(&matched.taker)
            .chain(matched.makers.iter())
            .filter(|leg| leg.signer.eq_ignore_ascii_case(&expected));
        for leg in own_legs {
            let settlement_id = matched.settlement_id.as_deref().ok_or_else(|| {
                PredictFunAgentError::Read(format!(
                    "local match {} is not settled",
                    matched.transaction_hash
                ))
            })?;
            let fill = normalize_match_leg(matched, leg, settlement_id)?;
            if seen.insert(fill.event_id.clone()) {
                fills.push(fill);
            }
        }
    }
    Ok(fills)
}

fn normalize_match_leg(
    matched: &PredictFunMatch,
    leg: &PredictFunMatchOrder,
    settlement_id: &str,
) -> Result<PredictFunNormalizedFill, PredictFunAgentError> {
    let fee = leg.fee.as_ref().ok_or_else(|| {
        PredictFunAgentError::Read(format!(
            "settled local match {settlement_id} has no exact fee evidence"
        ))
    })?;
    normalized_fill(
        settlement_id,
        None,
        &leg.hash,
        matched.market.id,
        leg.quote_type,
        wei_decimal(&leg.amount, "match gross quantity")?,
        wei_decimal(&leg.price, "match price")?,
        wei_decimal(&fee.amount, "match fee")?,
        fee.asset_type,
        None,
    )
}

fn normalize_wallet_event(
    event: &PredictFunWalletEvent,
    account: Address,
) -> Result<PredictFunRuntimeEvent, PredictFunAgentError> {
    let wallet = Address::from_str(&event.wallet_address).map_err(|error| {
        PredictFunAgentError::Read(format!("invalid wallet event account: {error}"))
    })?;
    if wallet != account {
        return Err(PredictFunAgentError::Read(format!(
            "wallet event account {wallet:#x} does not match {account:#x}"
        )));
    }
    let has_complete_fill_evidence = event
        .details
        .settlement_id
        .as_deref()
        .is_some_and(|value| !value.is_empty())
        && event
            .details
            .fill
            .as_ref()
            .and_then(|fill| fill.fee.as_ref())
            .is_some();
    if event.event_type != "orderTransactionSuccess" || !has_complete_fill_evidence {
        let reason = if event.event_type == "orderTransactionSuccess" && !has_complete_fill_evidence
        {
            Some(
                event
                    .details
                    .reason
                    .clone()
                    .unwrap_or_else(|| "SETTLEMENT_EVIDENCE_INCOMPLETE".into()),
            )
        } else {
            event.details.reason.clone()
        };
        return Ok(PredictFunRuntimeEvent::Order {
            event_type: event.event_type.clone(),
            venue_order_id: event.order_id.clone(),
            native_order_hash: event.order_hash.to_ascii_lowercase(),
            market_id: event.details.market_id,
            side: event.details.quote_type,
            quantity: normalized_decimal_string(&event.details.quantity, "wallet order quantity")?,
            price: normalized_decimal_string(&event.details.price, "wallet order price")?,
            reason,
            timestamp_ms: event.timestamp,
        });
    }
    let settlement_id = event.details.settlement_id.as_deref().ok_or_else(|| {
        PredictFunAgentError::Read("successful wallet event has no settlement ID".into())
    })?;
    let fill = event.details.fill.as_ref().ok_or_else(|| {
        PredictFunAgentError::Read("successful wallet event has no fill evidence".into())
    })?;
    let fee = fill.fee.as_ref().ok_or_else(|| {
        PredictFunAgentError::Read("successful wallet fill has no exact fee evidence".into())
    })?;
    Ok(PredictFunRuntimeEvent::Fill(normalized_fill(
        settlement_id,
        Some(event.order_id.clone()),
        &event.order_hash,
        event.details.market_id,
        event.details.quote_type,
        wei_decimal(&fill.executed_size_wei, "wallet gross quantity")?,
        wei_decimal(&fill.executed_price_wei, "wallet price")?,
        wei_decimal(&fee.amount_wei, "wallet fee")?,
        fee.asset_type,
        Some(event.timestamp),
    )?))
}

#[allow(clippy::too_many_arguments)]
fn normalized_fill(
    settlement_id: &str,
    venue_order_id: Option<String>,
    order_hash: &str,
    market_id: u64,
    side: PredictFunQuoteType,
    gross: Decimal,
    price: Decimal,
    fee: Decimal,
    fee_asset: PredictFunFeeAsset,
    timestamp_ms: Option<u64>,
) -> Result<PredictFunNormalizedFill, PredictFunAgentError> {
    if settlement_id.is_empty() || order_hash.is_empty() {
        return Err(PredictFunAgentError::Read(
            "fill settlement ID or order hash is empty".into(),
        ));
    }
    if gross <= Decimal::ZERO || price <= Decimal::ZERO || fee < Decimal::ZERO {
        return Err(PredictFunAgentError::Read(
            "fill quantity, price or fee is invalid".into(),
        ));
    }
    if !matches!(
        (side, fee_asset),
        (PredictFunQuoteType::Bid, PredictFunFeeAsset::Shares)
            | (PredictFunQuoteType::Ask, PredictFunFeeAsset::Collateral)
    ) {
        return Err(PredictFunAgentError::Read(
            "fill fee asset does not match Predict.fun side semantics".into(),
        ));
    }
    let net = match fee_asset {
        PredictFunFeeAsset::Shares => gross.checked_sub(fee).ok_or_else(|| {
            PredictFunAgentError::Read("share fee exceeds executed quantity".into())
        })?,
        PredictFunFeeAsset::Collateral => gross,
    };
    if net <= Decimal::ZERO {
        return Err(PredictFunAgentError::Read(
            "share fee consumes executed quantity".into(),
        ));
    }
    let normalized_hash = order_hash.to_ascii_lowercase();
    Ok(PredictFunNormalizedFill {
        event_id: format!("{settlement_id}:{normalized_hash}"),
        settlement_id: settlement_id.to_string(),
        venue_order_id,
        native_order_hash: normalized_hash,
        market_id,
        side,
        gross_quantity: gross.normalize().to_string(),
        net_quantity: net.normalize().to_string(),
        price: price.normalize().to_string(),
        native_fee_amount: fee.normalize().to_string(),
        fee_asset,
        timestamp_ms,
    })
}

fn parse_decimal(value: &str, field: &str) -> Result<Decimal, PredictFunAgentError> {
    value
        .parse::<Decimal>()
        .map_err(|error| PredictFunAgentError::Read(format!("invalid {field} {value}: {error}")))
}

fn wei_decimal(value: &str, field: &str) -> Result<Decimal, PredictFunAgentError> {
    let wei = U256::from_str(value)
        .map_err(|error| PredictFunAgentError::Read(format!("invalid {field}: {error}")))?;
    wei_to_decimal(wei)
        .map_err(|error| PredictFunAgentError::Read(format!("invalid {field}: {error}")))
}

fn normalized_wei_string(value: U256) -> Result<String, PredictFunAgentError> {
    wei_to_decimal(value)
        .map(|value| value.normalize().to_string())
        .map_err(|error| PredictFunAgentError::Read(error.to_string()))
}

fn normalized_decimal_string(value: &str, field: &str) -> Result<String, PredictFunAgentError> {
    parse_decimal(value, field).map(|value| value.normalize().to_string())
}

fn normalize_activity(
    activity: &[PredictFunAccountActivity],
) -> Result<Vec<PredictFunNormalizedOrderEvidence>, PredictFunAgentError> {
    activity
        .iter()
        .filter_map(|entry| {
            let order = entry.order.as_ref()?;
            let market = entry.market.as_ref()?;
            let outcome = entry.outcome.as_ref()?;
            Some((entry, order, market, outcome))
        })
        .map(|(entry, order, market, outcome)| {
            let status = match entry.name.as_str() {
                "NO_MARKET_MATCH" => "NO_FILL",
                "CREATE" => "ACCEPTED",
                "CANCEL" => "CANCELLED",
                "MATCH" => "FILLED",
                _ => "UNKNOWN",
            }
            .to_string();
            let reason =
                (entry.name != "CREATE" && entry.name != "MATCH").then(|| entry.name.clone());
            let venue_order_id = entry.order_id.clone().or_else(|| order.id.clone());
            let native_order_hash = entry
                .order_hash
                .clone()
                .or_else(|| order.hash.clone())
                .map(|hash| hash.to_ascii_lowercase());
            let quantity = wei_decimal(&order.amount, "activity order quantity")?
                .normalize()
                .to_string();
            let price = wei_decimal(&order.price, "activity order price")?
                .normalize()
                .to_string();
            let identity = native_order_hash
                .as_deref()
                .or(venue_order_id.as_deref())
                .unwrap_or(outcome.on_chain_id.as_str());
            Ok(PredictFunNormalizedOrderEvidence {
                event_id: format!(
                    "activity:{}:{}:{}:{identity}",
                    entry.created_at, market.id, entry.name
                ),
                status,
                reason,
                venue_order_id,
                native_order_hash,
                market_id: market.id,
                token_id: outcome.on_chain_id.clone(),
                side: order.quote_type,
                quantity,
                price,
                occurred_at: entry.created_at.clone(),
                transaction_hash: entry.transaction_hash.clone(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rust_decimal_macros::dec;
    use tokio::sync::Notify;

    use super::*;

    const ACCOUNT: &str = "0x0000000000000000000000000000000000000001";

    #[tokio::test]
    async fn account_snapshot_fails_closed_without_waiting_for_refresh() {
        let snapshot = Arc::new(RwLock::new(Some(PredictFunStartupReadiness {
            chain_id: 56,
            gas_balance: U256::ZERO,
            collateral_balance: U256::from(1_u64),
            kernel_account_id: None,
            kernel_implementation: None,
            approvals: vec![],
        })));
        let refresh_started = Arc::new(Notify::new());
        let finish_refresh = Arc::new(Notify::new());
        let task_snapshot = Arc::clone(&snapshot);
        let task_started = Arc::clone(&refresh_started);
        let task_finish = Arc::clone(&finish_refresh);
        let refresh = tokio::spawn(async move {
            clear_account_readiness(&task_snapshot).unwrap();
            task_started.notify_one();
            task_finish.notified().await;
        });
        refresh_started.notified().await;

        let result = tokio::time::timeout(Duration::from_millis(10), async {
            read_account_readiness(&snapshot)
        })
        .await;

        assert!(result.is_ok());
        assert!(result.unwrap().unwrap_err().to_string().contains("stale"));
        finish_refresh.notify_one();
        refresh.await.unwrap();
    }

    #[test]
    fn share_fee_is_subtracted_from_gross_fill() {
        let fill = normalized_fill(
            "settlement-1",
            Some("order-1".into()),
            "0xAbC",
            42,
            PredictFunQuoteType::Bid,
            dec!(5),
            dec!(0.42),
            dec!(0.02),
            PredictFunFeeAsset::Shares,
            Some(1),
        )
        .unwrap();

        assert_eq!(fill.event_id, "settlement-1:0xabc");
        assert_eq!(fill.gross_quantity, "5");
        assert_eq!(fill.net_quantity, "4.98");
        assert_eq!(fill.native_fee_amount, "0.02");
    }

    #[test]
    fn collateral_fee_does_not_change_net_shares() {
        let fill = normalized_fill(
            "settlement-1",
            None,
            "0xabc",
            42,
            PredictFunQuoteType::Ask,
            dec!(5),
            dec!(0.42),
            dec!(0.01),
            PredictFunFeeAsset::Collateral,
            None,
        )
        .unwrap();

        assert_eq!(fill.net_quantity, "5");
    }

    #[test]
    fn missing_or_excess_share_fee_fails_closed() {
        let error = normalized_fill(
            "settlement-1",
            None,
            "0xabc",
            42,
            PredictFunQuoteType::Bid,
            dec!(1),
            dec!(0.5),
            dec!(1),
            PredictFunFeeAsset::Shares,
            None,
        )
        .unwrap_err();

        assert!(error.to_string().contains("consumes executed quantity"));
    }

    #[test]
    fn fee_asset_must_match_buy_or_sell_semantics() {
        let error = normalized_fill(
            "settlement-1",
            None,
            "0xabc",
            42,
            PredictFunQuoteType::Bid,
            dec!(1),
            dec!(0.5),
            dec!(0.01),
            PredictFunFeeAsset::Collateral,
            None,
        )
        .unwrap_err();

        assert!(error.to_string().contains("fee asset"));
    }

    #[test]
    fn settled_match_without_exact_fee_fails_closed() {
        let leg = PredictFunMatchOrder {
            quote_type: PredictFunQuoteType::Bid,
            amount: "5".into(),
            price: "0.42".into(),
            outcome: crate::http::models::PredictFunOutcome {
                name: "YES".into(),
                index_set: 1,
                on_chain_id: "12345".into(),
                status: None,
            },
            signer: "0x0000000000000000000000000000000000000001".into(),
            hash: "0xabc".into(),
            fee: None,
        };
        let matched = PredictFunMatch {
            market: crate::http::models::PredictFunMatchMarket { id: 42 },
            taker: leg.clone(),
            amount_filled: "5".into(),
            price_executed: "0.42".into(),
            makers: vec![],
            transaction_hash: "0xtx".into(),
            settlement_id: Some("settlement-1".into()),
            executed_at: "2026-01-01T00:00:00Z".into(),
        };

        let error = normalize_match_leg(&matched, &leg, "settlement-1").unwrap_err();

        assert!(error.to_string().contains("no exact fee evidence"));
    }

    #[test]
    fn settled_match_uses_documented_wei_units() {
        let leg = PredictFunMatchOrder {
            quote_type: PredictFunQuoteType::Bid,
            amount: "5102100000000000000".into(),
            price: "180000000000000000".into(),
            outcome: crate::http::models::PredictFunOutcome {
                name: "NO".into(),
                index_set: 2,
                on_chain_id: "12345".into(),
                status: None,
            },
            signer: ACCOUNT.into(),
            hash: "0x072ef53f892ceb61d8571153248e9f6e8d7fdfdb603e05b61e773b8310e28709".into(),
            fee: Some(crate::http::models::PredictFunMatchFee {
                amount: "91837800000000000".into(),
                asset_type: PredictFunFeeAsset::Shares,
            }),
        };
        let matched = PredictFunMatch {
            market: crate::http::models::PredictFunMatchMarket { id: 1_892_959 },
            taker: leg.clone(),
            amount_filled: leg.amount.clone(),
            price_executed: leg.price.clone(),
            makers: vec![],
            transaction_hash: "0x2b2028dc5b4c64fc8a6b8d628a51b20a3a45167e5aa6a42d4501fd132bdc85e1"
                .into(),
            settlement_id: Some("01a063a2-16c2-79f2-bddd-ce60ef4b920b".into()),
            executed_at: "2026-09-02T19:39:23Z".into(),
        };

        let fill =
            normalize_match_leg(&matched, &leg, "01a063a2-16c2-79f2-bddd-ce60ef4b920b").unwrap();

        assert_eq!(fill.gross_quantity, "5.1021");
        assert_eq!(fill.native_fee_amount, "0.0918378");
        assert_eq!(fill.net_quantity, "5.0102622");
        assert_eq!(fill.price, "0.18");
    }

    #[test]
    fn collateral_wei_is_exposed_in_human_usdt_units() {
        let raw = U256::from_str("10276363641963630026").unwrap();

        assert_eq!(normalized_wei_string(raw).unwrap(), "10.276363641963630026");
    }

    #[test]
    fn no_market_match_activity_is_no_fill_evidence_not_a_fill() {
        // Shape observed from account activity after the venue acknowledged order 3032452686.
        let activity: PredictFunAccountActivity = serde_json::from_str(
            r#"{
                "name":"NO_MARKET_MATCH",
                "createdAt":"2026-09-02T14:58:16.167Z",
                "transactionHash":null,
                "amountFilled":null,
                "priceExecuted":null,
                "order":{
                    "quoteType":"BID",
                    "amount":"5000000000000000000",
                    "price":"390000000000000000",
                    "fee":null
                },
                "market":{"id":1885421},
                "outcome":{"name":"Up","indexSet":1,"onChainId":"12345"}
            }"#,
        )
        .unwrap();

        let evidence = normalize_activity(&[activity]).unwrap();

        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].status, "NO_FILL");
        assert_eq!(evidence[0].reason.as_deref(), Some("NO_MARKET_MATCH"));
        assert_eq!(evidence[0].quantity, "5");
        assert_eq!(evidence[0].price, "0.39");
        assert_eq!(evidence[0].side, PredictFunQuoteType::Bid);
        assert!(evidence[0].venue_order_id.is_none());
        assert!(evidence[0].native_order_hash.is_none());
    }

    #[test]
    fn private_order_not_accepted_retains_correlated_reason_and_order_terms() {
        let event = PredictFunWalletEvent {
            event_type: "orderNotAccepted".to_string(),
            order_id: "3032452686".to_string(),
            order_hash: "0xAbC".to_string(),
            wallet_address: ACCOUNT.to_string(),
            timestamp: 1_788_359_896_167,
            details: crate::http::models::PredictFunWalletDetails {
                market_id: 1_885_421,
                outcome_index: 0,
                outcome: "Up".to_string(),
                quote_type: PredictFunQuoteType::Bid,
                quantity: "5".to_string(),
                quantity_filled: "0".to_string(),
                price: "0.39".to_string(),
                value: "1.95".to_string(),
                value_filled: "0".to_string(),
                strategy_type: crate::common::enums::PredictFunStrategy::Market,
                settlement_id: None,
                fill: None,
                is_maker: Some(false),
                reason: Some("NO_MARKET_MATCH".to_string()),
            },
        };

        let normalized =
            normalize_wallet_event(&event, Address::from_str(ACCOUNT).unwrap()).unwrap();
        let PredictFunRuntimeEvent::Order {
            event_type,
            venue_order_id,
            native_order_hash,
            market_id,
            side,
            quantity,
            price,
            reason,
            ..
        } = normalized
        else {
            panic!("expected order evidence, not fill evidence");
        };

        assert_eq!(event_type, "orderNotAccepted");
        assert_eq!(venue_order_id, "3032452686");
        assert_eq!(native_order_hash, "0xabc");
        assert_eq!(market_id, 1_885_421);
        assert_eq!(side, PredictFunQuoteType::Bid);
        assert_eq!(quantity, "5");
        assert_eq!(price, "0.39");
        assert_eq!(reason.as_deref(), Some("NO_MARKET_MATCH"));
    }

    #[test]
    fn live_success_without_settlement_evidence_is_an_order_update() {
        // Reconstructs the confirmed live shape for order 3042606137; the raw payload was not
        // retained, but it lacked the documented settlement ID and fill payload.
        let payload = r#"{
            "type":"M",
            "topic":"predictWalletEvents/redacted",
            "data":{
                "type":"orderTransactionSuccess",
                "orderId":"3042606137",
                "orderHash":"0x0000000000000000000000000000000000000000000000000000003042606137",
                "walletAddress":"0x0000000000000000000000000000000000000001",
                "timestamp":1788377964000,
                "details":{
                    "marketId":1892959,
                    "outcomeIndex":1,
                    "outcome":"NO",
                    "quoteType":"BID",
                    "quantity":"5.1021",
                    "quantityFilled":"0",
                    "price":"0.19",
                    "value":"0.969399",
                    "valueFilled":"0",
                    "strategyType":"MARKET",
                    "isMaker":false
                }
            }
        }"#;
        let PredictFunWsMessage::Wallet { event } = PredictFunWsMessage::parse(payload).unwrap()
        else {
            panic!("expected wallet event");
        };

        let normalized =
            normalize_wallet_event(&event, Address::from_str(ACCOUNT).unwrap()).unwrap();
        let PredictFunRuntimeEvent::Order {
            event_type,
            venue_order_id,
            native_order_hash,
            market_id,
            quantity,
            price,
            reason,
            ..
        } = normalized
        else {
            panic!("expected order update, not fill evidence");
        };

        assert_eq!(event_type, "orderTransactionSuccess");
        assert_eq!(venue_order_id, "3042606137");
        assert_eq!(
            native_order_hash,
            "0x0000000000000000000000000000000000000000000000000000003042606137"
        );
        assert_eq!(market_id, 1_892_959);
        assert_eq!(quantity, "5.1021");
        assert_eq!(price, "0.19");
        assert_eq!(reason.as_deref(), Some("SETTLEMENT_EVIDENCE_INCOMPLETE"));

        let mut missing_fee = (*event).clone();
        missing_fee.details.settlement_id = Some("settlement-3042606137".to_string());
        missing_fee.details.fill = Some(crate::http::models::PredictFunFill {
            executed_price_wei: "190000000000000000".to_string(),
            executed_size_wei: "5102100000000000000".to_string(),
            executed_value_wei: "969399000000000000".to_string(),
            fee: None,
        });
        assert!(matches!(
            normalize_wallet_event(&missing_fee, Address::from_str(ACCOUNT).unwrap()).unwrap(),
            PredictFunRuntimeEvent::Order { .. }
        ));

        missing_fee.details.fill.as_mut().unwrap().fee = Some(crate::http::models::PredictFunFee {
            amount_wei: "20000000000000000".to_string(),
            asset_type: PredictFunFeeAsset::Shares,
        });
        let PredictFunRuntimeEvent::Fill(fill) =
            normalize_wallet_event(&missing_fee, Address::from_str(ACCOUNT).unwrap()).unwrap()
        else {
            panic!("expected complete settlement evidence to normalize as a fill");
        };
        assert_eq!(fill.settlement_id, "settlement-3042606137");
        assert_eq!(fill.native_fee_amount, "0.02");
        assert_eq!(fill.net_quantity, "5.0821");
    }
}
