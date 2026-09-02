use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, Mutex},
};

use alloy_primitives::{Address, U256};
use async_trait::async_trait;
use nautilus_common::{
    clients::ExecutionClient,
    live::{get_runtime, runner::get_exec_event_sender},
    messages::execution::{
        BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
        GenerateFillReportsBuilder, GenerateOrderStatusReport, GenerateOrderStatusReports,
        GeneratePositionStatusReports, GeneratePositionStatusReportsBuilder, ModifyOrder,
        QueryAccount, SubmitOrder, SubmitOrderList,
    },
};
use nautilus_core::{
    Params, UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{
        AccountType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType,
        PositionSideSpecified, TimeInForce,
    },
    events::{OrderEventAny, OrderInitialized},
    identifiers::{AccountId, ClientId, PositionId, TradeId, Venue, VenueOrderId},
    instruments::InstrumentAny,
    orders::{Order, OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance, Money, Price, Quantity},
};
use rand::RngExt;
use rust_decimal::Decimal;
use tokio::task::JoinHandle;

use super::{
    cancellation::{CancelRequest, cancel_groups, collateral_balance, verify_rpc},
    session::PredictFunExecutionSession,
};
use crate::{
    common::{
        consts::{PREDICTFUN_VENUE, usdt},
        enums::{
            PredictFunAccountType, PredictFunSide, PredictFunSignatureType, PredictFunStrategy,
        },
        parse::wei_to_decimal,
    },
    config::{PredictFunExecClientConfig, SecretString},
    http::{
        PredictFunHttpClient,
        models::{
            PredictFunContractOrder, PredictFunCreateOrderData, PredictFunFeeAsset,
            PredictFunMatchOrder, PredictFunOrderRecord,
        },
        parse::parse_timestamp,
    },
    signing::{
        eip712::{PredictFunOrderSigner, order_hash},
        order_builder::{limit_order_amounts, market_order_amounts_by_quantity},
    },
    websocket::{PredictFunWsEvent, messages::PredictFunWalletEvent, parse::outcome_book_levels},
};

const LIMIT_EXPIRATION_FALLBACK_SECS: u64 = 4_102_444_800;
const MARKET_EXPIRATION_SECS: u64 = 300;
const MAX_SALT: u32 = 2_147_483_648;

#[derive(Debug, Clone)]
struct ExecutionInstrumentMeta {
    market_id: u64,
    token_id: String,
    price_precision: u8,
    is_yes: bool,
    is_neg_risk: bool,
    is_yield_bearing: bool,
    fee_rate_bps: u32,
}

/// Live PredictFun execution client.
///
/// Order cancellation remains fail-closed unless venue-side off-chain removal
/// and authoritative on-chain invalidation can both be reconciled.
#[derive(Debug)]
pub struct PredictFunExecutionClient {
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    config: PredictFunExecClientConfig,
    emitter: ExecutionEventEmitter,
    http_client: PredictFunHttpClient,
    signer: Arc<PredictFunOrderSigner>,
    token: Arc<Mutex<Option<SecretString>>>,
    account_address: Arc<Mutex<Option<Address>>>,
    instruments:
        Arc<Mutex<HashMap<nautilus_model::identifiers::InstrumentId, ExecutionInstrumentMeta>>>,
    order_ids: Arc<Mutex<HashMap<String, OrderAny>>>,
    accepted_order_ids: Arc<Mutex<HashSet<String>>>,
    fill_ids: Arc<Mutex<HashSet<String>>>,
    session: Option<PredictFunExecutionSession>,
    wallet_task: Option<JoinHandle<()>>,
    private_ready: Arc<AtomicBool>,
}

impl PredictFunExecutionClient {
    pub fn new(
        core: ExecutionClientCore,
        config: PredictFunExecClientConfig,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let private_key = config.private_key.as_ref().expect("validated private_key");
        let http_client = PredictFunHttpClient::new(
            config.api_url(),
            config.api_key.as_ref(),
            config.request_timeout_secs,
        )?;
        let signer = Arc::new(PredictFunOrderSigner::new(private_key.expose())?);
        let clock = get_atomic_clock_realtime();
        let emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            AccountType::Cash,
            Some(usdt()),
        );
        Ok(Self {
            core,
            clock,
            config,
            emitter,
            http_client,
            signer,
            token: Arc::new(Mutex::new(None)),
            account_address: Arc::new(Mutex::new(None)),
            instruments: Arc::new(Mutex::new(HashMap::new())),
            order_ids: Arc::new(Mutex::new(HashMap::new())),
            accepted_order_ids: Arc::new(Mutex::new(HashSet::new())),
            fill_ids: Arc::new(Mutex::new(HashSet::new())),
            session: None,
            wallet_task: None,
            private_ready: Arc::new(AtomicBool::new(false)),
        })
    }

    fn order_from_init(init: &OrderInitialized) -> anyhow::Result<OrderAny> {
        Ok(OrderAny::from_events(vec![OrderEventAny::Initialized(
            init.clone(),
        )])?)
    }

    fn validate_order(order: &OrderAny) -> anyhow::Result<()> {
        if order.is_reduce_only() || order.is_quote_quantity() {
            anyhow::bail!("PredictFun does not support reduce-only or quote-quantity orders");
        }
        match order.order_type() {
            OrderType::Limit => {
                if !matches!(order.time_in_force(), TimeInForce::Gtc | TimeInForce::Gtd) {
                    anyhow::bail!("PredictFun LIMIT orders require GTC or GTD");
                }
            }
            OrderType::Market => {
                if order.time_in_force() != TimeInForce::Fok {
                    anyhow::bail!("PredictFun MARKET orders require FOK");
                }
                if order.is_post_only() {
                    anyhow::bail!("PredictFun MARKET orders cannot be post-only");
                }
            }
            other => anyhow::bail!("PredictFun does not support {other:?} orders"),
        }
        Ok(())
    }

    fn instrument_meta(
        &self,
        instrument_id: nautilus_model::identifiers::InstrumentId,
    ) -> anyhow::Result<ExecutionInstrumentMeta> {
        self.instruments
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun instrument lock poisoned"))?
            .get(&instrument_id)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("PredictFun execution instrument not registered: {instrument_id}")
            })
    }

    fn auth_token(&self) -> anyhow::Result<SecretString> {
        self.token
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun token lock poisoned"))?
            .clone()
            .ok_or_else(|| anyhow::anyhow!("PredictFun execution client is not authenticated"))
    }

    fn deny(&self, order: &OrderAny, error: impl std::fmt::Display) {
        self.emitter.emit_order_denied(order, &error.to_string());
    }
}

fn ensure_private_execution_ready(private_ready: &AtomicBool) -> anyhow::Result<()> {
    if !private_ready.load(Ordering::Acquire) {
        anyhow::bail!("PredictFun private execution stream is not ready");
    }
    Ok(())
}

#[async_trait(?Send)]
impl ExecutionClient for PredictFunExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected() && self.private_ready.load(Ordering::Acquire)
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        *PREDICTFUN_VENUE
    }

    fn oms_type(&self) -> OmsType {
        OmsType::Netting
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
        info: Option<Params>,
    ) -> anyhow::Result<()> {
        self.emitter
            .try_emit_account_state(balances, margins, reported, ts_event, info)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }
        self.emitter.set_sender(get_exec_event_sender());
        self.core.set_started();
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }
        self.core.set_stopped();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.order_ids
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun order identity lock poisoned"))?
            .clear();
        self.fill_ids
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun settlement lock poisoned"))?
            .clear();
        self.accepted_order_ids
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun accepted-order lock poisoned"))?
            .clear();
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.core.is_connected() {
            return Ok(());
        }
        verify_rpc(
            self.config.rpc_url.as_ref().expect("validated rpc_url"),
            self.config.environment,
        )
        .await?;
        let mut session = PredictFunExecutionSession::connect(
            self.http_client.clone(),
            self.config.websocket_url()?,
            self.config.api_key.clone(),
            Arc::clone(&self.signer),
            self.config.environment,
            self.config.account_type,
            self.config.account_address.as_deref(),
            self.config.transport_backend,
            Arc::clone(&self.token),
            Arc::clone(&self.private_ready),
        )
        .await?;
        *self
            .account_address
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun account lock poisoned"))? =
            Some(session.account_address());
        let initial_account = async {
            let token = self.auth_token()?;
            let balance = load_account_balance(
                &self.http_client,
                &token,
                self.config.rpc_url.as_ref().expect("validated rpc_url"),
                self.config.environment,
                session.account_address(),
            )
            .await?;
            let reconciliation = self
                .generate_mass_status(Some(24 * 60))
                .await?
                .ok_or_else(|| anyhow::anyhow!("PredictFun reconciliation returned no status"))?;
            if !reconciliation.reports_complete() {
                anyhow::bail!("PredictFun initial reconciliation is incomplete");
            }
            self.emitter.emit_account_state(
                vec![balance],
                vec![],
                true,
                self.clock.get_time_ns(),
                None,
            );
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(error) = initial_account {
            session.disconnect().await;
            if let Ok(mut token) = self.token.lock() {
                *token = None;
            }
            if let Ok(mut account) = self.account_address.lock() {
                *account = None;
            }
            return Err(error.context("failed initial PredictFun account state"));
        }
        let mut events = match session.take_events() {
            Ok(events) => events,
            Err(error) => {
                session.disconnect().await;
                if let Ok(mut token) = self.token.lock() {
                    *token = None;
                }
                if let Ok(mut account) = self.account_address.lock() {
                    *account = None;
                }
                return Err(error);
            }
        };
        let emitter = self.emitter.clone();
        let order_ids = Arc::clone(&self.order_ids);
        let accepted_order_ids = Arc::clone(&self.accepted_order_ids);
        let fill_ids = Arc::clone(&self.fill_ids);
        let instruments = Arc::clone(&self.instruments);
        let http_client = self.http_client.clone();
        let token = Arc::clone(&self.token);
        let private_ready = Arc::clone(&self.private_ready);
        let rpc_url = self.config.rpc_url.clone().expect("validated rpc_url");
        let environment = self.config.environment;
        let account_address = session.account_address();
        self.wallet_task = Some(get_runtime().spawn(async move {
            while let Some(event) = events.recv().await {
                match event {
                    PredictFunWsEvent::Message(
                    crate::websocket::messages::PredictFunWsMessage::Wallet { event },
                    ) => {
                        if let Err(error) = handle_wallet_event(
                            &event,
                            &emitter,
                            &order_ids,
                            &accepted_order_ids,
                            &fill_ids,
                            &instruments,
                            account_address,
                        ) {
                            log::error!("PredictFun wallet event rejected: {error}");
                        }
                    }
                    PredictFunWsEvent::Reconnected => {
                        let fresh_token = token.lock().ok().and_then(|guard| guard.clone());
                        let Some(fresh_token) = fresh_token else {
                            log::error!("PredictFun reconnect completed without an auth token");
                            continue;
                        };
                        match reconcile_private_stream(
                            &http_client,
                            &fresh_token,
                            &rpc_url,
                            environment,
                            account_address,
                            &instruments,
                        )
                        .await
                        {
                            Ok(balance) => {
                                emitter.emit_account_state(
                                    vec![balance],
                                    vec![],
                                    true,
                                    get_atomic_clock_realtime().get_time_ns(),
                                    None,
                                );
                                private_ready.store(true, Ordering::Release);
                            }
                            Err(error) => log::error!(
                                "PredictFun reconnect reconciliation failed; private execution remains unavailable: {error}"
                            ),
                        }
                    }
                    PredictFunWsEvent::Error(error) => {
                        log::error!("PredictFun execution WebSocket error: {error}");
                    }
                    _ => {}
                }
            }
            private_ready.store(false, Ordering::Release);
        }));
        self.session = Some(session);
        self.private_ready.store(true, Ordering::Release);
        self.core.set_connected();
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.core.set_disconnected();
        self.private_ready.store(false, Ordering::Release);
        if let Some(session) = &mut self.session {
            session.disconnect().await;
        }
        self.session = None;
        if let Some(task) = self.wallet_task.take() {
            task.abort();
        }
        *self
            .token
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun token lock poisoned"))? = None;
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = Self::order_from_init(&cmd.order_init)?;
        if let Err(error) = ensure_private_execution_ready(&self.private_ready) {
            self.deny(&order, error);
            return Ok(());
        }
        if let Err(error) = Self::validate_order(&order) {
            self.deny(&order, error);
            return Ok(());
        }
        let meta = match self.instrument_meta(cmd.instrument_id) {
            Ok(meta) => meta,
            Err(error) => {
                self.deny(&order, error);
                return Ok(());
            }
        };
        let token = self
            .token
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun token lock poisoned"))?
            .clone();
        let account = *self
            .account_address
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun account lock poisoned"))?;
        let (Some(token), Some(account)) = (token, account) else {
            self.deny(&order, "PredictFun execution client is not authenticated");
            return Ok(());
        };
        self.emitter.emit_order_submitted(&order);
        let client = self.http_client.clone();
        let signer = Arc::clone(&self.signer);
        let emitter = self.emitter.clone();
        let order_ids = Arc::clone(&self.order_ids);
        let accepted_order_ids = Arc::clone(&self.accepted_order_ids);
        let config = self.config.clone();
        get_runtime().spawn(async move {
            let result = build_order_data(&client, &signer, &config, account, &order, &meta).await;
            let data = match result {
                Ok(data) => data,
                Err(error) => {
                    emitter.emit_order_rejected(
                        &order,
                        &error.to_string(),
                        get_atomic_clock_realtime().get_time_ns(),
                        false,
                    );
                    return;
                }
            };
            if let Some(order_hash) = data.order.hash.clone()
                && let Ok(mut identities) = order_ids.lock()
            {
                identities.insert(order_hash, order.clone());
            }
            match client.create_order(&token, data).await {
                Ok(response) => {
                    let venue_order_id = VenueOrderId::new(response.order_id.as_str());
                    if let Ok(mut identities) = order_ids.lock() {
                        identities.insert(response.order_id.clone(), order.clone());
                        identities.insert(response.order_hash, order.clone());
                    }
                    let should_emit = accepted_order_ids
                        .lock()
                        .is_ok_and(|mut ids| ids.insert(response.order_id));
                    if should_emit {
                        emitter.emit_order_accepted(
                            &order,
                            venue_order_id,
                            get_atomic_clock_realtime().get_time_ns(),
                        );
                    }
                }
                Err(error) => {
                    // A timeout or 5xx after POST is ambiguous: do not emit a
                    // false terminal rejection. Reconciliation can resolve it
                    // from the deterministic order hash or wallet stream.
                    if error.is_definitive_rejection() {
                        emitter.emit_order_rejected(
                            &order,
                            &error.to_string(),
                            get_atomic_clock_realtime().get_time_ns(),
                            false,
                        );
                    } else {
                        log::error!(
                            "PredictFun order submission outcome is ambiguous for {}: {error}",
                            order.client_order_id()
                        );
                    }
                }
            }
        });
        Ok(())
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        anyhow::bail!(
            "PredictFun order-list submission is unsupported; submit orders individually: {}",
            cmd.order_list.id
        )
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let order = self.core.get_order(&cmd.client_order_id)?;
        self.emitter.emit_order_modify_rejected(
            &order,
            order.venue_order_id(),
            "PredictFun has no atomic modify operation; cancel and submit a replacement",
            self.clock.get_time_ns(),
        );
        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let order = self.core.get_order(&cmd.client_order_id)?;
        let Some(venue_order_id) = order.venue_order_id().or(cmd.venue_order_id) else {
            log::warn!(
                "PredictFun cancel for {} is awaiting a venue order ID",
                cmd.client_order_id
            );
            return Ok(());
        };
        self.cancel_orders_command(vec![(order, venue_order_id)])?;
        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let cache = self.core.cache();
        let orders = cache.orders_open(
            Some(&PREDICTFUN_VENUE),
            Some(&cmd.instrument_id),
            Some(&cmd.strategy_id),
            Some(&self.core.account_id),
            Some(cmd.order_side),
        );
        let mut targets = Vec::new();
        for order in orders {
            if let Some(venue_order_id) = order.venue_order_id() {
                targets.push((order.cloned(), venue_order_id));
            } else {
                log::warn!(
                    "PredictFun cancel-all deferred for {} while awaiting a venue order ID",
                    order.client_order_id()
                );
            }
        }
        self.cancel_orders_command(targets)
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        let mut targets = Vec::new();
        for cancel in cmd.cancels {
            let order = self.core.get_order(&cancel.client_order_id)?;
            if let Some(venue_order_id) = order.venue_order_id().or(cancel.venue_order_id) {
                targets.push((order, venue_order_id));
            } else {
                log::warn!(
                    "PredictFun batch cancel for {} is awaiting a venue order ID",
                    cancel.client_order_id
                );
            }
        }
        self.cancel_orders_command(targets)
    }

    fn query_account(&self, cmd: QueryAccount) -> anyhow::Result<()> {
        log::debug!("Querying PredictFun account: {cmd:?}");
        let token = self.auth_token()?;
        let account = self
            .account_address
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun account lock poisoned"))?
            .ok_or_else(|| anyhow::anyhow!("PredictFun execution client has no account address"))?;
        let client = self.http_client.clone();
        let rpc_url = self.config.rpc_url.clone().expect("validated rpc_url");
        let environment = self.config.environment;
        let emitter = self.emitter.clone();
        get_runtime().spawn(async move {
            match load_account_balance(&client, &token, &rpc_url, environment, account).await {
                Ok(balance) => emitter.emit_account_state(
                    vec![balance],
                    vec![],
                    true,
                    get_atomic_clock_realtime().get_time_ns(),
                    None,
                ),
                Err(error) => log::error!("PredictFun account query failed: {error}"),
            }
        });
        Ok(())
    }

    fn on_instrument(&mut self, instrument: InstrumentAny) {
        let InstrumentAny::BinaryOption(binary) = instrument else {
            return;
        };
        let Some(info) = &binary.info else {
            log::warn!(
                "PredictFun instrument {} has no execution metadata",
                binary.id
            );
            return;
        };
        let Some(market_id) = info.get_u64("marketId") else {
            log::warn!("PredictFun instrument {} has no marketId", binary.id);
            return;
        };
        let fee_rate_bps = info
            .get_u64("feeRateBps")
            .and_then(|value| u32::try_from(value).ok());
        let Some(fee_rate_bps) = fee_rate_bps else {
            log::warn!("PredictFun instrument {} has invalid feeRateBps", binary.id);
            return;
        };
        let meta = ExecutionInstrumentMeta {
            market_id,
            token_id: binary.raw_symbol.to_string(),
            price_precision: binary.price_precision,
            is_yes: binary
                .outcome
                .is_some_and(|value| value.as_str().eq_ignore_ascii_case("yes")),
            is_neg_risk: info.get_bool("isNegRisk").unwrap_or(false),
            is_yield_bearing: info.get_bool("isYieldBearing").unwrap_or(false),
            fee_rate_bps,
        };
        if let Ok(mut instruments) = self.instruments.lock() {
            instruments.insert(binary.id, meta);
            self.core.set_instruments_initialized();
        }
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let token = self.auth_token()?;
        let mut params = HashMap::new();
        if let Some(instrument_id) = cmd.instrument_id {
            let meta = self.instrument_meta(instrument_id)?;
            params.insert("marketId".to_string(), meta.market_id.to_string());
        }
        let positions = self
            .http_client
            .get_positions(&token, Some(&params))
            .await?;
        let instruments = self
            .instruments
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun instrument lock poisoned"))?;
        let ts_init = self.clock.get_time_ns();
        positions
            .into_iter()
            .filter_map(|position| {
                let instrument = instruments.iter().find(|(_, meta)| {
                    meta.token_id == position.outcome.on_chain_id
                        && meta.market_id == position.market.id
                });
                let Some((instrument_id, _)) = instrument else {
                    log::warn!(
                        "Skipping PredictFun position {} for unknown token {}",
                        position.id,
                        position.outcome.on_chain_id
                    );
                    return None;
                };
                Some((position, *instrument_id))
            })
            .map(|(position, instrument_id)| {
                let amount = Decimal::from_str(&position.amount)?;
                if amount.is_sign_negative() {
                    anyhow::bail!("PredictFun position amount cannot be negative: {amount}");
                }
                let quantity = Quantity::from_decimal_dp(amount, 16)?;
                let side = if quantity.is_zero() {
                    PositionSideSpecified::Flat
                } else {
                    PositionSideSpecified::Long
                };
                let avg_px_open = Decimal::from_str(&position.average_buy_price_usd).ok();
                Ok(PositionStatusReport::new(
                    self.core.account_id,
                    instrument_id,
                    side,
                    quantity,
                    ts_init,
                    ts_init,
                    None,
                    Some(PositionId::new(position.id.as_str())),
                    avg_px_open,
                ))
            })
            .collect()
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        self.load_fill_reports(&cmd).await
    }

    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let ts_init = self.clock.get_time_ns();
        let lookback_start = lookback_mins.map(|minutes| {
            UnixNanos::from(
                ts_init
                    .as_u64()
                    .saturating_sub(minutes.saturating_mul(60_000_000_000)),
            )
        });
        let fill_cmd = GenerateFillReportsBuilder::default()
            .ts_init(ts_init)
            .start(lookback_start)
            .build()?;
        let position_cmd = GeneratePositionStatusReportsBuilder::default()
            .ts_init(ts_init)
            .start(lookback_start)
            .build()?;
        let order_reports = self.load_order_status_reports(None, false).await?;
        let fill_reports = self.load_fill_reports(&fill_cmd).await?;
        let position_reports = self.generate_position_status_reports(&position_cmd).await?;
        // Require the authenticated activity stream to be readable as part of a
        // complete private reconciliation snapshot.
        self.http_client
            .get_account_activity(&self.auth_token()?, None)
            .await?;
        let reported_orders = order_reports
            .iter()
            .map(|report| report.venue_order_id)
            .collect::<HashSet<_>>();
        let reports_complete = fill_reports
            .iter()
            .all(|report| reported_orders.contains(&report.venue_order_id));
        let mut status = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            *PREDICTFUN_VENUE,
            ts_init,
            None,
        );
        if lookback_start.is_some() {
            status.set_report_window(lookback_start, reports_complete);
        }
        status.add_order_reports(order_reports);
        status.add_fill_reports(fill_reports);
        status.add_position_reports(position_reports);
        Ok(Some(status))
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let reports = self
            .load_order_status_reports(cmd.instrument_id, false)
            .await?;
        Ok(reports.into_iter().find(|report| {
            cmd.venue_order_id
                .is_some_and(|value| value == report.venue_order_id)
                || cmd
                    .client_order_id
                    .is_some_and(|value| Some(value) == report.client_order_id)
                || (cmd.venue_order_id.is_none() && cmd.client_order_id.is_none())
        }))
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        self.load_order_status_reports(cmd.instrument_id, cmd.open_only)
            .await
    }
}

impl PredictFunExecutionClient {
    fn cancel_orders_command(&self, targets: Vec<(OrderAny, VenueOrderId)>) -> anyhow::Result<()> {
        if targets.is_empty() {
            return Ok(());
        }
        let token = self.auth_token()?;
        let account_address = self
            .account_address
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun account lock poisoned"))?
            .ok_or_else(|| anyhow::anyhow!("PredictFun execution client has no account address"))?;
        let rpc_url = self.config.rpc_url.clone().expect("validated rpc_url");
        let private_key = self
            .config
            .private_key
            .clone()
            .expect("validated private_key");
        let environment = self.config.environment;
        let account_type = self.config.account_type;
        let timeout_secs = self.config.request_timeout_secs;
        let client = self.http_client.clone();
        let instruments = Arc::clone(&self.instruments);
        let emitter = self.emitter.clone();
        get_runtime().spawn(async move {
            let records = match client.get_orders(&token, None).await {
                Ok(records) => records,
                Err(error) => {
                    log::warn!(
                        "PredictFun cancellation could not resolve orders; awaiting reconciliation: {error}"
                    );
                    return;
                }
            };
            let metadata = match instruments.lock() {
                Ok(metadata) => metadata.clone(),
                Err(_) => {
                    log::error!("PredictFun cancellation instrument lock poisoned");
                    return;
                }
            };
            let mut requests = Vec::new();
            let mut order_by_id = HashMap::new();
            for (order, venue_order_id) in targets {
                let id = venue_order_id.to_string();
                let Some(record) = records.iter().find(|record| record.id == id) else {
                    log::warn!(
                        "PredictFun cancellation could not find venue order {id}; awaiting reconciliation"
                    );
                    continue;
                };
                let Some(_meta) = metadata
                    .values()
                    .find(|meta| meta.token_id == record.order.token_id)
                else {
                    log::error!(
                        "PredictFun cancellation order {id} references unknown token {}",
                        record.order.token_id
                    );
                    continue;
                };
                requests.push(CancelRequest {
                    venue_order_id: id.clone(),
                    order: record.order.clone(),
                    is_neg_risk: record.is_neg_risk,
                    is_yield_bearing: record.is_yield_bearing,
                });
                let order_hash = record
                    .order
                    .hash
                    .clone()
                    .or_else(|| record.order_hash.clone());
                order_by_id.insert(id, (order, venue_order_id, order_hash));
            }
            if requests.is_empty() {
                return;
            }
            let remove_ids = requests
                .iter()
                .map(|request| request.venue_order_id.clone())
                .collect::<Vec<_>>();
            if let Err(error) = client.remove_orders(&token, remove_ids).await {
                // REST removal is advisory. Continue to authoritative invalidation so a
                // transport failure cannot leave a matchable order active on-chain.
                log::warn!(
                    "PredictFun off-chain order removal failed; continuing on-chain: {error}"
                );
            }
            let outcomes = cancel_groups(
                requests,
                &rpc_url,
                &private_key,
                environment,
                account_type,
                account_address,
                timeout_secs,
            )
            .await;
            for (id, outcome) in outcomes {
                let Some((order, venue_order_id, order_hash)) = order_by_id.remove(&id) else {
                    continue;
                };
                match outcome {
                    Ok(()) => {
                        let Some(order_hash) = order_hash else {
                            log::warn!(
                                "PredictFun cancellation for {id} confirmed on-chain but has no hash for REST reconciliation"
                            );
                            continue;
                        };
                        match client.get_order(&token, &order_hash).await {
                            Ok(record) if record.status.eq_ignore_ascii_case("CANCELLED") => {
                                emitter.emit_order_canceled(
                                    &order,
                                    Some(venue_order_id),
                                    get_atomic_clock_realtime().get_time_ns(),
                                );
                            }
                            Ok(record) if record.status.eq_ignore_ascii_case("FILLED") => log::info!(
                                "PredictFun cancel/fill race resolved as filled for {id}; awaiting fill reconciliation"
                            ),
                            Ok(record) => log::warn!(
                                "PredictFun cancellation for {id} is on-chain terminal but REST status is {}; awaiting reconciliation",
                                record.status
                            ),
                            Err(error) => log::warn!(
                                "PredictFun cancellation for {id} is on-chain terminal but REST reconciliation failed: {error}"
                            ),
                        }
                    }
                    Err(error) => log::warn!(
                        "PredictFun cancellation outcome is ambiguous for {id}; awaiting reconciliation: {error}"
                    ),
                }
            }
        });
        Ok(())
    }

    async fn load_fill_reports(
        &self,
        cmd: &GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let account_address = self
            .account_address
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun account lock poisoned"))?
            .ok_or_else(|| anyhow::anyhow!("PredictFun execution client has no account address"))?;
        let signer = format!("{account_address:#x}");
        let mut params = HashMap::from([("signerAddress".to_string(), signer.clone())]);
        if let Some(instrument_id) = cmd.instrument_id {
            params.insert(
                "marketId".to_string(),
                self.instrument_meta(instrument_id)?.market_id.to_string(),
            );
        }
        let matches = self.http_client.get_matches(Some(&params)).await?;
        let token = self.auth_token()?;
        let records = self.http_client.get_orders(&token, None).await?;
        let venue_ids = records
            .iter()
            .filter_map(|record| {
                record
                    .order
                    .hash
                    .as_ref()
                    .or(record.order_hash.as_ref())
                    .map(|hash| (hash.to_ascii_lowercase(), record.id.clone()))
            })
            .collect::<HashMap<_, _>>();
        let instruments = self
            .instruments
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun instrument lock poisoned"))?;
        let identities = self
            .order_ids
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun order identity lock poisoned"))?;
        let ts_init = self.clock.get_time_ns();
        let mut reports = Vec::new();
        for matched in matches {
            let Some(settlement_id) = matched.settlement_id.as_deref() else {
                // The public matches feed includes executions before their
                // on-chain settlement ID is assigned. Reconcile them once
                // settled so the TradeId remains stable and authoritative.
                continue;
            };
            let ts_event = parse_timestamp(Some(&matched.executed_at)).ok_or_else(|| {
                anyhow::anyhow!(
                    "PredictFun match {} has invalid executedAt {}",
                    settlement_id,
                    matched.executed_at
                )
            })?;
            if cmd.start.is_some_and(|start| ts_event < start)
                || cmd.end.is_some_and(|end| ts_event > end)
            {
                continue;
            }
            for (leg, liquidity) in std::iter::once((&matched.taker, LiquiditySide::Taker))
                .chain(
                    matched
                        .makers
                        .iter()
                        .map(|maker| (maker, LiquiditySide::Maker)),
                )
                .filter(|(leg, _)| leg.signer.eq_ignore_ascii_case(&signer))
            {
                let venue_order_id =
                    venue_ids
                        .get(&leg.hash.to_ascii_lowercase())
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "PredictFun match {} references unmapped order hash {}",
                                settlement_id,
                                leg.hash
                            )
                        })?;
                let venue_order_id = VenueOrderId::new(venue_order_id.as_str());
                if cmd
                    .venue_order_id
                    .is_some_and(|filter| filter != venue_order_id)
                {
                    continue;
                }
                let (instrument_id, meta) = instruments
                    .iter()
                    .find(|(_, meta)| meta.token_id == leg.outcome.on_chain_id)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "PredictFun match {} references unknown token {}",
                            settlement_id,
                            leg.outcome.on_chain_id
                        )
                    })?;
                if cmd
                    .instrument_id
                    .is_some_and(|filter| filter != *instrument_id)
                {
                    continue;
                }
                reports.push(match_fill_report(
                    self.core.account_id,
                    *instrument_id,
                    meta.price_precision,
                    settlement_id,
                    venue_order_id,
                    leg,
                    liquidity,
                    identities.get(&leg.hash),
                    ts_event,
                    ts_init,
                )?);
            }
        }
        Ok(reports)
    }

    async fn load_order_status_reports(
        &self,
        instrument_id: Option<nautilus_model::identifiers::InstrumentId>,
        open_only: bool,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let token = self.auth_token()?;
        let mut params = HashMap::new();
        if open_only {
            params.insert("status".to_string(), "OPEN".to_string());
        }
        let records = self.http_client.get_orders(&token, Some(&params)).await?;
        let instruments = self
            .instruments
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun instrument lock poisoned"))?;
        let identities = self
            .order_ids
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun order identity lock poisoned"))?;
        records
            .into_iter()
            .filter_map(|record| {
                let instrument = instruments
                    .iter()
                    .find(|(_, meta)| meta.token_id == record.order.token_id);
                let Some((record_instrument_id, meta)) = instrument else {
                    log::warn!(
                        "Skipping PredictFun order {} for unknown token {}",
                        record.id,
                        record.order.token_id
                    );
                    return None;
                };
                if instrument_id.is_some_and(|value| value != *record_instrument_id) {
                    return None;
                }
                Some((record, *record_instrument_id, meta.price_precision))
            })
            .map(|(record, record_instrument_id, price_precision)| {
                order_status_report(
                    self.core.account_id,
                    record_instrument_id,
                    price_precision,
                    &record,
                    &identities,
                    self.clock.get_time_ns(),
                )
            })
            .collect()
    }
}

#[expect(clippy::too_many_arguments)]
fn match_fill_report(
    account_id: AccountId,
    instrument_id: nautilus_model::identifiers::InstrumentId,
    price_precision: u8,
    settlement_id: &str,
    venue_order_id: VenueOrderId,
    leg: &PredictFunMatchOrder,
    liquidity: LiquiditySide,
    local_order: Option<&OrderAny>,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<FillReport> {
    let fee = leg
        .fee
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("PredictFun match fee is missing"))?;
    let price = Decimal::from_str(&leg.price)?;
    let commission = collateral_fee_value(Decimal::from_str(&fee.amount)?, fee.asset_type, price)?;
    let side = match leg.quote_type {
        crate::common::enums::PredictFunQuoteType::Bid => OrderSide::Buy,
        crate::common::enums::PredictFunQuoteType::Ask => OrderSide::Sell,
    };
    Ok(FillReport::new(
        account_id,
        instrument_id,
        venue_order_id,
        TradeId::new(fill_identity(settlement_id, &leg.hash).as_str()),
        side,
        Quantity::from_decimal_dp(Decimal::from_str(&leg.amount)?, 16)?,
        Price::from_decimal_dp(price, price_precision)?,
        Money::from_decimal(commission, usdt())?,
        liquidity,
        local_order.map(Order::client_order_id),
        None,
        ts_event,
        ts_init,
        None,
    ))
}

fn collateral_fee_value(
    amount: Decimal,
    asset_type: PredictFunFeeAsset,
    execution_price: Decimal,
) -> anyhow::Result<Decimal> {
    match asset_type {
        PredictFunFeeAsset::Collateral => Ok(amount),
        PredictFunFeeAsset::Shares => amount
            .checked_mul(execution_price)
            .ok_or_else(|| anyhow::anyhow!("PredictFun share fee conversion overflow")),
    }
}

fn order_status_report(
    account_id: AccountId,
    instrument_id: nautilus_model::identifiers::InstrumentId,
    price_precision: u8,
    record: &PredictFunOrderRecord,
    identities: &HashMap<String, OrderAny>,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderStatusReport> {
    let order_hash = record
        .order
        .hash
        .as_deref()
        .or(record.order_hash.as_deref());
    let local_order = identities
        .get(&record.id)
        .or_else(|| order_hash.and_then(|hash| identities.get(hash)));
    let order_side = match record.order.side {
        PredictFunSide::Buy => OrderSide::Buy,
        PredictFunSide::Sell => OrderSide::Sell,
    };
    let order_type = match record.strategy {
        PredictFunStrategy::Limit => OrderType::Limit,
        PredictFunStrategy::Market => OrderType::Market,
    };
    let time_in_force = local_order.map_or(
        match record.strategy {
            PredictFunStrategy::Limit => TimeInForce::Gtc,
            PredictFunStrategy::Market => TimeInForce::Fok,
        },
        Order::time_in_force,
    );
    let order_status = match record.status.to_ascii_uppercase().as_str() {
        "OPEN" => OrderStatus::Accepted,
        "FILLED" => OrderStatus::Filled,
        "EXPIRED" => OrderStatus::Expired,
        "CANCELLED" => OrderStatus::Canceled,
        "INVALIDATED" => OrderStatus::Rejected,
        other => anyhow::bail!("unknown PredictFun order status: {other}"),
    };
    let quantity = Quantity::from_decimal_dp(Decimal::from_str(&record.amount)?, 16)?;
    let filled_qty = Quantity::from_decimal_dp(Decimal::from_str(&record.amount_filled)?, 16)?;
    let maker = wei_to_decimal(U256::from_str(&record.order.maker_amount)?)?;
    let taker = wei_to_decimal(U256::from_str(&record.order.taker_amount)?)?;
    let price_decimal = match record.order.side {
        PredictFunSide::Buy => maker.checked_div(taker),
        PredictFunSide::Sell => taker.checked_div(maker),
    }
    .ok_or_else(|| anyhow::anyhow!("PredictFun order price division failed"))?;
    let price = Price::from_decimal_dp(price_decimal, price_precision)?;
    let mut report = OrderStatusReport::new(
        account_id,
        instrument_id,
        local_order.map(Order::client_order_id),
        VenueOrderId::new(record.id.as_str()),
        order_side,
        order_type,
        time_in_force,
        order_status,
        quantity,
        filled_qty,
        ts_init,
        ts_init,
        ts_init,
        None,
    );
    report.price = (order_type == OrderType::Limit).then_some(price);
    report.post_only = local_order.is_some_and(Order::is_post_only);
    report.expire_time = local_order.and_then(Order::expire_time);
    Ok(report)
}

async fn load_account_balance(
    client: &PredictFunHttpClient,
    token: &SecretString,
    rpc_url: &SecretString,
    environment: crate::common::enums::PredictFunEnvironment,
    account: Address,
) -> anyhow::Result<AccountBalance> {
    let total = wei_to_decimal(collateral_balance(rpc_url, environment, account).await?)?;
    let open_orders = client.get_orders(token, None).await?;
    let locked = locked_collateral(&open_orders)?;
    if locked > total {
        anyhow::bail!("PredictFun reserved collateral {locked} exceeds on-chain balance {total}");
    }
    Ok(AccountBalance::from_total_and_locked(
        total,
        locked,
        usdt(),
    )?)
}

fn locked_collateral(open_orders: &[PredictFunOrderRecord]) -> anyhow::Result<Decimal> {
    open_orders
        .iter()
        .filter(|record| {
            record.status.eq_ignore_ascii_case("OPEN") && record.order.side == PredictFunSide::Buy
        })
        .try_fold(Decimal::ZERO, |locked, record| {
            let quantity = Decimal::from_str(&record.amount)?;
            let filled = Decimal::from_str(&record.amount_filled)?;
            let maker = wei_to_decimal(U256::from_str(&record.order.maker_amount)?)?;
            let taker = wei_to_decimal(U256::from_str(&record.order.taker_amount)?)?;
            let price = maker
                .checked_div(taker)
                .ok_or_else(|| anyhow::anyhow!("PredictFun reserved balance division failed"))?;
            let remaining = quantity.checked_sub(filled).ok_or_else(|| {
                anyhow::anyhow!("PredictFun order filled amount exceeds quantity")
            })?;
            locked
                .checked_add(remaining.checked_mul(price).ok_or_else(|| {
                    anyhow::anyhow!("PredictFun reserved balance multiplication failed")
                })?)
                .ok_or_else(|| anyhow::anyhow!("PredictFun reserved balance overflow"))
        })
}

async fn reconcile_private_stream(
    client: &PredictFunHttpClient,
    token: &SecretString,
    rpc_url: &SecretString,
    environment: crate::common::enums::PredictFunEnvironment,
    account: Address,
    instruments: &Arc<
        Mutex<HashMap<nautilus_model::identifiers::InstrumentId, ExecutionInstrumentMeta>>,
    >,
) -> anyhow::Result<AccountBalance> {
    let records = client.get_orders(token, None).await?;
    // A successful positions request is part of the private snapshot gate, even when empty.
    client.get_positions(token, None).await?;
    client.get_account_activity(token, None).await?;
    let signer = format!("{account:#x}");
    let matches = client
        .get_matches(Some(&HashMap::from([(
            "signerAddress".to_string(),
            signer.clone(),
        )])))
        .await?;
    let mut order_hashes = records
        .iter()
        .filter_map(|record| {
            record
                .order
                .hash
                .as_ref()
                .or(record.order_hash.as_ref())
                .map(|hash| hash.to_ascii_lowercase())
        })
        .collect::<HashSet<_>>();
    let known_tokens = instruments
        .lock()
        .map_err(|_| anyhow::anyhow!("PredictFun instrument lock poisoned"))?
        .values()
        .map(|meta| meta.token_id.clone())
        .collect::<HashSet<_>>();
    for matched in matches {
        let Some(settlement_id) = matched.settlement_id.as_deref() else {
            continue;
        };
        for leg in std::iter::once(&matched.taker)
            .chain(matched.makers.iter())
            .filter(|leg| leg.signer.eq_ignore_ascii_case(&signer))
        {
            let normalized_hash = leg.hash.to_ascii_lowercase();
            if !order_hashes.contains(&normalized_hash) {
                let completed = client.get_order(token, &leg.hash).await?;
                if completed.order.token_id != leg.outcome.on_chain_id {
                    anyhow::bail!(
                        "PredictFun reconnect order {} token does not match settled outcome",
                        leg.hash
                    );
                }
                order_hashes.insert(normalized_hash);
            }
            if !known_tokens.contains(&leg.outcome.on_chain_id) {
                anyhow::bail!(
                    "PredictFun reconnect match {} references unknown token {}",
                    settlement_id,
                    leg.outcome.on_chain_id
                );
            }
            leg.fee.as_ref().ok_or_else(|| {
                anyhow::anyhow!("PredictFun reconnect match {settlement_id} has no fee")
            })?;
        }
    }
    load_account_balance(client, token, rpc_url, environment, account).await
}

async fn build_order_data(
    client: &PredictFunHttpClient,
    signer: &PredictFunOrderSigner,
    config: &PredictFunExecClientConfig,
    account: Address,
    order: &OrderAny,
    meta: &ExecutionInstrumentMeta,
) -> anyhow::Result<PredictFunCreateOrderData> {
    let side = match order.order_side() {
        OrderSide::Buy => PredictFunSide::Buy,
        OrderSide::Sell => PredictFunSide::Sell,
        OrderSide::NoOrderSide => anyhow::bail!("PredictFun order side is unspecified"),
    };
    let strategy = match order.order_type() {
        OrderType::Limit => PredictFunStrategy::Limit,
        OrderType::Market => PredictFunStrategy::Market,
        other => anyhow::bail!("PredictFun does not support {other:?} orders"),
    };
    let quantity = order.quantity().as_decimal();
    let amounts = match strategy {
        PredictFunStrategy::Limit => limit_order_amounts(
            side,
            order
                .price()
                .ok_or_else(|| anyhow::anyhow!("PredictFun LIMIT order requires price"))?
                .as_decimal(),
            quantity,
        )?,
        PredictFunStrategy::Market => {
            let book = client.get_orderbook(meta.market_id).await?;
            let (bids, asks) =
                outcome_book_levels(&book, meta.market_id, meta.price_precision, meta.is_yes)?;
            market_order_amounts_by_quantity(
                side,
                quantity,
                &bids,
                &asks,
                config.market_slippage_bps,
                false,
            )?
        }
    };
    let expiration = match strategy {
        PredictFunStrategy::Market => get_atomic_clock_realtime()
            .get_time_ns()
            .as_seconds()
            .saturating_add(MARKET_EXPIRATION_SECS),
        PredictFunStrategy::Limit => order
            .expire_time()
            .map_or(LIMIT_EXPIRATION_FALLBACK_SECS, |value| value.as_seconds()),
    };
    let local_address = signer.address();
    let maker = match config.account_type {
        PredictFunAccountType::Eoa => local_address,
        PredictFunAccountType::PredictAccount => account,
    };
    let mut contract = PredictFunContractOrder {
        salt: rand::rng().random_range(0..=MAX_SALT).to_string(),
        maker: format!("{maker:#x}"),
        signer: format!("{maker:#x}"),
        taker: format!("{:#x}", Address::ZERO),
        token_id: meta.token_id.clone(),
        maker_amount: amounts.maker_amount.to_string(),
        taker_amount: amounts.taker_amount.to_string(),
        expiration: expiration.to_string(),
        nonce: "0".to_string(),
        fee_rate_bps: meta.fee_rate_bps.to_string(),
        side,
        signature_type: PredictFunSignatureType::Eoa,
        signature: None,
        hash: None,
    };
    let hash = order_hash(
        &contract,
        config.environment,
        meta.is_neg_risk,
        meta.is_yield_bearing,
    )?;
    let signature = match config.account_type {
        PredictFunAccountType::Eoa => signer.sign_order(
            &contract,
            config.environment,
            meta.is_neg_risk,
            meta.is_yield_bearing,
        )?,
        PredictFunAccountType::PredictAccount => signer.sign_order_for_predict_account(
            &contract,
            account,
            config.environment,
            meta.is_neg_risk,
            meta.is_yield_bearing,
        )?,
    };
    contract.hash = Some(format!("{hash:#x}"));
    contract.signature = Some(signature);
    Ok(PredictFunCreateOrderData {
        price_per_share: amounts.price_per_share.to_string(),
        strategy,
        order: contract,
        slippage_bps: (strategy == PredictFunStrategy::Market)
            .then(|| config.market_slippage_bps.to_string()),
        is_fill_or_kill: (strategy == PredictFunStrategy::Market).then_some(true),
        is_post_only: (strategy == PredictFunStrategy::Limit).then_some(order.is_post_only()),
        self_trade_prevention: None,
        is_min_amount_out: (strategy == PredictFunStrategy::Market)
            .then_some(amounts.is_min_amount_out),
    })
}

fn handle_wallet_event(
    event: &PredictFunWalletEvent,
    emitter: &ExecutionEventEmitter,
    order_ids: &Arc<Mutex<HashMap<String, OrderAny>>>,
    accepted_order_ids: &Arc<Mutex<HashSet<String>>>,
    fill_ids: &Arc<Mutex<HashSet<String>>>,
    instruments: &Arc<
        Mutex<HashMap<nautilus_model::identifiers::InstrumentId, ExecutionInstrumentMeta>>,
    >,
    expected_account: Address,
) -> anyhow::Result<()> {
    ensure_wallet_event_account(&event.wallet_address, expected_account)?;
    let order = {
        let identities = order_ids
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun order identity lock poisoned"))?;
        identities
            .get(&event.order_id)
            .or_else(|| identities.get(&event.order_hash))
            .cloned()
    };
    let Some(order) = order else {
        log::debug!(
            "Ignoring PredictFun wallet event for external order {}",
            event.order_id
        );
        return Ok(());
    };
    let venue_order_id = VenueOrderId::new(event.order_id.as_str());
    let ts_event = UnixNanos::from_millis(event.timestamp);
    match event.event_type.as_str() {
        "orderAccepted" => {
            let should_emit = accepted_order_ids
                .lock()
                .map_err(|_| anyhow::anyhow!("PredictFun accepted-order lock poisoned"))?
                .insert(event.order_id.clone());
            if should_emit {
                emitter.emit_order_accepted(&order, venue_order_id, ts_event);
            }
        }
        "orderNotAccepted" => emitter.emit_order_rejected(
            &order,
            event
                .details
                .reason
                .as_deref()
                .unwrap_or("PredictFun rejected order"),
            ts_event,
            event.details.reason.as_deref() == Some("rejectedPostOnly"),
        ),
        "orderExpired" => emitter.emit_order_expired(&order, Some(venue_order_id), ts_event),
        "orderCancelled" => emitter.emit_order_canceled(&order, Some(venue_order_id), ts_event),
        "orderTransactionSuccess" => {
            let settlement_id = event
                .details
                .settlement_id
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("successful settlement missing settlementId"))?;
            let fill_id = fill_identity(settlement_id, &event.order_hash);
            let mut seen = fill_ids
                .lock()
                .map_err(|_| anyhow::anyhow!("PredictFun fill identity lock poisoned"))?;
            if seen.contains(&fill_id) {
                return Ok(());
            }
            let fill = event
                .details
                .fill
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("successful settlement missing fill"))?;
            let qty = wei_to_decimal(U256::from_str(&fill.executed_size_wei)?)?;
            let price = wei_to_decimal(U256::from_str(&fill.executed_price_wei)?)?;
            let instrument = instruments
                .lock()
                .map_err(|_| anyhow::anyhow!("PredictFun instrument lock poisoned"))?
                .get(&order.instrument_id())
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("PredictFun fill instrument metadata missing"))?;
            let last_qty = nautilus_model::types::Quantity::from_decimal_dp(qty, 16)?;
            let last_px =
                nautilus_model::types::Price::from_decimal_dp(price, instrument.price_precision)?;
            let commission = fill
                .fee
                .as_ref()
                .map(|fee| {
                    let amount = wei_to_decimal(U256::from_str(&fee.amount_wei)?)?;
                    let value = collateral_fee_value(amount, fee.asset_type, price)?;
                    Ok::<Money, anyhow::Error>(Money::from_decimal(value, usdt())?)
                })
                .transpose()?;
            let trade_id = TradeId::new(fill_id.as_str());
            let liquidity = match event.details.is_maker {
                Some(true) => LiquiditySide::Maker,
                Some(false) => LiquiditySide::Taker,
                None => LiquiditySide::NoLiquiditySide,
            };
            emitter.emit_order_filled(
                &order,
                venue_order_id,
                None,
                trade_id,
                last_qty,
                last_px,
                usdt(),
                commission,
                liquidity,
                ts_event,
            );
            seen.insert(fill_id);
        }
        "orderTransactionFailed" => log::warn!(
            "PredictFun on-chain settlement failed for order {}: {:?}",
            event.order_id,
            event.details.reason
        ),
        _ => {}
    }
    Ok(())
}

fn fill_identity(settlement_id: &str, order_hash: &str) -> String {
    format!("{settlement_id}:{}", order_hash.to_ascii_lowercase())
}

fn ensure_wallet_event_account(
    wallet_address: &str,
    expected_account: Address,
) -> anyhow::Result<()> {
    let actual = Address::from_str(wallet_address)
        .map_err(|error| anyhow::anyhow!("invalid PredictFun wallet event address: {error}"))?;
    if actual != expected_account {
        anyhow::bail!(
            "PredictFun wallet event account {actual:#x} does not match expected {expected_account:#x}"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn submit_readiness_gate_rejects_unavailable_private_stream() {
        let private_ready = AtomicBool::new(false);

        let error = ensure_private_execution_ready(&private_ready).unwrap_err();

        assert!(error.to_string().contains("not ready"));
    }

    #[rstest]
    fn submit_readiness_gate_accepts_reconciled_private_stream() {
        let private_ready = AtomicBool::new(true);

        assert!(ensure_private_execution_ready(&private_ready).is_ok());
    }

    fn match_leg(asset_type: PredictFunFeeAsset) -> PredictFunMatchOrder {
        PredictFunMatchOrder {
            quote_type: crate::common::enums::PredictFunQuoteType::Bid,
            amount: "2.5000".to_string(),
            price: "0.42".to_string(),
            outcome: crate::http::models::PredictFunOutcome {
                name: "Yes".to_string(),
                index_set: 1,
                on_chain_id: "123".to_string(),
                status: None,
            },
            signer: format!("{:#x}", Address::ZERO),
            hash: "0xabc".to_string(),
            fee: Some(crate::http::models::PredictFunMatchFee {
                amount: "0.01".to_string(),
                asset_type,
            }),
        }
    }

    #[test]
    fn converts_documented_open_order_to_nautilus_report() {
        let record = PredictFunOrderRecord {
            id: "venue-1".to_string(),
            order_hash: None,
            market_id: 42,
            status: "OPEN".to_string(),
            amount: "10.000".to_string(),
            amount_filled: "2.000".to_string(),
            is_neg_risk: false,
            is_yield_bearing: false,
            strategy: PredictFunStrategy::Limit,
            order: PredictFunContractOrder {
                salt: "1".to_string(),
                maker: format!("{:#x}", Address::ZERO),
                signer: format!("{:#x}", Address::ZERO),
                taker: format!("{:#x}", Address::ZERO),
                token_id: "123".to_string(),
                maker_amount: "4000000000000000000".to_string(),
                taker_amount: "10000000000000000000".to_string(),
                expiration: LIMIT_EXPIRATION_FALLBACK_SECS.to_string(),
                nonce: "0".to_string(),
                fee_rate_bps: "0".to_string(),
                side: PredictFunSide::Buy,
                signature_type: PredictFunSignatureType::Eoa,
                signature: Some("0x00".to_string()),
                hash: Some("0xhash".to_string()),
            },
        };
        let report = order_status_report(
            AccountId::from("PREDICTFUN-001"),
            nautilus_model::identifiers::InstrumentId::from("123.PREDICTFUN"),
            2,
            &record,
            &HashMap::new(),
            UnixNanos::from(1),
        )
        .unwrap();
        assert_eq!(report.order_status, OrderStatus::Accepted);
        assert_eq!(report.price, Some(Price::from("0.40")));
        assert_eq!(report.quantity, Quantity::from("10.0000000000000000"));
        assert_eq!(report.filled_qty, Quantity::from("2.0000000000000000"));
        assert_eq!(locked_collateral(&[record]).unwrap(), Decimal::new(32, 1));
    }

    #[test]
    fn converts_collateral_match_fee_exactly() {
        let report = match_fill_report(
            AccountId::from("PREDICTFUN-001"),
            nautilus_model::identifiers::InstrumentId::from("123.PREDICTFUN"),
            2,
            "settlement-1",
            VenueOrderId::new("venue-1"),
            &match_leg(PredictFunFeeAsset::Collateral),
            LiquiditySide::Taker,
            None,
            UnixNanos::from(1),
            UnixNanos::from(2),
        )
        .unwrap();
        assert_eq!(report.order_side, OrderSide::Buy);
        assert_eq!(report.last_qty, Quantity::from("2.5000000000000000"));
        assert_eq!(report.last_px, Price::from("0.42"));
        assert_eq!(report.commission, Money::from("0.01 USDT"));
        assert_eq!(report.liquidity_side, LiquiditySide::Taker);
        assert_eq!(report.trade_id, TradeId::new("settlement-1:0xabc"));
    }

    #[test]
    fn converts_share_denominated_match_fee_at_execution_price() {
        let report = match_fill_report(
            AccountId::from("PREDICTFUN-001"),
            nautilus_model::identifiers::InstrumentId::from("123.PREDICTFUN"),
            2,
            "settlement-1",
            VenueOrderId::new("venue-1"),
            &match_leg(PredictFunFeeAsset::Shares),
            LiquiditySide::Maker,
            None,
            UnixNanos::from(1),
            UnixNanos::from(2),
        )
        .unwrap();
        assert_eq!(report.commission, Money::from("0.0042 USDT"));
    }

    #[rstest]
    fn fill_identity_is_composite_and_normalizes_order_hash_case() {
        assert_eq!(fill_identity("settlement-1", "0xAbC"), "settlement-1:0xabc");
        assert_ne!(
            fill_identity("settlement-1", "0xabc"),
            fill_identity("settlement-1", "0xdef")
        );
    }

    #[rstest]
    fn wallet_event_account_must_match_authenticated_account() {
        let expected = Address::from([1_u8; 20]);
        assert!(ensure_wallet_event_account(&format!("{expected:#x}"), expected).is_ok());

        let error =
            ensure_wallet_event_account(&format!("{:#x}", Address::from([2_u8; 20])), expected)
                .unwrap_err();
        assert!(error.to_string().contains("does not match expected"));
        assert!(ensure_wallet_event_account("not-an-address", expected).is_err());
    }
}
