use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ahash::AHashMap;
use nautilus_common::{
    clients::DataClient,
    live::{get_runtime, runner::get_data_event_sender},
    messages::{
        DataEvent,
        data::{
            DataResponse, InstrumentResponse, InstrumentsResponse, RequestBookSnapshot,
            RequestInstrument, RequestInstruments, SubscribeBars, SubscribeBookDeltas,
            SubscribeInstrument, SubscribeInstrumentStatus, SubscribeQuotes, SubscribeTrades,
            UnsubscribeBookDeltas, UnsubscribeInstrumentStatus, UnsubscribeQuotes,
        },
    },
    providers::InstrumentProvider,
};
use nautilus_core::{
    AtomicMap, UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_model::{
    data::{Data, InstrumentStatus},
    enums::{BookType, MarketStatusAction},
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    common::{consts::PREDICTFUN_VENUE, precision::NAUTILUS_QUANTITY_PRECISION},
    config::PredictFunDataClientConfig,
    http::PredictFunHttpClient,
    provider::{PredictFunInstrumentMeta, PredictFunInstrumentProvider, fetch_instruments},
    websocket::{
        PredictFunWebSocketClient, PredictFunWebSocketSubscriptionHandle, PredictFunWsEvent,
        book::BookVersionState,
        messages::PredictFunWsMessage,
        parse::{parse_book_snapshots, quote_pair_from_snapshot},
    },
};

#[derive(Debug, Clone, Copy)]
struct MarketPair {
    yes: InstrumentId,
    no: InstrumentId,
    price_precision: u8,
}

#[derive(Debug)]
pub struct PredictFunDataClient {
    client_id: ClientId,
    config: PredictFunDataClientConfig,
    http_client: PredictFunHttpClient,
    ws_client: PredictFunWebSocketClient,
    provider: PredictFunInstrumentProvider,
    ws_handle: Option<PredictFunWebSocketSubscriptionHandle>,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    metadata: Arc<AtomicMap<InstrumentId, PredictFunInstrumentMeta>>,
    markets: Arc<AtomicMap<u64, MarketPair>>,
    book_subscriptions: Arc<Mutex<HashSet<InstrumentId>>>,
    quote_subscriptions: Arc<Mutex<HashSet<InstrumentId>>>,
    status_subscriptions: Arc<Mutex<HashSet<InstrumentId>>>,
    connected: AtomicBool,
    cancellation: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    data_sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    clock: &'static AtomicTime,
}

impl PredictFunDataClient {
    pub fn new(client_id: ClientId, config: PredictFunDataClientConfig) -> anyhow::Result<Self> {
        config.validate()?;
        let http_client = PredictFunHttpClient::new(
            config.api_url(),
            config.api_key.as_ref(),
            config.request_timeout_secs,
        )?;
        let ws_client = PredictFunWebSocketClient::new(
            config.websocket_url()?,
            config.api_key.clone(),
            config.transport_backend,
        );
        let provider = PredictFunInstrumentProvider::new(http_client.clone());
        Ok(Self {
            client_id,
            config,
            http_client,
            ws_client,
            provider,
            ws_handle: None,
            instruments: Arc::new(AtomicMap::new()),
            metadata: Arc::new(AtomicMap::new()),
            markets: Arc::new(AtomicMap::new()),
            book_subscriptions: Arc::new(Mutex::new(HashSet::new())),
            quote_subscriptions: Arc::new(Mutex::new(HashSet::new())),
            status_subscriptions: Arc::new(Mutex::new(HashSet::new())),
            connected: AtomicBool::new(false),
            cancellation: CancellationToken::new(),
            tasks: Vec::new(),
            data_sender: get_data_event_sender(),
            clock: get_atomic_clock_realtime(),
        })
    }

    fn meta(&self, instrument_id: InstrumentId) -> anyhow::Result<PredictFunInstrumentMeta> {
        self.metadata
            .get_cloned(&instrument_id)
            .ok_or_else(|| anyhow::anyhow!("PredictFun instrument not found: {instrument_id}"))
    }

    fn subscribe_market_topic(&self, market_id: u64) -> anyhow::Result<()> {
        self.ws_handle
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PredictFun WebSocket is not connected"))?
            .subscribe(format!("predictOrderbook/{market_id}"))
    }

    fn maybe_unsubscribe_market(&self, market_id: u64) -> anyhow::Result<()> {
        let metadata = self.metadata.load();
        let still_needed = metadata
            .iter()
            .filter(|(_, meta)| meta.market_id == market_id)
            .any(|(id, _)| {
                contains(&self.book_subscriptions, id) || contains(&self.quote_subscriptions, id)
            });
        if !still_needed {
            self.ws_handle
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("PredictFun WebSocket is not connected"))?
                .unsubscribe(format!("predictOrderbook/{market_id}"))?;
        }
        Ok(())
    }

    fn maybe_unsubscribe_status(&self, market_id: u64) -> anyhow::Result<()> {
        let metadata = self.metadata.load();
        let still_needed = metadata
            .iter()
            .filter(|(_, meta)| meta.market_id == market_id)
            .any(|(id, _)| contains(&self.status_subscriptions, id));
        if !still_needed {
            let handle = self
                .ws_handle
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("PredictFun WebSocket is not connected"))?;
            handle.unsubscribe(format!("predictTradingStatus/{market_id}"))?;
            handle.unsubscribe(format!("predictMarketStatus/{market_id}"))?;
        }
        Ok(())
    }

    fn build_market_pairs(
        metadata: &AHashMap<InstrumentId, PredictFunInstrumentMeta>,
    ) -> anyhow::Result<AHashMap<u64, MarketPair>> {
        let mut grouped: HashMap<u64, Vec<(InstrumentId, PredictFunInstrumentMeta)>> =
            HashMap::new();
        for (id, meta) in metadata {
            grouped
                .entry(meta.market_id)
                .or_default()
                .push((*id, *meta));
        }
        let mut pairs = AHashMap::new();
        for (market_id, instruments) in grouped {
            let yes = instruments.iter().find(|(_, meta)| meta.is_yes);
            let no = instruments.iter().find(|(_, meta)| !meta.is_yes);
            let (Some((yes_id, yes_meta)), Some((no_id, no_meta))) = (yes, no) else {
                anyhow::bail!("PredictFun market {market_id} does not contain YES and NO outcomes");
            };
            if yes_meta.price_precision != no_meta.price_precision {
                anyhow::bail!("PredictFun market {market_id} outcomes disagree on precision");
            }
            pairs.insert(
                market_id,
                MarketPair {
                    yes: *yes_id,
                    no: *no_id,
                    price_precision: yes_meta.price_precision,
                },
            );
        }
        Ok(pairs)
    }

    fn apply_discovery(
        instruments_cache: &AtomicMap<InstrumentId, InstrumentAny>,
        metadata_cache: &AtomicMap<InstrumentId, PredictFunInstrumentMeta>,
        markets_cache: &AtomicMap<u64, MarketPair>,
        instruments: AHashMap<InstrumentId, InstrumentAny>,
        metadata: AHashMap<InstrumentId, PredictFunInstrumentMeta>,
    ) -> anyhow::Result<Vec<InstrumentAny>> {
        let markets = Self::build_market_pairs(&metadata)?;
        let new_instruments = instruments
            .iter()
            .filter(|(id, _)| !instruments_cache.contains_key(id))
            .map(|(_, instrument)| instrument.clone())
            .collect::<Vec<_>>();

        // Preserve old definitions because existing subscriptions can still deliver their
        // terminal status while a new discovery generation becomes active.
        metadata_cache.rcu(|cached| cached.extend(metadata.clone()));
        markets_cache.rcu(|cached| cached.extend(markets.clone()));
        instruments_cache.rcu(|cached| cached.extend(instruments.clone()));
        Ok(new_instruments)
    }

    fn spawn_instrument_refresh_task(&mut self) {
        let Some(interval_mins) = self.config.update_instruments_interval_mins else {
            return;
        };
        if interval_mins == 0 {
            return;
        }

        let interval = Duration::from_secs(interval_mins.saturating_mul(60));
        let cancellation = self.cancellation.clone();
        let http_client = self.http_client.clone();
        let filters = self.config.market_filters.clone();
        let instruments_cache = Arc::clone(&self.instruments);
        let metadata_cache = Arc::clone(&self.metadata);
        let markets_cache = Arc::clone(&self.markets);
        let data_sender = self.data_sender.clone();

        self.tasks.push(get_runtime().spawn(async move {
            loop {
                tokio::select! {
                    () = tokio::time::sleep(interval) => {}
                    () = cancellation.cancelled() => break,
                }

                let fetched = fetch_instruments(&http_client, filters.as_ref()).await;
                if cancellation.is_cancelled() {
                    break;
                }
                let refreshed = fetched.and_then(|(instruments, metadata)| {
                    let instruments = instruments
                        .into_iter()
                        .map(|instrument| (instrument.id(), instrument))
                        .collect::<AHashMap<_, _>>();
                    let metadata = metadata.into_iter().collect::<AHashMap<_, _>>();
                    Self::apply_discovery(
                        &instruments_cache,
                        &metadata_cache,
                        &markets_cache,
                        instruments,
                        metadata,
                    )
                });
                match refreshed {
                    Ok(instruments) => {
                        for instrument in instruments {
                            if let Err(error) = data_sender.send(DataEvent::Instrument(instrument))
                            {
                                log::warn!(
                                    "Failed to publish refreshed PredictFun instrument: {error}"
                                );
                                break;
                            }
                        }
                    }
                    Err(error) => {
                        // A refresh is additive. Failure leaves the last complete generation and
                        // all active WebSocket subscriptions untouched.
                        log::warn!("Failed to refresh PredictFun instruments: {error}");
                    }
                }
            }
        }));
    }
}

fn contains(set: &Arc<Mutex<HashSet<InstrumentId>>>, id: &InstrumentId) -> bool {
    set.lock().is_ok_and(|guard| guard.contains(id))
}

#[async_trait::async_trait(?Send)]
impl DataClient for PredictFunDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*PREDICTFUN_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!(
            "Starting PredictFun data client: client_id={}, environment={:?}",
            self.client_id,
            self.config.environment
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.cancellation.cancel();
        self.connected.store(false, Ordering::Release);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.cancellation = CancellationToken::new();
        self.instruments.store(AHashMap::new());
        self.metadata.store(AHashMap::new());
        self.markets.store(AHashMap::new());
        self.book_subscriptions
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun book subscription lock poisoned"))?
            .clear();
        self.quote_subscriptions
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun quote subscription lock poisoned"))?
            .clear();
        self.status_subscriptions
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun status subscription lock poisoned"))?
            .clear();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        self.cancellation = CancellationToken::new();
        self.provider
            .load_all(self.config.market_filters.as_ref())
            .await?;
        let instruments = self
            .provider
            .store()
            .get_all()
            .iter()
            .map(|(id, instrument)| (*id, instrument.clone()))
            .collect::<AHashMap<_, _>>();
        let metadata = self
            .provider
            .metadata()
            .iter()
            .map(|(id, meta)| (*id, *meta))
            .collect::<AHashMap<_, _>>();
        let discovered = Self::apply_discovery(
            &self.instruments,
            &self.metadata,
            &self.markets,
            instruments,
            metadata,
        )?;
        for instrument in discovered {
            self.data_sender
                .send(DataEvent::Instrument(instrument))
                .map_err(|error| anyhow::anyhow!("data engine stopped: {error}"))?;
        }
        self.ws_client.connect().await?;
        self.ws_handle = Some(self.ws_client.subscription_handle());
        let mut out_rx = self
            .ws_client
            .take_out_rx()
            .ok_or_else(|| anyhow::anyhow!("PredictFun WebSocket receiver unavailable"))?;
        let sender = self.data_sender.clone();
        let cancellation = self.cancellation.clone();
        let markets = Arc::clone(&self.markets);
        let book_subscriptions = Arc::clone(&self.book_subscriptions);
        let quote_subscriptions = Arc::clone(&self.quote_subscriptions);
        let status_subscriptions = Arc::clone(&self.status_subscriptions);
        let clock = self.clock;
        self.tasks.push(get_runtime().spawn(async move {
            let mut versions = HashMap::<u64, BookVersionState>::new();
            loop {
                tokio::select! {
                    () = cancellation.cancelled() => break,
                    event = out_rx.recv() => {
                        let Some(event) = event else { break };
                        dispatch_ws_event(
                            event,
                            &sender,
                            &markets,
                            &book_subscriptions,
                            &quote_subscriptions,
                            &status_subscriptions,
                            &mut versions,
                            clock,
                        );
                    }
                }
            }
        }));
        self.spawn_instrument_refresh_task();
        self.connected.store(true, Ordering::Release);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.cancellation.cancel();
        self.ws_client.disconnect().await;
        for task in self.tasks.drain(..) {
            if let Err(error) = task.await {
                log::warn!("PredictFun data task stopped with error: {error}");
            }
        }
        self.ws_handle = None;
        self.connected.store(false, Ordering::Release);
        Ok(())
    }

    fn subscribe_instrument(&mut self, cmd: SubscribeInstrument) -> anyhow::Result<()> {
        let instrument = self.meta(cmd.instrument_id)?;
        let _ = instrument;
        if let Some(value) = self.instruments.get_cloned(&cmd.instrument_id) {
            self.data_sender.send(DataEvent::Instrument(value))?;
        }
        Ok(())
    }

    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        if cmd.book_type != BookType::L2_MBP {
            anyhow::bail!("PredictFun supports L2_MBP full-snapshot books only");
        }
        let meta = self.meta(cmd.instrument_id)?;
        self.book_subscriptions
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun book subscription lock poisoned"))?
            .insert(cmd.instrument_id);
        self.subscribe_market_topic(meta.market_id)
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        let meta = self.meta(cmd.instrument_id)?;
        self.quote_subscriptions
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun quote subscription lock poisoned"))?
            .insert(cmd.instrument_id);
        self.subscribe_market_topic(meta.market_id)
    }

    fn subscribe_trades(&mut self, _cmd: SubscribeTrades) -> anyhow::Result<()> {
        anyhow::bail!("PredictFun does not document an authoritative public trade stream")
    }

    fn subscribe_bars(&mut self, _cmd: SubscribeBars) -> anyhow::Result<()> {
        anyhow::bail!("PredictFun does not publish native bar data")
    }

    fn subscribe_instrument_status(
        &mut self,
        cmd: SubscribeInstrumentStatus,
    ) -> anyhow::Result<()> {
        let meta = self.meta(cmd.instrument_id)?;
        let inserted = self
            .status_subscriptions
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun status subscription lock poisoned"))?
            .insert(cmd.instrument_id);
        if inserted {
            let handle = self
                .ws_handle
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("PredictFun WebSocket is not connected"))?;
            handle.subscribe(format!("predictTradingStatus/{}", meta.market_id))?;
            handle.subscribe(format!("predictMarketStatus/{}", meta.market_id))?;
        }
        Ok(())
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        let meta = self.meta(cmd.instrument_id)?;
        self.book_subscriptions
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun book subscription lock poisoned"))?
            .remove(&cmd.instrument_id);
        self.maybe_unsubscribe_market(meta.market_id)
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        let meta = self.meta(cmd.instrument_id)?;
        self.quote_subscriptions
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun quote subscription lock poisoned"))?
            .remove(&cmd.instrument_id);
        self.maybe_unsubscribe_market(meta.market_id)
    }

    fn unsubscribe_instrument_status(
        &mut self,
        cmd: &UnsubscribeInstrumentStatus,
    ) -> anyhow::Result<()> {
        let meta = self.meta(cmd.instrument_id)?;
        self.status_subscriptions
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun status subscription lock poisoned"))?
            .remove(&cmd.instrument_id);
        self.maybe_unsubscribe_status(meta.market_id)
    }

    fn request_instruments(&self, request: RequestInstruments) -> anyhow::Result<()> {
        let response = InstrumentsResponse::new(
            request.request_id,
            request.client_id.unwrap_or(self.client_id),
            *PREDICTFUN_VENUE,
            self.instruments.load().values().cloned().collect(),
            None,
            None,
            self.clock.get_time_ns(),
            request.params,
        );
        self.data_sender
            .send(DataEvent::Response(DataResponse::Instruments(response)))?;
        Ok(())
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        let instrument = self
            .instruments
            .get_cloned(&request.instrument_id)
            .ok_or_else(|| anyhow::anyhow!("PredictFun instrument not found"))?;
        let response = InstrumentResponse::new(
            request.request_id,
            request.client_id.unwrap_or(self.client_id),
            request.instrument_id,
            instrument,
            None,
            None,
            self.clock.get_time_ns(),
            request.params,
        );
        self.data_sender
            .send(DataEvent::Response(DataResponse::Instrument(Box::new(
                response,
            ))))?;
        Ok(())
    }

    fn request_book_snapshot(&self, request: RequestBookSnapshot) -> anyhow::Result<()> {
        let meta = self.meta(request.instrument_id)?;
        let pair = self
            .markets
            .get_cloned(&meta.market_id)
            .ok_or_else(|| anyhow::anyhow!("PredictFun market pair not found"))?;
        let client = self.http_client.clone();
        let sender = self.data_sender.clone();
        let client_id = request.client_id.unwrap_or(self.client_id);
        let clock = self.clock;
        get_runtime().spawn(async move {
            let result = async {
                let snapshot = client.get_orderbook(meta.market_id).await?;
                let (yes, no) = parse_book_snapshots(
                    &snapshot,
                    meta.market_id,
                    pair.yes,
                    pair.no,
                    pair.price_precision,
                    NAUTILUS_QUANTITY_PRECISION,
                    clock.get_time_ns(),
                )?;
                let deltas = if meta.is_yes { yes } else { no };
                let mut book = nautilus_model::orderbook::OrderBook::new(
                    request.instrument_id,
                    BookType::L2_MBP,
                );
                book.apply_deltas(&deltas)?;
                let response = nautilus_common::messages::data::BookResponse::new(
                    request.request_id,
                    client_id,
                    request.instrument_id,
                    book,
                    None,
                    None,
                    clock.get_time_ns(),
                    request.params,
                );
                sender.send(DataEvent::Response(DataResponse::Book(response)))?;
                anyhow::Ok(())
            }
            .await;
            if let Err(error) = result {
                log::error!("PredictFun book snapshot request failed: {error}");
            }
        });
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn dispatch_ws_event(
    event: PredictFunWsEvent,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    markets: &AtomicMap<u64, MarketPair>,
    book_subscriptions: &Arc<Mutex<HashSet<InstrumentId>>>,
    quote_subscriptions: &Arc<Mutex<HashSet<InstrumentId>>>,
    status_subscriptions: &Arc<Mutex<HashSet<InstrumentId>>>,
    versions: &mut HashMap<u64, BookVersionState>,
    clock: &'static AtomicTime,
) {
    match event {
        PredictFunWsEvent::Reconnected => versions.clear(),
        PredictFunWsEvent::Error(error) => log::warn!("PredictFun WebSocket error: {error}"),
        PredictFunWsEvent::Message(PredictFunWsMessage::OrderBook { book }) => {
            let Some(pair) = markets.get_cloned(&book.market_id) else {
                log::warn!(
                    "PredictFun book arrived for unknown market {}",
                    book.market_id
                );
                return;
            };
            let state = versions.entry(book.market_id).or_default();
            if let Err(error) = state.accept(book.version) {
                log::warn!(
                    "PredictFun book version discontinuity; accepting full resnapshot: {error}"
                );
                state.reset();
                if let Err(error) = state.accept(book.version) {
                    log::error!("PredictFun book resnapshot version rejected: {error}");
                    return;
                }
            }
            let ts_init = clock.get_time_ns();
            match parse_book_snapshots(
                &book,
                book.market_id,
                pair.yes,
                pair.no,
                pair.price_precision,
                NAUTILUS_QUANTITY_PRECISION,
                ts_init,
            ) {
                Ok((yes, no)) => {
                    if contains(book_subscriptions, &pair.yes) {
                        let _ = sender.send(DataEvent::Data(Data::Deltas(Box::new(yes))));
                    }
                    if contains(book_subscriptions, &pair.no) {
                        let _ = sender.send(DataEvent::Data(Data::Deltas(Box::new(no))));
                    }
                }
                Err(error) => log::warn!("PredictFun book rejected: {error}"),
            }
            match quote_pair_from_snapshot(
                &book,
                book.market_id,
                pair.yes,
                pair.no,
                pair.price_precision,
                NAUTILUS_QUANTITY_PRECISION,
                ts_init,
            ) {
                Ok((Some(yes), Some(no))) => {
                    if contains(quote_subscriptions, &pair.yes) {
                        let _ = sender.send(DataEvent::Data(Data::Quote(yes)));
                    }
                    if contains(quote_subscriptions, &pair.no) {
                        let _ = sender.send(DataEvent::Data(Data::Quote(no)));
                    }
                }
                Ok(_) => {}
                Err(error) => log::warn!("PredictFun quote rejected: {error}"),
            }
        }
        PredictFunWsEvent::Message(PredictFunWsMessage::Response(response)) => {
            if response.success == Some(false) {
                log::warn!(
                    "PredictFun WebSocket request rejected: {:?}",
                    response.error
                );
            }
        }
        PredictFunWsEvent::Message(PredictFunWsMessage::TradingStatus(event)) => {
            emit_market_status(
                sender,
                markets,
                status_subscriptions,
                event.market_id,
                &event.trading_status,
                event.ts_ms,
                false,
                clock,
            );
        }
        PredictFunWsEvent::Message(PredictFunWsMessage::MarketStatus(event)) => {
            emit_market_status(
                sender,
                markets,
                status_subscriptions,
                event.market_id,
                &event.status,
                event.ts_ms,
                true,
                clock,
            );
        }
        PredictFunWsEvent::Message(_) => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_market_status(
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    markets: &AtomicMap<u64, MarketPair>,
    subscriptions: &Arc<Mutex<HashSet<InstrumentId>>>,
    market_id: u64,
    raw_status: &str,
    ts_ms: u64,
    terminal_status: bool,
    clock: &'static AtomicTime,
) {
    let Some(pair) = markets.get_cloned(&market_id) else {
        log::warn!("PredictFun status arrived for unknown market {market_id}");
        return;
    };
    let normalized = raw_status.to_ascii_uppercase();
    let action = match normalized.as_str() {
        "ACTIVE" | "OPEN" | "TRADING" => MarketStatusAction::Trading,
        "PRE_OPEN" | "PENDING" | "UPCOMING" => MarketStatusAction::PreOpen,
        "PAUSED" => MarketStatusAction::Pause,
        "HALTED" => MarketStatusAction::Halt,
        "SUSPENDED" => MarketStatusAction::Suspend,
        "CLOSED" | "RESOLVED" | "SETTLED" | "CANCELLED" if terminal_status => {
            MarketStatusAction::Close
        }
        _ => MarketStatusAction::NotAvailableForTrading,
    };
    let is_trading = matches!(action, MarketStatusAction::Trading);
    let ts_event = UnixNanos::from_millis(ts_ms);
    let ts_init = clock.get_time_ns();
    for instrument_id in [pair.yes, pair.no] {
        if !contains(subscriptions, &instrument_id) {
            continue;
        }
        let status = InstrumentStatus::new(
            instrument_id,
            action,
            ts_event,
            ts_init,
            None,
            Some(raw_status.into()),
            Some(is_trading),
            None,
            None,
        );
        if let Err(error) = sender.send(DataEvent::InstrumentStatus(status)) {
            log::error!("Failed to emit PredictFun status for {instrument_id}: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::UnixNanos;

    use super::*;
    use crate::{
        http::{
            models::{PredictFunMarket, PredictFunOutcome},
            parse::create_instrument,
        },
        provider::PredictFunInstrumentMeta,
    };

    fn generation(
        market_id: u64,
    ) -> (
        AHashMap<InstrumentId, InstrumentAny>,
        AHashMap<InstrumentId, PredictFunInstrumentMeta>,
    ) {
        let outcomes = [
            PredictFunOutcome {
                name: "YES".to_string(),
                index_set: 1,
                on_chain_id: format!("{market_id}1"),
                status: Some("OPEN".to_string()),
            },
            PredictFunOutcome {
                name: "NO".to_string(),
                index_set: 2,
                on_chain_id: format!("{market_id}2"),
                status: Some("OPEN".to_string()),
            },
        ];
        let market = PredictFunMarket {
            id: market_id,
            title: Some(format!("market {market_id}")),
            question: None,
            condition_id: format!("condition-{market_id}"),
            decimal_precision: 2,
            fee_rate_bps: 200,
            is_neg_risk: false,
            is_yield_bearing: false,
            trading_status: "OPEN".to_string(),
            status: "REGISTERED".to_string(),
            outcomes: outcomes.to_vec(),
            starts_at: Some("2026-09-02T17:00:00Z".to_string()),
            ends_at: Some("2026-09-02T17:05:00Z".to_string()),
            category_slug: Some(format!("btc-updown-5m-{market_id}")),
            market_variant: Some("CRYPTO_UP_DOWN".to_string()),
            variant_data: None,
        };
        let mut instruments = AHashMap::new();
        let mut metadata = AHashMap::new();
        for outcome in &outcomes {
            let instrument = create_instrument(&market, outcome, UnixNanos::default())
                .expect("valid test instrument");
            let instrument_id = instrument.id();
            instruments.insert(instrument_id, instrument);
            metadata.insert(
                instrument_id,
                PredictFunInstrumentMeta {
                    market_id,
                    price_precision: 2,
                    is_yes: outcome.index_set == 1,
                },
            );
        }
        (instruments, metadata)
    }

    #[test]
    fn discovery_generations_are_additive_and_emit_each_instrument_once() {
        let instruments = AtomicMap::new();
        let metadata = AtomicMap::new();
        let markets = AtomicMap::new();
        let (first_instruments, first_metadata) = generation(100);

        let first = PredictFunDataClient::apply_discovery(
            &instruments,
            &metadata,
            &markets,
            first_instruments.clone(),
            first_metadata.clone(),
        )
        .expect("first discovery");
        let duplicate = PredictFunDataClient::apply_discovery(
            &instruments,
            &metadata,
            &markets,
            first_instruments,
            first_metadata,
        )
        .expect("duplicate discovery");
        let (second_instruments, second_metadata) = generation(101);
        let second = PredictFunDataClient::apply_discovery(
            &instruments,
            &metadata,
            &markets,
            second_instruments,
            second_metadata,
        )
        .expect("second discovery");

        assert_eq!(first.len(), 2);
        assert!(duplicate.is_empty());
        assert_eq!(second.len(), 2);
        assert_eq!(instruments.len(), 4);
        assert_eq!(metadata.len(), 4);
        assert_eq!(markets.len(), 2);
        assert!(markets.contains_key(&100));
        assert!(markets.contains_key(&101));
    }

    #[test]
    fn invalid_refresh_keeps_last_complete_discovery_generation() {
        let instruments = AtomicMap::new();
        let metadata = AtomicMap::new();
        let markets = AtomicMap::new();
        let (first_instruments, first_metadata) = generation(100);
        PredictFunDataClient::apply_discovery(
            &instruments,
            &metadata,
            &markets,
            first_instruments,
            first_metadata,
        )
        .expect("first discovery");
        let (second_instruments, mut second_metadata) = generation(101);
        let removed = *second_metadata.keys().next().expect("metadata entry");
        second_metadata.remove(&removed);

        let result = PredictFunDataClient::apply_discovery(
            &instruments,
            &metadata,
            &markets,
            second_instruments,
            second_metadata,
        );

        assert!(result.is_err());
        assert_eq!(instruments.len(), 2);
        assert_eq!(metadata.len(), 2);
        assert_eq!(markets.len(), 1);
        assert!(markets.contains_key(&100));
        assert!(!markets.contains_key(&101));
    }
}
