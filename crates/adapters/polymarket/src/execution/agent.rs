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

//! Provides a narrow native execution-agent facade for Polymarket.

use std::{collections::HashSet, fmt, future::Future, sync::Arc};

use nautilus_model::{enums::LiquiditySide, identifiers::VenueOrderId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{
    agent_lifecycle::{
        PolymarketLifecycleBackend, PolymarketLifecycleError, PolymarketLifecycleOperation,
        PolymarketLifecycleReadiness, PolymarketLifecycleReconciliation,
        PolymarketLifecycleSubmission, PreparedPolymarketLifecycle, into_backend_prepared,
        prepared_lifecycle, validate_lifecycle_readiness,
    },
    order_builder::PolymarketOrderBuilder,
    parse::compute_commission,
};
use crate::{
    common::consts::LOT_SIZE_SCALE,
    common::enums::{
        PolymarketEventType, PolymarketLiquiditySide, PolymarketOrderSide, PolymarketOrderStatus,
        PolymarketOrderType, PolymarketTradeStatus, SignatureType,
    },
    http::{
        clob::{PolymarketClobHttpClient, PolymarketClobPublicClient},
        error::Error as HttpError,
        models::{
            ClobBookResponse, ClobMarketResponse, PolymarketOpenOrder, PolymarketOrder,
            PolymarketTradeReport,
        },
        query::{
            AssetType, BalanceAllowance, CancelResponse, GetBalanceAllowanceParams,
            GetOrdersParams, GetTradesParams, OrderResponse,
        },
    },
    signing::eip712::{PolymarketApproval, approval_plan},
    websocket::{
        client::PolymarketWebSocketClient,
        messages::{PolymarketWsMessage, UserWsMessage},
    },
};

/// Native CLOB floor for the quote-denominated amount of an immediate BUY
pub const MARKETABLE_BUY_MINIMUM_NOTIONAL: Decimal = Decimal::ONE;

/// Provider-neutral private evidence emitted by the native Polymarket agent.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PolymarketNormalizedPrivateEvent {
    Order {
        venue_order_id: String,
        market_id: String,
        instrument_id: String,
        side: PolymarketOrderSide,
        price: Decimal,
        original_shares: Decimal,
        matched_shares: Option<Decimal>,
        status: Option<PolymarketOrderStatus>,
        status_reason: Option<String>,
        event_type: PolymarketEventType,
        associated_fill_ids: Vec<String>,
        holding_wallet: String,
        venue_timestamp: String,
    },
    Fill {
        fill_id: String,
        market_id: String,
        instrument_id: String,
        side: PolymarketOrderSide,
        liquidity_side: PolymarketLiquiditySide,
        price: Decimal,
        gross_shares: Decimal,
        fee_rate_bps: Decimal,
        status: PolymarketTradeStatus,
        related_venue_order_ids: Vec<String>,
        transaction_hash: Option<String>,
        holding_wallet: String,
        venue_timestamp: String,
    },
    Reconnected,
}

/// A malformed private event is rejected rather than converted with lossy defaults.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum NormalizePrivateEventError {
    #[error("private event field {field} is not an exact decimal: {value}")]
    InvalidDecimal { field: &'static str, value: String },
    #[error("private fill fee evidence is invalid: {0}")]
    InvalidFeeEvidence(String),
}

/// Computes the exact collateral-denominated fee represented by an authenticated private fill.
/// Callers must separately verify that the event rate matches current canonical market metadata.
pub fn exact_private_fill_fee(
    fee_rate_bps: Decimal,
    fee_exponent: u8,
    shares: Decimal,
    price: Decimal,
    liquidity_side: PolymarketLiquiditySide,
) -> Result<Decimal, NormalizePrivateEventError> {
    if !(Decimal::ZERO..=Decimal::from(10_000u32)).contains(&fee_rate_bps)
        || fee_exponent > 8
        || shares <= Decimal::ZERO
        || price <= Decimal::ZERO
        || price >= Decimal::ONE
    {
        return Err(NormalizePrivateEventError::InvalidFeeEvidence(
            "rate, exponent, shares, or price is outside venue bounds".to_string(),
        ));
    }
    Ok(compute_commission(
        fee_rate_bps / Decimal::from(10_000u32),
        f64::from(fee_exponent),
        shares,
        price,
        match liquidity_side {
            PolymarketLiquiditySide::Maker => LiquiditySide::Maker,
            PolymarketLiquiditySide::Taker => LiquiditySide::Taker,
        },
    ))
}

/// Stateful exact-duplicate filter. Reordered status transitions remain observable.
#[derive(Debug, Default)]
pub struct PolymarketPrivateEventNormalizer {
    seen_revisions: HashSet<String>,
}

impl PolymarketPrivateEventNormalizer {
    /// Normalizes one user event and suppresses only byte-equivalent logical revisions.
    pub fn normalize(
        &mut self,
        message: PolymarketWsMessage,
        holding_wallet: &str,
    ) -> Result<Option<PolymarketNormalizedPrivateEvent>, NormalizePrivateEventError> {
        if matches!(message, PolymarketWsMessage::Reconnected) {
            return Ok(Some(PolymarketNormalizedPrivateEvent::Reconnected));
        }
        let Some((revision, event)) = normalize_private_event(message, holding_wallet)? else {
            return Ok(None);
        };
        Ok(self.seen_revisions.insert(revision).then_some(event))
    }
}

/// Request for a price-protected immediate order denominated in shares.
#[derive(Clone, Debug)]
pub struct MarketableSharesOrderRequest {
    pub condition_id: String,
    pub token_id: String,
    pub side: PolymarketOrderSide,
    pub protected_price: Decimal,
    pub shares: Decimal,
    pub time_in_force: PolymarketOrderType,
    pub neg_risk: bool,
    pub tick_decimals: u32,
}

/// Current venue evidence captured immediately before order signing.
#[derive(Clone, Debug)]
pub struct MarketableOrderPreflight {
    pub best_price: Decimal,
    pub executable_shares: Decimal,
    /// Quote collateral required to consume exactly the requested BUY shares from this book.
    pub buy_quote_amount: Option<Decimal>,
    /// Marginal ask used by the venue's V2 market-BUY amount encoding.
    pub buy_crossing_price: Option<Decimal>,
    pub minimum_shares: Decimal,
    pub tick_size: Decimal,
    pub catalog_tick_size: Option<Decimal>,
    pub catalog_tick_mismatch: bool,
    pub fee_rate_bps: Decimal,
    pub available_balance: Decimal,
}

/// External startup evidence which cannot be derived from the CLOB order endpoints.
#[derive(Clone, Debug)]
pub struct PolymarketStartupEvidence {
    pub chain_id: u64,
    pub geoblocked: bool,
    pub private_stream_authenticated: bool,
    pub signer_address: String,
    pub funder_address: String,
}

/// Native capabilities currently backed by this adapter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PolymarketAgentCapabilities {
    pub prepare_submit_cancel: bool,
    pub private_events: bool,
    pub reconciliation: bool,
    pub redeem: bool,
    pub merge: bool,
    pub split: bool,
}

impl Default for PolymarketAgentCapabilities {
    fn default() -> Self {
        Self {
            prepare_submit_cancel: true,
            private_events: true,
            reconciliation: true,
            redeem: false,
            merge: false,
            split: false,
        }
    }
}

/// A signed order which has not been sent to Polymarket.
///
/// The signed payload is deliberately private and this type is not cloneable or serializable.
pub struct PreparedMarketableOrder {
    order: PolymarketOrder,
    time_in_force: PolymarketOrderType,
    native_order_hash: VenueOrderId,
    shares: Decimal,
    preflight: MarketableOrderPreflight,
}

impl PreparedMarketableOrder {
    /// Returns the deterministic native order hash without exposing the signed payload.
    #[must_use]
    pub const fn native_order_hash(&self) -> VenueOrderId {
        self.native_order_hash
    }

    /// Returns the requested number of shares.
    #[must_use]
    pub const fn shares(&self) -> Decimal {
        self.shares
    }

    /// Returns the signature type carried by the prepared order.
    #[must_use]
    pub const fn signature_type(&self) -> SignatureType {
        self.order.signature_type
    }

    /// Returns the venue evidence used before signing.
    #[must_use]
    pub const fn preflight(&self) -> &MarketableOrderPreflight {
        &self.preflight
    }
}

impl fmt::Debug for PreparedMarketableOrder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedMarketableOrder")
            .field("native_order_hash", &self.native_order_hash)
            .field("shares", &self.shares)
            .field("time_in_force", &self.time_in_force)
            .field("preflight", &self.preflight)
            .field("signed_payload", &"<redacted>")
            .finish()
    }
}

/// Result of one successful prepared-order submission.
#[derive(Clone, Debug)]
pub struct SubmittedMarketableOrder {
    pub native_order_hash: VenueOrderId,
    pub venue_order_id: VenueOrderId,
    pub shares: Decimal,
    pub response: OrderResponse,
}

/// Error returned while preparing a marketable shares order.
#[derive(Debug, Error)]
pub enum PrepareMarketableOrderError {
    #[error("marketable orders require FAK or FOK time in force")]
    UnsupportedTimeInForce,
    #[error("share quantity must be positive")]
    InvalidShares,
    #[error(
        "STALE_BOOK: current best price {best_price} moved beyond protected price {protected_price}"
    )]
    PriceMoved {
        best_price: Decimal,
        protected_price: Decimal,
    },
    #[error("VENUE_MINIMUM: requested {requested} shares, current minimum is {minimum}")]
    BelowMinimum {
        requested: Decimal,
        minimum: Decimal,
    },
    #[error(
        "VENUE_MINIMUM: marketable BUY notional {requested} pUSD is below native minimum {minimum} pUSD"
    )]
    BelowMinimumNotional {
        requested: Decimal,
        minimum: Decimal,
    },
    #[error("INSUFFICIENT_DEPTH: requested {requested} shares, executable depth is {available}")]
    InsufficientDepth {
        requested: Decimal,
        available: Decimal,
    },
    #[error("BALANCE_INSUFFICIENT: required {required}, available {available}")]
    InsufficientBalance {
        required: Decimal,
        available: Decimal,
    },
    #[error("ALLOWANCE_NOT_READY: one or more required scoped allowances are insufficient")]
    AllowanceNotReady,
    #[error("PRICE_TICK_INVALID: protected price {price} does not conform to tick {tick_size}")]
    InvalidTick { price: Decimal, tick_size: Decimal },
    #[error("MARKET_NOT_TRADABLE: {0}")]
    MarketNotTradable(String),
    #[error("Polymarket native execution requires Deposit Wallet signature_type=3")]
    SignatureTypeNotDepositWallet,
    #[error("failed to build signed order: {0}")]
    Build(String),
    #[error("failed to derive native order hash: {0}")]
    Hash(String),
    #[error("preflight request failed: {0}")]
    Preflight(String),
}

/// Startup gate failure for a Deposit Wallet execution agent.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PolymarketStartupError {
    #[error("Polygon chain ID mismatch: expected 137, received {0}")]
    WrongChain(u64),
    #[error("Polymarket reports this host as geoblocked")]
    Geoblocked,
    #[error("authenticated private user stream is not ready")]
    PrivateStreamNotReady,
    #[error("Deposit Wallet signature_type=3 is required")]
    WrongSignatureType,
    #[error("Deposit Wallet funder must be distinct from signer")]
    SignerFunderNotDistinct,
    #[error("startup signer identity does not match the configured signer")]
    SignerMismatch,
    #[error("startup funder identity does not match the configured Deposit Wallet")]
    FunderMismatch,
}

/// Definitive or ambiguous outcome from the single submit attempt.
#[derive(Debug, Error)]
pub enum SubmitPreparedOrderError {
    #[error("SUBMIT_UNKNOWN for {native_order_hash}: {reason}")]
    Unknown {
        native_order_hash: VenueOrderId,
        reason: String,
    },
    #[error("order {native_order_hash} was rejected: {reason}")]
    Rejected {
        native_order_hash: VenueOrderId,
        reason: String,
    },
}

/// Account evidence returned by one reconciliation snapshot.
#[derive(Debug)]
pub struct PolymarketAgentReconciliation {
    pub orders: Vec<PolymarketOpenOrder>,
    pub fills: Vec<PolymarketTradeReport>,
    pub balances_and_allowances: Vec<BalanceAllowance>,
}

/// Public facade for a native Polymarket execution-agent process.
pub struct PolymarketAgentFacade {
    http: PolymarketClobHttpClient,
    public_http: PolymarketClobPublicClient,
    order_builder: Arc<PolymarketOrderBuilder>,
    private_events: Option<PolymarketWebSocketClient>,
    lifecycle: Option<Arc<dyn PolymarketLifecycleBackend>>,
}

impl fmt::Debug for PolymarketAgentFacade {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PolymarketAgentFacade")
            .field("http", &"<authenticated>")
            .field("public_http", &"<public>")
            .field("order_builder", &"<signer redacted>")
            .field("private_events_configured", &self.private_events.is_some())
            .field("lifecycle_configured", &self.lifecycle.is_some())
            .finish()
    }
}

impl PolymarketAgentFacade {
    /// Creates a facade from already authenticated native clients.
    ///
    /// # Errors
    ///
    /// Returns an error unless the builder is configured for a distinct type-3 Deposit Wallet.
    #[must_use]
    pub fn new(
        http: PolymarketClobHttpClient,
        public_http: PolymarketClobPublicClient,
        order_builder: Arc<PolymarketOrderBuilder>,
        private_events: Option<PolymarketWebSocketClient>,
    ) -> Result<Self, PolymarketStartupError> {
        if order_builder.signature_type() != SignatureType::Poly1271 {
            return Err(PolymarketStartupError::WrongSignatureType);
        }
        if addresses_equal(
            order_builder.signer_address(),
            order_builder.maker_address(),
        ) {
            return Err(PolymarketStartupError::SignerFunderNotDistinct);
        }
        Ok(Self {
            http,
            public_http,
            order_builder,
            private_events,
            lifecycle: None,
        })
    }

    /// Configures a concrete native Deposit Wallet lifecycle backend.
    #[must_use]
    pub fn with_lifecycle_backend(
        mut self,
        lifecycle: Arc<dyn PolymarketLifecycleBackend>,
    ) -> Self {
        self.lifecycle = Some(lifecycle);
        self
    }

    /// Returns the capabilities backed by concrete native clients.
    #[must_use]
    pub fn capabilities(&self) -> PolymarketAgentCapabilities {
        PolymarketAgentCapabilities {
            private_events: self.private_events.is_some(),
            redeem: self.lifecycle.is_some(),
            merge: self.lifecycle.is_some(),
            split: self.lifecycle.is_some(),
            ..PolymarketAgentCapabilities::default()
        }
    }

    /// Returns and validates current Deposit Wallet lifecycle readiness.
    ///
    /// # Errors
    ///
    /// Returns an error when no backend is configured or any readiness gate fails closed.
    pub async fn lifecycle_readiness(
        &self,
    ) -> Result<PolymarketLifecycleReadiness, PolymarketLifecycleError> {
        let backend = self
            .lifecycle
            .as_ref()
            .ok_or(PolymarketLifecycleError::Unsupported)?;
        let readiness = backend.readiness().await?;
        validate_lifecycle_readiness(
            &readiness,
            self.order_builder.signer_address(),
            self.order_builder.maker_address(),
        )?;
        Ok(readiness)
    }

    /// Performs lifecycle readiness/preflight and signs without submitting to the Relayer.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend is absent, not ready, or rejects preparation.
    pub async fn prepare_lifecycle(
        &self,
        operation: PolymarketLifecycleOperation,
    ) -> Result<PreparedPolymarketLifecycle, PolymarketLifecycleError> {
        self.lifecycle_readiness().await?;
        let backend = self
            .lifecycle
            .as_ref()
            .ok_or(PolymarketLifecycleError::Unsupported)?;
        let prepared = backend.prepare(&operation).await?;
        Ok(prepared_lifecycle(operation, prepared))
    }

    /// Consumes a prepared lifecycle request and invokes the backend submit boundary once.
    ///
    /// # Errors
    ///
    /// Returns `Unknown` whenever the Relayer POST outcome cannot be proven.
    pub async fn submit_prepared_lifecycle(
        &self,
        prepared: PreparedPolymarketLifecycle,
    ) -> Result<PolymarketLifecycleSubmission, PolymarketLifecycleError> {
        let backend = self
            .lifecycle
            .as_ref()
            .ok_or(PolymarketLifecycleError::Unsupported)?;
        backend
            .submit_prepared(into_backend_prepared(prepared))
            .await
            .map_err(Into::into)
    }

    /// Reconciles a durable Relayer transaction ID without resubmitting it.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend is absent or the read-only poll fails.
    pub async fn reconcile_lifecycle(
        &self,
        submission: &PolymarketLifecycleSubmission,
    ) -> Result<PolymarketLifecycleReconciliation, PolymarketLifecycleError> {
        let backend = self
            .lifecycle
            .as_ref()
            .ok_or(PolymarketLifecycleError::Unsupported)?;
        backend.reconcile(submission).await.map_err(Into::into)
    }

    /// Validates chain, geography, account identity, and private stream readiness.
    ///
    /// # Errors
    ///
    /// Returns an error when any required startup gate is not ready.
    pub fn validate_startup(
        &self,
        evidence: &PolymarketStartupEvidence,
    ) -> Result<(), PolymarketStartupError> {
        if evidence.chain_id != 137 {
            return Err(PolymarketStartupError::WrongChain(evidence.chain_id));
        }
        if evidence.geoblocked {
            return Err(PolymarketStartupError::Geoblocked);
        }
        if !addresses_equal(
            &evidence.signer_address,
            self.order_builder.signer_address(),
        ) {
            return Err(PolymarketStartupError::SignerMismatch);
        }
        if !addresses_equal(&evidence.funder_address, self.order_builder.maker_address()) {
            return Err(PolymarketStartupError::FunderMismatch);
        }
        if !evidence.private_stream_authenticated || !self.private_events_authenticated() {
            return Err(PolymarketStartupError::PrivateStreamNotReady);
        }
        Ok(())
    }

    /// Rechecks native venue state and signs a protected FAK/FOK order without submitting it.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid request, a non-Deposit Wallet signer, or signing failure.
    pub async fn prepare_order(
        &self,
        request: MarketableSharesOrderRequest,
    ) -> Result<PreparedMarketableOrder, PrepareMarketableOrderError> {
        if !matches!(
            request.time_in_force,
            PolymarketOrderType::FAK | PolymarketOrderType::FOK
        ) {
            return Err(PrepareMarketableOrderError::UnsupportedTimeInForce);
        }
        if request.shares <= Decimal::ZERO {
            return Err(PrepareMarketableOrderError::InvalidShares);
        }

        let balance_params = GetBalanceAllowanceParams {
            asset_type: Some(match request.side {
                PolymarketOrderSide::Buy => AssetType::Collateral,
                PolymarketOrderSide::Sell => AssetType::Conditional,
            }),
            token_id: (request.side == PolymarketOrderSide::Sell).then(|| request.token_id.clone()),
            signature_type: Some(SignatureType::Poly1271),
        };
        let (market, book, tick, fee, balance_allowance) = tokio::try_join!(
            self.public_http.get_market(&request.condition_id),
            self.public_http.get_book(&request.token_id),
            self.http.get_tick_size(&request.token_id),
            self.http.get_fee_rate(&request.token_id),
            self.http.get_balance_allowance(balance_params),
        )
        .map_err(|error| PrepareMarketableOrderError::Preflight(error.strategy_reason()))?;
        let preflight = validate_preflight(
            &request,
            &market,
            &book,
            tick.minimum_tick_size,
            fee.base_fee,
            &balance_allowance,
        )?;

        self.sign_preflighted_order(request, preflight)
    }

    fn sign_preflighted_order(
        &self,
        request: MarketableSharesOrderRequest,
        preflight: MarketableOrderPreflight,
    ) -> Result<PreparedMarketableOrder, PrepareMarketableOrderError> {
        // Both sides are share-denominated. For BUY, maker collateral is the protected ceiling;
        // price improvement returns unused collateral instead of increasing the requested shares.
        let order = self
            .order_builder
            .build_limit_order(
                &request.token_id,
                request.side,
                request.protected_price,
                request.shares,
                request.time_in_force,
                "0",
                request.neg_risk,
                preflight.tick_size.scale(),
            )
            .map_err(|error| PrepareMarketableOrderError::Build(error.to_string()))?;
        if order.signature_type != SignatureType::Poly1271 {
            return Err(PrepareMarketableOrderError::SignatureTypeNotDepositWallet);
        }
        let native_order_hash = self
            .order_builder
            .expected_order_id(&order, request.neg_risk)
            .map_err(|error| PrepareMarketableOrderError::Hash(error.to_string()))?;

        Ok(PreparedMarketableOrder {
            order,
            time_in_force: request.time_in_force,
            native_order_hash,
            shares: request.shares,
            preflight,
        })
    }

    /// Consumes and submits a prepared order with exactly one HTTP write attempt.
    ///
    /// # Errors
    ///
    /// Returns [`SubmitPreparedOrderError::Unknown`] whenever the write may have reached the venue.
    pub async fn submit_prepared(
        &self,
        prepared: PreparedMarketableOrder,
    ) -> Result<SubmittedMarketableOrder, SubmitPreparedOrderError> {
        let http = &self.http;
        submit_once(prepared, |order, time_in_force| async move {
            http.post_order(&order, time_in_force, false).await
        })
        .await
    }

    /// Cancels an order by native venue order ID.
    pub async fn cancel_order(&self, order_id: &str) -> crate::http::error::Result<CancelResponse> {
        self.http.cancel_order(order_id).await
    }

    /// Lists open orders using the native authenticated endpoint.
    pub async fn list_orders(
        &self,
        params: GetOrdersParams,
    ) -> crate::http::error::Result<Vec<PolymarketOpenOrder>> {
        self.http.get_orders(params).await
    }

    /// Lists fills using the native authenticated endpoint.
    pub async fn list_fills(
        &self,
        params: GetTradesParams,
    ) -> crate::http::error::Result<Vec<PolymarketTradeReport>> {
        self.http.get_trades(params).await
    }

    /// Fetches strict balance and scoped allowance evidence.
    pub async fn balance_allowance(
        &self,
        params: GetBalanceAllowanceParams,
    ) -> crate::http::error::Result<BalanceAllowance> {
        self.http.get_balance_allowance(params).await
    }

    /// Fetches orders, fills, and requested account evidence for restart reconciliation.
    pub async fn reconcile(
        &self,
        order_params: GetOrdersParams,
        fill_params: GetTradesParams,
        balance_params: Vec<GetBalanceAllowanceParams>,
    ) -> crate::http::error::Result<PolymarketAgentReconciliation> {
        let (orders, fills) = tokio::try_join!(
            self.http.get_orders(order_params),
            self.http.get_trades(fill_params),
        )?;
        let mut balances_and_allowances = Vec::with_capacity(balance_params.len());
        for params in balance_params {
            balances_and_allowances.push(self.http.get_balance_allowance(params).await?);
        }
        Ok(PolymarketAgentReconciliation {
            orders,
            fills,
            balances_and_allowances,
        })
    }

    /// Connects and subscribes the configured authenticated private event stream.
    ///
    /// # Errors
    ///
    /// Returns an error when no private client is configured or connection fails.
    pub async fn connect_private_events(&mut self) -> anyhow::Result<()> {
        let client = self
            .private_events
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Polymarket private event client is not configured"))?;
        client.connect().await?;
        client.subscribe_user().await?;
        if !client
            .wait_for_authenticated(std::time::Duration::from_secs(15))
            .await
        {
            anyhow::bail!("Polymarket private user stream authentication timed out or failed");
        }
        Ok(())
    }

    /// Returns whether the private event stream is authenticated.
    #[must_use]
    pub fn private_events_authenticated(&self) -> bool {
        self.private_events
            .as_ref()
            .is_some_and(PolymarketWebSocketClient::is_authenticated)
    }

    /// Takes the private event receiver for an external event loop.
    pub fn take_private_event_receiver(
        &mut self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<PolymarketWsMessage>> {
        self.private_events
            .as_mut()
            .and_then(PolymarketWebSocketClient::take_message_receiver)
    }

    /// Takes a provider-neutral, duplicate-filtered private event receiver.
    ///
    /// Exact duplicate revisions are suppressed while fill-before-order and reordered status
    /// transitions remain observable. Decimal parse failures are delivered to the receiver.
    pub fn take_normalized_private_event_receiver(
        &mut self,
    ) -> Option<
        tokio::sync::mpsc::UnboundedReceiver<
            Result<PolymarketNormalizedPrivateEvent, NormalizePrivateEventError>,
        >,
    > {
        let mut source = self.take_private_event_receiver()?;
        let holding_wallet = self.order_builder.maker_address().to_string();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut normalizer = PolymarketPrivateEventNormalizer::default();
            while let Some(message) = source.recv().await {
                match normalizer.normalize(message, &holding_wallet) {
                    Ok(Some(event)) => {
                        if sender.send(Ok(event)).is_err() {
                            break;
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        if sender.send(Err(error)).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        Some(receiver)
    }

    /// Disconnects the configured private event stream.
    ///
    /// # Errors
    ///
    /// Returns an error when disconnection fails.
    pub async fn disconnect_private_events(&mut self) -> anyhow::Result<()> {
        if let Some(client) = self.private_events.as_mut() {
            client.disconnect().await?;
        }
        Ok(())
    }
}

fn exact_decimal(field: &'static str, value: &str) -> Result<Decimal, NormalizePrivateEventError> {
    value
        .parse::<Decimal>()
        .map_err(|_| NormalizePrivateEventError::InvalidDecimal {
            field,
            value: value.to_string(),
        })
}

fn optional_exact_decimal(
    field: &'static str,
    value: &str,
) -> Result<Option<Decimal>, NormalizePrivateEventError> {
    if value.trim().is_empty() {
        Ok(None)
    } else {
        exact_decimal(field, value).map(Some)
    }
}

fn normalize_private_event(
    message: PolymarketWsMessage,
    holding_wallet: &str,
) -> Result<Option<(String, PolymarketNormalizedPrivateEvent)>, NormalizePrivateEventError> {
    match message {
        PolymarketWsMessage::Market(_) => Ok(None),
        PolymarketWsMessage::Reconnected => Ok(Some((
            format!("reconnected:{holding_wallet}"),
            PolymarketNormalizedPrivateEvent::Reconnected,
        ))),
        PolymarketWsMessage::User(UserWsMessage::Order(order)) => {
            let price = exact_decimal("price", &order.price)?;
            let original_shares = exact_decimal("original_size", &order.original_size)?;
            let matched_shares = optional_exact_decimal("size_matched", &order.size_matched)?;
            let status = order.status.as_ref().map(|status| status.status);
            let status_reason = order.status.and_then(|status| status.reason);
            let associated_fill_ids = order.associate_trades.unwrap_or_default();
            let revision = format!(
                "order:{}:{:?}:{}:{}:{}",
                order.id, status, order.size_matched, order.timestamp, order.event_type,
            );
            Ok(Some((
                revision,
                PolymarketNormalizedPrivateEvent::Order {
                    venue_order_id: order.id,
                    market_id: order.market.to_string(),
                    instrument_id: order.asset_id.to_string(),
                    side: order.side,
                    price,
                    original_shares,
                    matched_shares,
                    status,
                    status_reason,
                    event_type: order.event_type,
                    associated_fill_ids,
                    holding_wallet: holding_wallet.to_string(),
                    venue_timestamp: order.timestamp,
                },
            )))
        }
        PolymarketWsMessage::User(UserWsMessage::Trade(trade)) => {
            let price = exact_decimal("price", &trade.price)?;
            let gross_shares = exact_decimal("size", &trade.size)?;
            let fee_rate_bps = exact_decimal("fee_rate_bps", &trade.fee_rate_bps)?;
            let mut related_venue_order_ids = Vec::with_capacity(trade.maker_orders.len() + 1);
            related_venue_order_ids.push(trade.taker_order_id.clone());
            related_venue_order_ids.extend(
                trade
                    .maker_orders
                    .iter()
                    .map(|order| order.order_id.clone()),
            );
            related_venue_order_ids.sort_unstable();
            related_venue_order_ids.dedup();
            let revision = format!(
                "fill:{}:{:?}:{}:{}",
                trade.id, trade.status, trade.last_update, trade.timestamp,
            );
            Ok(Some((
                revision,
                PolymarketNormalizedPrivateEvent::Fill {
                    fill_id: trade.id,
                    market_id: trade.market.to_string(),
                    instrument_id: trade.asset_id.to_string(),
                    side: trade.side,
                    liquidity_side: trade.trader_side,
                    price,
                    gross_shares,
                    fee_rate_bps,
                    status: trade.status,
                    related_venue_order_ids,
                    transaction_hash: trade.transaction_hash,
                    holding_wallet: holding_wallet.to_string(),
                    venue_timestamp: trade.timestamp,
                },
            )))
        }
    }
}

fn addresses_equal(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn validate_preflight(
    request: &MarketableSharesOrderRequest,
    market: &ClobMarketResponse,
    book: &ClobBookResponse,
    tick_size: Decimal,
    fee_rate_bps: Decimal,
    balance_allowance: &BalanceAllowance,
) -> Result<MarketableOrderPreflight, PrepareMarketableOrderError> {
    if market.condition_id != request.condition_id
        || market.enable_order_book != Some(true)
        || market.active != Some(true)
        || market.closed
        || market.archived != Some(false)
        || market.accepting_orders != Some(true)
        || market.neg_risk != Some(request.neg_risk)
        || !market
            .tokens
            .iter()
            .any(|token| token.token_id == request.token_id)
    {
        return Err(PrepareMarketableOrderError::MarketNotTradable(
            "market identity/status metadata is incomplete or not actionable".to_string(),
        ));
    }
    let minimum_shares = market.minimum_order_size.ok_or_else(|| {
        PrepareMarketableOrderError::MarketNotTradable(
            "current minimum order size is unavailable".to_string(),
        )
    })?;
    if request.shares < minimum_shares {
        return Err(PrepareMarketableOrderError::BelowMinimum {
            requested: request.shares,
            minimum: minimum_shares,
        });
    }
    if tick_size <= Decimal::ZERO || request.protected_price % tick_size != Decimal::ZERO {
        return Err(PrepareMarketableOrderError::InvalidTick {
            price: request.protected_price,
            tick_size,
        });
    }

    let levels = match request.side {
        PolymarketOrderSide::Buy => &book.asks,
        PolymarketOrderSide::Sell => &book.bids,
    };
    let mut parsed = levels
        .iter()
        .map(|level| {
            let price = level.price.parse::<Decimal>().map_err(|error| {
                PrepareMarketableOrderError::Preflight(format!("invalid book price: {error}"))
            })?;
            let size = level.size.parse::<Decimal>().map_err(|error| {
                PrepareMarketableOrderError::Preflight(format!("invalid book quantity: {error}"))
            })?;
            Ok((price, size))
        })
        .collect::<Result<Vec<_>, PrepareMarketableOrderError>>()?;
    parsed.sort_by(|left, right| match request.side {
        PolymarketOrderSide::Buy => left.0.cmp(&right.0),
        PolymarketOrderSide::Sell => right.0.cmp(&left.0),
    });
    let best_price = match request.side {
        PolymarketOrderSide::Buy => parsed.iter().map(|(price, _)| *price).min(),
        PolymarketOrderSide::Sell => parsed.iter().map(|(price, _)| *price).max(),
    }
    .ok_or_else(|| PrepareMarketableOrderError::MarketNotTradable("book is empty".to_string()))?;
    let price_is_protected = match request.side {
        PolymarketOrderSide::Buy => best_price <= request.protected_price,
        PolymarketOrderSide::Sell => best_price >= request.protected_price,
    };
    if !price_is_protected {
        return Err(PrepareMarketableOrderError::PriceMoved {
            best_price,
            protected_price: request.protected_price,
        });
    }
    let executable_shares = parsed
        .iter()
        .filter(|(price, _)| match request.side {
            PolymarketOrderSide::Buy => *price <= request.protected_price,
            PolymarketOrderSide::Sell => *price >= request.protected_price,
        })
        .map(|(_, size)| *size)
        .sum::<Decimal>();
    if executable_shares < request.shares {
        return Err(PrepareMarketableOrderError::InsufficientDepth {
            requested: request.shares,
            available: executable_shares,
        });
    }

    let (buy_quote_amount, buy_crossing_price) = match request.side {
        PolymarketOrderSide::Buy => {
            let mut remaining = request.shares;
            let mut quote = Decimal::ZERO;
            let mut crossing = None;
            for (price, size) in parsed
                .iter()
                .filter(|(price, _)| *price <= request.protected_price)
            {
                if remaining.is_zero() {
                    break;
                }
                let consumed = remaining.min(*size);
                quote += consumed * *price;
                remaining -= consumed;
                crossing = Some(*price);
            }
            debug_assert!(remaining.is_zero());
            (Some(quote), crossing)
        }
        PolymarketOrderSide::Sell => (None, None),
    };
    let signed_buy_notional = if request.side == PolymarketOrderSide::Buy {
        (request.shares.trunc_with_scale(LOT_SIZE_SCALE) * request.protected_price).normalize()
    } else {
        Decimal::ZERO
    };
    if request.side == PolymarketOrderSide::Buy
        && signed_buy_notional < MARKETABLE_BUY_MINIMUM_NOTIONAL
    {
        return Err(PrepareMarketableOrderError::BelowMinimumNotional {
            requested: signed_buy_notional.normalize(),
            minimum: MARKETABLE_BUY_MINIMUM_NOTIONAL,
        });
    }

    let available_balance = balance_allowance.balance / Decimal::from(1_000_000u32);
    let required_balance = match request.side {
        PolymarketOrderSide::Buy => signed_buy_notional,
        PolymarketOrderSide::Sell => request.shares,
    };
    if available_balance < required_balance {
        return Err(PrepareMarketableOrderError::InsufficientBalance {
            required: required_balance,
            available: available_balance,
        });
    }
    // Order placement only needs the exchange which will settle this market. Requiring the
    // complete lifecycle approval plan here would incorrectly block ordinary binary orders when
    // unrelated combo/collateral-adapter permissions are absent.
    let settlement_exchange = if request.neg_risk {
        crate::signing::eip712::NEG_RISK_CTF_EXCHANGE
    } else {
        crate::signing::eip712::CTF_EXCHANGE
    };
    let required_spenders = approval_plan().filter_map(|approval| match (request.side, approval) {
        (PolymarketOrderSide::Buy, PolymarketApproval::Collateral { spender, .. }) => {
            (spender == settlement_exchange).then(|| format!("{spender:#x}"))
        }
        (
            PolymarketOrderSide::Sell,
            PolymarketApproval::ConditionalTokens {
                operator: spender, ..
            },
        ) => (spender == settlement_exchange).then(|| format!("{spender:#x}")),
        _ => None,
    });
    if required_spenders.into_iter().any(|required| {
        !balance_allowance
            .allowances
            .iter()
            .any(|(spender, allowance)| {
                addresses_equal(spender, &required) && !allowance.trim_start_matches('0').is_empty()
            })
    }) {
        return Err(PrepareMarketableOrderError::AllowanceNotReady);
    }

    Ok(MarketableOrderPreflight {
        best_price,
        executable_shares,
        buy_quote_amount,
        buy_crossing_price,
        minimum_shares,
        tick_size,
        catalog_tick_size: market.minimum_tick_size,
        catalog_tick_mismatch: market.minimum_tick_size != Some(tick_size),
        fee_rate_bps,
        available_balance,
    })
}

async fn submit_once<F, Fut>(
    prepared: PreparedMarketableOrder,
    operation: F,
) -> Result<SubmittedMarketableOrder, SubmitPreparedOrderError>
where
    F: FnOnce(PolymarketOrder, PolymarketOrderType) -> Fut,
    Fut: Future<Output = Result<OrderResponse, HttpError>>,
{
    let native_order_hash = prepared.native_order_hash;
    let response = match operation(prepared.order, prepared.time_in_force).await {
        Ok(response) => response,
        Err(error) if error.is_submit_outcome_unknown() => {
            return Err(SubmitPreparedOrderError::Unknown {
                native_order_hash,
                reason: error.strategy_reason(),
            });
        }
        Err(error) => {
            return Err(SubmitPreparedOrderError::Rejected {
                native_order_hash,
                reason: error.strategy_reason(),
            });
        }
    };

    let Some(order_id) = response.order_id.as_deref() else {
        let reason = response.error_msg.as_deref().map_or_else(
            || "response did not contain an order ID or rejection reason".to_string(),
            str::to_string,
        );
        return if response.error_msg.is_some() {
            Err(SubmitPreparedOrderError::Rejected {
                native_order_hash,
                reason,
            })
        } else {
            Err(SubmitPreparedOrderError::Unknown {
                native_order_hash,
                reason,
            })
        };
    };
    let venue_order_id =
        VenueOrderId::new_checked(order_id).map_err(|error| SubmitPreparedOrderError::Unknown {
            native_order_hash,
            reason: format!("venue returned an invalid order ID: {error}"),
        })?;
    if !response.success || venue_order_id != native_order_hash {
        return Err(SubmitPreparedOrderError::Unknown {
            native_order_hash,
            reason: "venue response did not confirm the expected native order hash".to_string(),
        });
    }

    Ok(SubmittedMarketableOrder {
        native_order_hash,
        venue_order_id,
        shares: prepared.shares,
        response,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        common::credential::{Credential, EvmPrivateKey},
        signing::eip712::OrderSigner,
        websocket::messages::{PolymarketUserOrder, PolymarketUserTrade},
    };

    fn user_fixture(name: &str) -> UserWsMessage {
        match name {
            "trade" => UserWsMessage::Trade(
                serde_json::from_str::<PolymarketUserTrade>(include_str!(
                    "../../test_data/ws_user_trade.json"
                ))
                .unwrap(),
            ),
            "placement" => UserWsMessage::Order(
                serde_json::from_str::<PolymarketUserOrder>(include_str!(
                    "../../test_data/ws_user_order_placement.json"
                ))
                .unwrap(),
            ),
            "update" => UserWsMessage::Order(
                serde_json::from_str::<PolymarketUserOrder>(include_str!(
                    "../../test_data/ws_user_order_update.json"
                ))
                .unwrap(),
            ),
            _ => panic!("unknown fixture"),
        }
    }

    #[test]
    fn normalized_private_event_preserves_exact_fee_quantity_and_wallet() {
        let mut normalizer = PolymarketPrivateEventNormalizer::default();
        let wallet = "0x1111111111111111111111111111111111111111";
        let event = normalizer
            .normalize(PolymarketWsMessage::User(user_fixture("trade")), wallet)
            .unwrap()
            .unwrap();
        let PolymarketNormalizedPrivateEvent::Fill {
            gross_shares,
            fee_rate_bps,
            price,
            holding_wallet,
            related_venue_order_ids,
            ..
        } = event
        else {
            panic!("expected normalized fill")
        };

        assert_eq!(gross_shares, dec!(25.0));
        assert_eq!(fee_rate_bps, Decimal::ZERO);
        assert_eq!(price, dec!(0.5));
        assert_eq!(holding_wallet, wallet);
        assert_eq!(related_venue_order_ids.len(), 2);
    }

    #[test]
    fn exact_private_fill_fee_uses_dynamic_curve_and_venue_rounding() {
        assert_eq!(
            exact_private_fill_fee(
                dec!(700),
                1,
                dec!(100),
                dec!(0.5),
                PolymarketLiquiditySide::Taker,
            )
            .unwrap(),
            dec!(1.75),
        );
        assert_eq!(
            exact_private_fill_fee(
                dec!(700),
                1,
                dec!(100),
                dec!(0.5),
                PolymarketLiquiditySide::Maker,
            )
            .unwrap(),
            Decimal::ZERO,
        );
        assert!(
            exact_private_fill_fee(
                dec!(10001),
                1,
                dec!(100),
                dec!(0.5),
                PolymarketLiquiditySide::Taker,
            )
            .is_err()
        );
    }

    #[test]
    fn exact_duplicate_fill_is_suppressed() {
        let mut normalizer = PolymarketPrivateEventNormalizer::default();
        let wallet = "0x1111111111111111111111111111111111111111";

        assert!(
            normalizer
                .normalize(PolymarketWsMessage::User(user_fixture("trade")), wallet,)
                .unwrap()
                .is_some()
        );
        assert!(
            normalizer
                .normalize(PolymarketWsMessage::User(user_fixture("trade")), wallet,)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn reordered_order_revisions_are_not_discarded() {
        let mut normalizer = PolymarketPrivateEventNormalizer::default();
        let wallet = "0x1111111111111111111111111111111111111111";

        for fixture in ["update", "placement"] {
            assert!(
                normalizer
                    .normalize(PolymarketWsMessage::User(user_fixture(fixture)), wallet,)
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[test]
    fn fill_before_order_ack_is_preserved() {
        let mut normalizer = PolymarketPrivateEventNormalizer::default();
        let wallet = "0x1111111111111111111111111111111111111111";
        let fill = normalizer
            .normalize(PolymarketWsMessage::User(user_fixture("trade")), wallet)
            .unwrap();
        let order = normalizer
            .normalize(PolymarketWsMessage::User(user_fixture("placement")), wallet)
            .unwrap();

        assert!(matches!(
            fill,
            Some(PolymarketNormalizedPrivateEvent::Fill { .. })
        ));
        assert!(matches!(
            order,
            Some(PolymarketNormalizedPrivateEvent::Order { .. })
        ));
    }

    const TOKEN_ID: &str =
        "71321045679252212594626385532706912750332728571942532289631379312455583992563";

    fn make_facade() -> PolymarketAgentFacade {
        let private_key = EvmPrivateKey::new(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        )
        .unwrap();
        let signer = OrderSigner::new(&private_key).unwrap();
        let signer_address = format!("{:#x}", signer.address());
        let maker_address = "0x1111111111111111111111111111111111111111".to_string();
        let builder = PolymarketOrderBuilder::new(
            signer,
            signer_address,
            maker_address.clone(),
            SignatureType::Poly1271,
        );
        let credential = Credential::new(
            "test-key",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "test-passphrase".to_string(),
        )
        .unwrap();
        let http = PolymarketClobHttpClient::new(
            credential,
            maker_address,
            Some("http://127.0.0.1:9".to_string()),
            1,
        )
        .unwrap();
        let public_http =
            PolymarketClobPublicClient::new(Some("http://127.0.0.1:9".to_string()), 1).unwrap();
        PolymarketAgentFacade::new(http, public_http, Arc::new(builder), None).unwrap()
    }

    fn five_share_request() -> MarketableSharesOrderRequest {
        MarketableSharesOrderRequest {
            condition_id: "0xcondition".to_string(),
            token_id: TOKEN_ID.to_string(),
            side: PolymarketOrderSide::Buy,
            protected_price: dec!(0.50),
            shares: dec!(5),
            time_in_force: PolymarketOrderType::FAK,
            neg_risk: false,
            tick_decimals: 2,
        }
    }

    fn passing_preflight() -> MarketableOrderPreflight {
        MarketableOrderPreflight {
            best_price: dec!(0.49),
            executable_shares: dec!(10),
            buy_quote_amount: Some(dec!(2.45)),
            buy_crossing_price: Some(dec!(0.49)),
            minimum_shares: dec!(5),
            tick_size: dec!(0.01),
            catalog_tick_size: Some(dec!(0.01)),
            catalog_tick_mismatch: false,
            fee_rate_bps: dec!(200),
            available_balance: dec!(10),
        }
    }

    fn market(minimum_order_size: Decimal) -> ClobMarketResponse {
        let mut market: ClobMarketResponse = serde_json::from_value(serde_json::json!({
            "enable_order_book": true,
            "active": true,
            "condition_id": "0xcondition",
            "closed": false,
            "archived": false,
            "accepting_orders": true,
            "minimum_order_size": 5,
            "minimum_tick_size": 0.01,
            "neg_risk": false,
            "tokens": [{"token_id": TOKEN_ID, "outcome": "Yes", "winner": false}]
        }))
        .unwrap();
        market.minimum_order_size = Some(minimum_order_size);
        market
    }

    fn book(ask_price: &str, ask_size: &str) -> ClobBookResponse {
        serde_json::from_value(serde_json::json!({
            "bids": [{"price": "0.48", "size": "20"}],
            "asks": [{"price": ask_price, "size": ask_size}]
        }))
        .unwrap()
    }

    fn balance(balance_micros: &str, allowance: &str) -> BalanceAllowance {
        serde_json::from_value(serde_json::json!({
            "balance": balance_micros,
            "allowances": {"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": allowance}
        }))
        .unwrap()
    }

    fn ready_balance(side: PolymarketOrderSide) -> BalanceAllowance {
        let allowances = approval_plan()
            .filter_map(|approval| match (side, approval) {
                (PolymarketOrderSide::Buy, PolymarketApproval::Collateral { spender, .. }) => {
                    Some((format!("{spender:#x}"), "1"))
                }
                (
                    PolymarketOrderSide::Sell,
                    PolymarketApproval::ConditionalTokens {
                        operator: spender, ..
                    },
                ) => Some((format!("{spender:#x}"), "1")),
                _ => None,
            })
            .collect::<HashMap<_, _>>();
        serde_json::from_value(serde_json::json!({
            "balance": "10000000",
            "allowances": allowances,
        }))
        .unwrap()
    }

    #[rstest]
    fn signing_accepts_exact_five_shares_without_network_io() {
        let facade = make_facade();
        let prepared = facade
            .sign_preflighted_order(five_share_request(), passing_preflight())
            .unwrap();

        assert_eq!(prepared.shares(), dec!(5));
        assert_eq!(prepared.signature_type(), SignatureType::Poly1271);
        assert_eq!(prepared.order.taker_amount, dec!(5_000_000));
        assert_eq!(prepared.order.maker_amount, dec!(2_500_000));
    }

    #[rstest]
    fn protected_buy_keeps_exact_shares_at_the_price_ceiling() {
        let facade = make_facade();
        let mut request = five_share_request();
        request.protected_price = dec!(0.99);
        let preflight = validate_preflight(
            &request,
            &market(dec!(5)),
            &book("0.56", "10"),
            dec!(0.01),
            dec!(700),
            &ready_balance(PolymarketOrderSide::Buy),
        )
        .unwrap();

        let prepared = facade.sign_preflighted_order(request, preflight).unwrap();

        assert_eq!(prepared.order.maker_amount, dec!(4_950_000));
        assert_eq!(prepared.order.taker_amount, dec!(5_000_000));
    }

    #[rstest]
    fn protected_buy_fails_closed_when_share_ceiling_exceeds_precision() {
        let facade = make_facade();
        let mut preflight = passing_preflight();
        preflight.tick_size = dec!(0.001);
        preflight.buy_quote_amount = Some(dec!(2.055));
        preflight.buy_crossing_price = Some(dec!(0.411));
        let mut request = five_share_request();
        request.protected_price = dec!(0.411);

        let error = facade
            .sign_preflighted_order(request, preflight)
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("Polymarket FAK BUY maker amount 2.055 pUSD exceeds 2 decimal places")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn ambiguous_timeout_is_never_retried_and_surfaces_unknown() {
        let facade = make_facade();
        let prepared = facade
            .sign_preflighted_order(five_share_request(), passing_preflight())
            .unwrap();
        let calls = AtomicUsize::new(0);

        let result = submit_once(prepared, |_, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err(HttpError::Timeout) }
        })
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            result,
            Err(SubmitPreparedOrderError::Unknown { .. })
        ));
    }

    #[rstest]
    #[case::moved_bbo(
        market(dec!(5)),
        book("0.51", "10"),
        balance("10000000", "10000000"),
        PrepareMarketableOrderError::PriceMoved {
            best_price: dec!(0.51),
            protected_price: dec!(0.50),
        }
    )]
    #[case::dynamic_minimum(
        market(dec!(6)),
        book("0.49", "10"),
        balance("10000000", "10000000"),
        PrepareMarketableOrderError::BelowMinimum {
            requested: dec!(5),
            minimum: dec!(6),
        }
    )]
    #[case::insufficient_depth(
        market(dec!(5)),
        book("0.49", "4.99"),
        balance("10000000", "10000000"),
        PrepareMarketableOrderError::InsufficientDepth {
            requested: dec!(5),
            available: dec!(4.99),
        }
    )]
    #[case::insufficient_balance(
        market(dec!(5)),
        book("0.49", "10"),
        balance("2449999", "10000000"),
        PrepareMarketableOrderError::InsufficientBalance {
            required: dec!(2.5),
            available: dec!(2.449999),
        }
    )]
    #[case::missing_allowance(
        market(dec!(5)),
        book("0.49", "10"),
        balance("10000000", "0"),
        PrepareMarketableOrderError::AllowanceNotReady
    )]
    fn preflight_fails_closed(
        #[case] market: ClobMarketResponse,
        #[case] book: ClobBookResponse,
        #[case] balance: BalanceAllowance,
        #[case] expected: PrepareMarketableOrderError,
    ) {
        let error = validate_preflight(
            &five_share_request(),
            &market,
            &book,
            dec!(0.01),
            dec!(200),
            &balance,
        )
        .unwrap_err();
        assert_eq!(error.to_string(), expected.to_string());
    }

    #[rstest]
    fn marketable_buy_below_native_notional_fails_before_signing() {
        let mut request = five_share_request();
        request.protected_price = dec!(0.09);
        let error = validate_preflight(
            &request,
            &market(dec!(5)),
            &book("0.09", "10"),
            dec!(0.01),
            dec!(200),
            &ready_balance(PolymarketOrderSide::Buy),
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            PrepareMarketableOrderError::BelowMinimumNotional {
                requested: dec!(0.45),
                minimum: dec!(1),
            }
            .to_string(),
        );
    }

    #[rstest]
    #[case(PolymarketOrderSide::Buy)]
    #[case(PolymarketOrderSide::Sell)]
    fn preflight_requires_only_the_side_specific_allowance_set(#[case] side: PolymarketOrderSide) {
        let mut request = five_share_request();
        request.side = side;
        if side == PolymarketOrderSide::Sell {
            request.protected_price = dec!(0.47);
        }

        let result = validate_preflight(
            &request,
            &market(dec!(5)),
            &book("0.49", "10"),
            dec!(0.01),
            dec!(200),
            &ready_balance(side),
        );

        assert!(result.is_ok());
    }

    #[rstest]
    fn marketable_buy_accepts_exact_native_minimum_notional() {
        let mut request = five_share_request();
        request.protected_price = dec!(0.20);

        let preflight = validate_preflight(
            &request,
            &market(dec!(5)),
            &book("0.20", "10"),
            dec!(0.01),
            dec!(200),
            &ready_balance(PolymarketOrderSide::Buy),
        )
        .expect("one pUSD is the inclusive native marketable BUY floor");

        assert_eq!(preflight.buy_quote_amount, Some(dec!(1)));
    }

    #[rstest]
    fn protected_limit_targets_exact_shares_before_later_book_motion() {
        let facade = make_facade();
        let mut request = five_share_request();
        request.protected_price = dec!(0.41);
        let preflight = validate_preflight(
            &request,
            &market(dec!(5)),
            &book("0.40", "10"),
            dec!(0.01),
            dec!(700),
            &ready_balance(PolymarketOrderSide::Buy),
        )
        .unwrap();

        let prepared = facade.sign_preflighted_order(request, preflight).unwrap();

        // The protected limit keeps taker shares fixed at five. If the order matches at 0.40, the
        // venue returns unused collateral rather than converting 2.05 pUSD into 5.125 shares.
        assert_eq!(prepared.order.maker_amount, dec!(2_050_000));
        assert_eq!(prepared.order.taker_amount, dec!(5_000_000));
        assert_eq!(prepared.shares(), dec!(5));
        assert_eq!(prepared.time_in_force, PolymarketOrderType::FAK);
    }

    #[rstest]
    fn live_tick_overrides_stale_catalog_tick_and_controls_signing_precision() {
        let facade = make_facade();
        let mut request = five_share_request();
        request.protected_price = dec!(0.502);
        request.shares = dec!(10);
        request.tick_decimals = 2;
        let stale_market = market(dec!(5));
        let current_book = book("0.501", "10");

        let preflight = validate_preflight(
            &request,
            &stale_market,
            &current_book,
            dec!(0.001),
            dec!(200),
            &ready_balance(PolymarketOrderSide::Buy),
        )
        .expect("live tick must be authoritative");
        assert_eq!(preflight.tick_size, dec!(0.001));
        assert_eq!(preflight.catalog_tick_size, Some(dec!(0.01)));
        assert!(preflight.catalog_tick_mismatch);

        let prepared = facade
            .sign_preflighted_order(request, preflight)
            .expect("sign with live tick precision");
        assert_eq!(prepared.order.maker_amount, dec!(5_020_000));
        assert_eq!(prepared.order.taker_amount, dec!(10_000_000));
    }

    #[rstest]
    fn stale_request_precision_cannot_bypass_live_tick() {
        let mut request = five_share_request();
        request.protected_price = dec!(0.505);
        request.tick_decimals = 3;

        let error = validate_preflight(
            &request,
            &market(dec!(5)),
            &book("0.50", "10"),
            dec!(0.01),
            dec!(200),
            &ready_balance(PolymarketOrderSide::Buy),
        )
        .expect_err("live tick must reject a nonconforming protected price");

        let PrepareMarketableOrderError::InvalidTick { price, tick_size } = error else {
            panic!("expected invalid live tick, received {error}");
        };
        assert_eq!(price, dec!(0.505));
        assert_eq!(tick_size, dec!(0.01));
    }

    #[rstest]
    fn startup_requires_an_actually_authenticated_private_stream() {
        let facade = make_facade();
        let evidence = PolymarketStartupEvidence {
            chain_id: 137,
            geoblocked: false,
            private_stream_authenticated: true,
            signer_address: facade.order_builder.signer_address().to_uppercase(),
            funder_address: facade.order_builder.maker_address().to_uppercase(),
        };
        assert_eq!(
            facade.validate_startup(&evidence),
            Err(PolymarketStartupError::PrivateStreamNotReady)
        );
    }

    #[rstest]
    fn lifecycle_capabilities_fail_closed_without_a_native_relayer() {
        let facade = make_facade();
        let capabilities = facade.capabilities();
        assert!(!capabilities.redeem);
        assert!(!capabilities.merge);
        assert!(!capabilities.split);
    }
}
