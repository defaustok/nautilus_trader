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

//! Provider-facing execution boundary for the Terminal native agent.

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    time::{Duration, SystemTime},
};

use alloy_primitives::{Address, U256};
use rand::RngExt;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use thiserror::Error;

use super::cancellation::{CancelRequest, cancel_groups, collateral_balance};
use super::lifecycle::{
    PredictFunApprovalOperation, PredictFunApprovalScope, PredictFunLifecycle,
    PredictFunLifecycleBackend, PredictFunStartupRequirements, approval_plan,
};
use crate::{
    common::{
        enums::{
            PredictFunAccountType, PredictFunEnvironment, PredictFunSide, PredictFunSignatureType,
            PredictFunStrategy,
        },
        parse::{decimal_to_wei, wei_to_decimal},
    },
    config::SecretString,
    http::{
        PredictFunHttpClient,
        error::PredictFunHttpError,
        models::{
            PredictFunAccountActivity, PredictFunBook, PredictFunContractOrder,
            PredictFunCreateOrderData, PredictFunCreateOrderResponse, PredictFunMarket,
            PredictFunMatch, PredictFunOrderRecord, PredictFunPosition,
            PredictFunRemoveOrdersResponse,
        },
    },
    signing::{
        eip712::{PredictFunOrderSigner, order_hash},
        order_builder::{
            PredictFunOrderAmounts, limit_order_amounts, market_order_amounts_by_quantity,
        },
    },
    websocket::parse::outcome_book_levels,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictFunAgentInstrument {
    pub token_id: String,
    pub price_precision: u8,
    pub fee_rate_bps: u32,
    pub is_neg_risk: bool,
    pub is_yield_bearing: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PredictFunPrepareFokOrder {
    pub client_order_id: String,
    pub instrument: PredictFunAgentInstrument,
    pub side: PredictFunSide,
    pub shares: Decimal,
    pub protected_price: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PredictFunCheckedFokOrder {
    pub order: PredictFunPrepareFokOrder,
    pub market_id: u64,
    pub is_yes: bool,
    pub minimum_shares: Decimal,
    pub minimum_notional: Decimal,
    pub quantity_step: Decimal,
    pub max_book_age_ms: u64,
}

const FOK_ORDER_TTL_SECS: u64 = 300;
const RECONCILIATION_RETRY_DELAYS: [Duration; 2] =
    [Duration::from_millis(100), Duration::from_millis(250)];

pub struct PreparedPredictFunOrder {
    client_order_id: String,
    native_order_hash: String,
    data: PredictFunCreateOrderData,
}

impl PreparedPredictFunOrder {
    #[must_use]
    pub fn client_order_id(&self) -> &str {
        &self.client_order_id
    }

    #[must_use]
    pub fn native_order_hash(&self) -> &str {
        &self.native_order_hash
    }
}

impl fmt::Debug for PreparedPredictFunOrder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct(stringify!(PreparedPredictFunOrder))
            .field("client_order_id", &self.client_order_id)
            .field("native_order_hash", &self.native_order_hash)
            .field("data", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Venue acknowledgement that the create request was accepted for processing.
///
/// This is never fill evidence. FOK completion must come from the private stream or
/// reconciliation (`MATCH`/settlement for a fill, `NO_MARKET_MATCH` for a no-fill).
pub struct PredictFunAgentSubmitResult {
    pub client_order_id: String,
    pub venue_order_id: String,
    pub native_order_hash: String,
}

#[derive(Debug, Error)]
pub enum PredictFunAgentError {
    #[error("invalid PredictFun agent request: {0}")]
    Invalid(String),
    #[error("PredictFun definitively rejected the order: {0}")]
    DefinitiveRejected(String),
    #[error("PredictFun submission outcome is unknown after dispatch: {0}")]
    UnknownAfterDispatch(String),
    #[error("PredictFun account read failed: {0}")]
    Read(String),
}

impl PredictFunAgentError {
    #[must_use]
    pub const fn is_unknown_after_dispatch(&self) -> bool {
        matches!(self, Self::UnknownAfterDispatch(_))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PredictFunAgentReconciliation {
    pub orders: Vec<PredictFunOrderRecord>,
    pub fills: Vec<PredictFunMatch>,
    pub positions: Vec<PredictFunPosition>,
    pub activity: Vec<PredictFunAccountActivity>,
}

#[allow(async_fn_in_trait)]
pub trait PredictFunAgentHttp: Send + Sync {
    async fn get_market(&self, market_id: u64) -> Result<PredictFunMarket, PredictFunHttpError>;

    async fn get_orderbook(&self, market_id: u64) -> Result<PredictFunBook, PredictFunHttpError>;

    async fn create_order(
        &self,
        token: &SecretString,
        data: PredictFunCreateOrderData,
    ) -> Result<PredictFunCreateOrderResponse, PredictFunHttpError>;

    async fn remove_orders(
        &self,
        token: &SecretString,
        ids: Vec<String>,
    ) -> Result<PredictFunRemoveOrdersResponse, PredictFunHttpError>;

    async fn get_orders(
        &self,
        token: &SecretString,
        params: Option<&HashMap<String, String>>,
    ) -> Result<Vec<PredictFunOrderRecord>, PredictFunHttpError>;

    async fn get_matches(
        &self,
        params: Option<&HashMap<String, String>>,
    ) -> Result<Vec<PredictFunMatch>, PredictFunHttpError>;

    async fn get_positions(
        &self,
        token: &SecretString,
        params: Option<&HashMap<String, String>>,
    ) -> Result<Vec<PredictFunPosition>, PredictFunHttpError>;

    async fn get_account_activity(
        &self,
        token: &SecretString,
        params: Option<&HashMap<String, String>>,
    ) -> Result<Vec<PredictFunAccountActivity>, PredictFunHttpError>;
}

impl PredictFunAgentHttp for PredictFunHttpClient {
    async fn get_market(&self, market_id: u64) -> Result<PredictFunMarket, PredictFunHttpError> {
        self.get_market(market_id).await
    }

    async fn get_orderbook(&self, market_id: u64) -> Result<PredictFunBook, PredictFunHttpError> {
        self.get_orderbook(market_id).await
    }

    async fn create_order(
        &self,
        token: &SecretString,
        data: PredictFunCreateOrderData,
    ) -> Result<PredictFunCreateOrderResponse, PredictFunHttpError> {
        self.create_order(token, data).await
    }

    async fn remove_orders(
        &self,
        token: &SecretString,
        ids: Vec<String>,
    ) -> Result<PredictFunRemoveOrdersResponse, PredictFunHttpError> {
        self.remove_orders(token, ids).await
    }

    async fn get_orders(
        &self,
        token: &SecretString,
        params: Option<&HashMap<String, String>>,
    ) -> Result<Vec<PredictFunOrderRecord>, PredictFunHttpError> {
        self.get_orders(token, params).await
    }

    async fn get_matches(
        &self,
        params: Option<&HashMap<String, String>>,
    ) -> Result<Vec<PredictFunMatch>, PredictFunHttpError> {
        self.get_matches(params).await
    }

    async fn get_positions(
        &self,
        token: &SecretString,
        params: Option<&HashMap<String, String>>,
    ) -> Result<Vec<PredictFunPosition>, PredictFunHttpError> {
        self.get_positions(token, params).await
    }

    async fn get_account_activity(
        &self,
        token: &SecretString,
        params: Option<&HashMap<String, String>>,
    ) -> Result<Vec<PredictFunAccountActivity>, PredictFunHttpError> {
        self.get_account_activity(token, params).await
    }
}

pub struct PredictFunAgentFacade<T> {
    http: T,
    signer: PredictFunOrderSigner,
    private_key: SecretString,
    environment: PredictFunEnvironment,
    account_type: PredictFunAccountType,
    account_address: Address,
}

impl<T> fmt::Debug for PredictFunAgentFacade<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PredictFunAgentFacade")
            .field("http", &"<redacted>")
            .field("signer", &"<redacted>")
            .field("private_key", &"<redacted>")
            .field("environment", &self.environment)
            .field("account_type", &self.account_type)
            .field("account_address", &self.account_address)
            .finish()
    }
}

impl<T: PredictFunAgentHttp> PredictFunAgentFacade<T> {
    pub fn new(
        http: T,
        private_key: SecretString,
        environment: PredictFunEnvironment,
        account_type: PredictFunAccountType,
        account_address: Address,
    ) -> Result<Self, PredictFunAgentError> {
        let signer = PredictFunOrderSigner::new(private_key.expose())
            .map_err(|e| PredictFunAgentError::Invalid(e.to_string()))?;
        if account_type == PredictFunAccountType::Eoa && signer.address() != account_address {
            return Err(PredictFunAgentError::Invalid(
                "EOA account address does not match the local signer".to_string(),
            ));
        }
        Ok(Self {
            http,
            signer,
            private_key,
            environment,
            account_type,
            account_address,
        })
    }

    pub fn prepare_fok_order(
        &self,
        request: PredictFunPrepareFokOrder,
    ) -> Result<PreparedPredictFunOrder, PredictFunAgentError> {
        let now_secs = SystemTime::UNIX_EPOCH
            .elapsed()
            .map_err(|error| PredictFunAgentError::Invalid(error.to_string()))?
            .as_secs();
        self.prepare_fok_order_at(request, now_secs)
    }

    /// Clock-injectable preparation entry point for deterministic expiry tests.
    pub fn prepare_fok_order_at(
        &self,
        request: PredictFunPrepareFokOrder,
        now_secs: u64,
    ) -> Result<PreparedPredictFunOrder, PredictFunAgentError> {
        if request.client_order_id.trim().is_empty() {
            return Err(PredictFunAgentError::Invalid(
                "client order ID cannot be empty".to_string(),
            ));
        }
        if request.protected_price.normalize().scale()
            > u32::from(request.instrument.price_precision)
        {
            return Err(PredictFunAgentError::Invalid(
                "protected price is not aligned to the instrument tick".to_string(),
            ));
        }
        let requested_shares = decimal_to_wei(request.shares)
            .map_err(|e| PredictFunAgentError::Invalid(e.to_string()))?;
        let requested_price = decimal_to_wei(request.protected_price)
            .map_err(|e| PredictFunAgentError::Invalid(e.to_string()))?;
        let amounts = limit_order_amounts(request.side, request.protected_price, request.shares)
            .map_err(|e| PredictFunAgentError::Invalid(e.to_string()))?;
        if amounts.amount != requested_shares || amounts.price_per_share != requested_price {
            return Err(PredictFunAgentError::Invalid(
                "shares or protected price require unsupported significant-digit rounding"
                    .to_string(),
            ));
        }
        self.prepare_fok_order_with_amounts_at(request, &amounts, now_secs)
    }

    fn prepare_fok_order_with_amounts_at(
        &self,
        request: PredictFunPrepareFokOrder,
        amounts: &PredictFunOrderAmounts,
        now_secs: u64,
    ) -> Result<PreparedPredictFunOrder, PredictFunAgentError> {
        let maker = match self.account_type {
            PredictFunAccountType::Eoa => self.signer.address(),
            PredictFunAccountType::PredictAccount => self.account_address,
        };
        let mut order = PredictFunContractOrder {
            salt: rand::rng().random_range(0..=2_147_483_648u32).to_string(),
            maker: format!("{maker:#x}"),
            signer: format!("{maker:#x}"),
            taker: format!("{:#x}", Address::ZERO),
            token_id: request.instrument.token_id,
            maker_amount: amounts.maker_amount.to_string(),
            taker_amount: amounts.taker_amount.to_string(),
            expiration: now_secs.saturating_add(FOK_ORDER_TTL_SECS).to_string(),
            nonce: "0".to_string(),
            fee_rate_bps: request.instrument.fee_rate_bps.to_string(),
            side: request.side,
            signature_type: PredictFunSignatureType::Eoa,
            signature: None,
            hash: None,
        };
        let hash = order_hash(
            &order,
            self.environment,
            request.instrument.is_neg_risk,
            request.instrument.is_yield_bearing,
        )
        .map_err(|e| PredictFunAgentError::Invalid(e.to_string()))?;
        let signature = match self.account_type {
            PredictFunAccountType::Eoa => self.signer.sign_order(
                &order,
                self.environment,
                request.instrument.is_neg_risk,
                request.instrument.is_yield_bearing,
            ),
            PredictFunAccountType::PredictAccount => self.signer.sign_order_for_predict_account(
                &order,
                self.account_address,
                self.environment,
                request.instrument.is_neg_risk,
                request.instrument.is_yield_bearing,
            ),
        }
        .map_err(|e| PredictFunAgentError::Invalid(e.to_string()))?;
        let native_order_hash = format!("{hash:#x}");
        order.hash = Some(native_order_hash.clone());
        order.signature = Some(signature);
        Ok(PreparedPredictFunOrder {
            client_order_id: request.client_order_id,
            native_order_hash,
            data: PredictFunCreateOrderData {
                price_per_share: amounts.price_per_share.to_string(),
                strategy: PredictFunStrategy::Market,
                order,
                slippage_bps: Some(amounts.slippage_bps.to_string()),
                is_fill_or_kill: Some(true),
                is_post_only: None,
                self_trade_prevention: None,
                is_min_amount_out: Some(false),
            },
        })
    }

    /// Performs fresh native market, full-depth, account and scoped-approval checks before
    /// signing. The caller must separately gate authenticated private-stream recovery.
    pub async fn prepare_fok_order_checked<B: PredictFunLifecycleBackend>(
        &self,
        token: &SecretString,
        lifecycle: &PredictFunLifecycle<B>,
        startup: &PredictFunStartupRequirements,
        request: PredictFunCheckedFokOrder,
    ) -> Result<PreparedPredictFunOrder, PredictFunAgentError> {
        let (market, book) = tokio::try_join!(
            self.http.get_market(request.market_id),
            self.http.get_orderbook(request.market_id),
        )
        .map_err(|error| PredictFunAgentError::Read(error.to_string()))?;
        let (positions, active_orders) = tokio::try_join!(
            self.list_positions(token, Some(request.market_id)),
            self.list_orders(token, true),
        )?;
        if market.id != request.market_id
            || market.trading_status != "OPEN"
            || market.status != "REGISTERED"
            || market.decimal_precision != request.order.instrument.price_precision
            || market.fee_rate_bps != request.order.instrument.fee_rate_bps
            || market.is_neg_risk != request.order.instrument.is_neg_risk
            || market.is_yield_bearing != request.order.instrument.is_yield_bearing
            || !market.outcomes.iter().any(|outcome| {
                outcome.on_chain_id == request.order.instrument.token_id
                    && outcome.index_set == if request.is_yes { 1 } else { 2 }
                    && (if request.is_yes {
                        outcome.name.eq_ignore_ascii_case("YES")
                            || outcome.name.eq_ignore_ascii_case("UP")
                    } else {
                        outcome.name.eq_ignore_ascii_case("NO")
                            || outcome.name.eq_ignore_ascii_case("DOWN")
                    })
            })
        {
            return Err(PredictFunAgentError::Invalid(
                "MARKET_NOT_TRADABLE: fresh market identity, fee, precision or status changed"
                    .to_string(),
            ));
        }
        let now_ms = SystemTime::UNIX_EPOCH
            .elapsed()
            .map_err(|error| PredictFunAgentError::Invalid(error.to_string()))?
            .as_millis() as u64;
        if request.max_book_age_ms == 0
            || book.update_timestamp_ms == 0
            || book.update_timestamp_ms > now_ms.saturating_add(5_000)
            || now_ms.saturating_sub(book.update_timestamp_ms) > request.max_book_age_ms
        {
            return Err(PredictFunAgentError::Invalid(
                "STALE_BOOK: fresh native order book is too old".to_string(),
            ));
        }
        let shares = request.order.shares;
        if request.order.side == PredictFunSide::Sell {
            let available = available_sell_shares(
                &positions,
                &active_orders,
                &request.order.instrument.token_id,
            )?;
            if available < shares {
                return Err(PredictFunAgentError::Invalid(format!(
                    "BALANCE_INSUFFICIENT: required {shares} outcome shares, available {available}"
                )));
            }
        }
        if shares < request.minimum_shares
            || request.quantity_step <= Decimal::ZERO
            || shares % request.quantity_step != Decimal::ZERO
        {
            return Err(PredictFunAgentError::Invalid(
                "VENUE_MINIMUM: shares, notional or quantity step is invalid".to_string(),
            ));
        }
        let (bids, asks) = outcome_book_levels(
            &book,
            request.market_id,
            market.decimal_precision,
            request.is_yes,
        )
        .map_err(|error| PredictFunAgentError::Read(error.to_string()))?;
        let unprotected_amounts =
            market_order_amounts_by_quantity(request.order.side, shares, &bids, &asks, 0, false)
                .map_err(|error| PredictFunAgentError::Invalid(error.to_string()))?;
        let worst_book_price = wei_to_decimal(unprotected_amounts.last_price)
            .map_err(|error| PredictFunAgentError::Invalid(error.to_string()))?;
        let slippage_bps = protected_slippage_bps(
            request.order.side,
            worst_book_price,
            request.order.protected_price,
        )?;
        let amounts = market_order_amounts_by_quantity(
            request.order.side,
            shares,
            &bids,
            &asks,
            slippage_bps,
            false,
        )
        .map_err(|error| PredictFunAgentError::Invalid(error.to_string()))?;
        let requested_shares = decimal_to_wei(shares)
            .map_err(|error| PredictFunAgentError::Invalid(error.to_string()))?;
        if amounts.amount != requested_shares {
            return Err(PredictFunAgentError::Invalid(
                "shares require unsupported significant-digit rounding".to_string(),
            ));
        }
        let last_price = wei_to_decimal(amounts.last_price)
            .map_err(|error| PredictFunAgentError::Invalid(error.to_string()))?;
        let price_moved = match request.order.side {
            PredictFunSide::Buy => last_price > request.order.protected_price,
            PredictFunSide::Sell => last_price < request.order.protected_price,
        };
        if price_moved {
            return Err(PredictFunAgentError::Invalid(format!(
                "PRICE_MOVED: worst executable price {last_price} exceeds protection {}",
                request.order.protected_price
            )));
        }
        let signed_amount = match request.order.side {
            PredictFunSide::Buy => amounts.maker_amount,
            PredictFunSide::Sell => amounts.taker_amount,
        };
        let signed_price = wei_to_decimal(signed_amount)
            .map_err(|error| PredictFunAgentError::Invalid(error.to_string()))?
            / shares;
        let signed_price_outside_protection = match request.order.side {
            PredictFunSide::Buy => signed_price > request.order.protected_price,
            PredictFunSide::Sell => signed_price < request.order.protected_price,
        };
        if signed_price_outside_protection {
            return Err(PredictFunAgentError::Invalid(
                "PRICE_MOVED: signed market order exceeds protected price".to_string(),
            ));
        }
        if shares * last_price < request.minimum_notional {
            return Err(PredictFunAgentError::Invalid(
                "VENUE_MINIMUM: executable native notional is below the current minimum"
                    .to_string(),
            ));
        }

        let mut exact_startup = startup.clone();
        // CLOB order creation is off-chain and gasless. Gas is only relevant to an explicitly
        // selected raw-transaction fallback for lifecycle operations.
        exact_startup.minimum_gas_balance = U256::ZERO;
        let required_allowance = if request.order.side == PredictFunSide::Buy {
            amounts.maker_amount
        } else {
            U256::ZERO
        };
        exact_startup.minimum_collateral_balance = required_allowance;
        exact_startup.required_approvals = approval_plan(
            exact_startup.environment,
            PredictFunApprovalScope {
                operation: PredictFunApprovalOperation::Trade,
                is_neg_risk: request.order.instrument.is_neg_risk,
                is_yield_bearing: request.order.instrument.is_yield_bearing,
                side: Some(request.order.side),
                required_allowance,
            },
        )
        .map_err(|error| PredictFunAgentError::Invalid(error.to_string()))?;
        lifecycle
            .startup_readiness(&exact_startup)
            .await
            .map_err(|error| PredictFunAgentError::Read(error.to_string()))?;

        let now_secs = SystemTime::UNIX_EPOCH
            .elapsed()
            .map_err(|error| PredictFunAgentError::Invalid(error.to_string()))?
            .as_secs();
        self.prepare_fok_order_with_amounts_at(request.order, &amounts, now_secs)
    }

    pub async fn submit_prepared(
        &self,
        token: &SecretString,
        prepared: PreparedPredictFunOrder,
    ) -> Result<PredictFunAgentSubmitResult, PredictFunAgentError> {
        let PreparedPredictFunOrder {
            client_order_id,
            native_order_hash,
            data,
        } = prepared;
        match self.http.create_order(token, data).await {
            Ok(response) => {
                if !response.order_hash.eq_ignore_ascii_case(&native_order_hash) {
                    return Err(PredictFunAgentError::UnknownAfterDispatch(
                        "venue response order hash does not match the prepared order".to_string(),
                    ));
                }
                Ok(PredictFunAgentSubmitResult {
                    client_order_id,
                    venue_order_id: response.order_id,
                    native_order_hash,
                })
            }
            Err(e) if e.is_definitive_rejection() => {
                Err(PredictFunAgentError::DefinitiveRejected(e.to_string()))
            }
            Err(e) => Err(PredictFunAgentError::UnknownAfterDispatch(e.to_string())),
        }
    }

    pub async fn list_orders(
        &self,
        token: &SecretString,
        active_only: bool,
    ) -> Result<Vec<PredictFunOrderRecord>, PredictFunAgentError> {
        let params =
            active_only.then(|| HashMap::from([("status".to_string(), "OPEN".to_string())]));
        self.http
            .get_orders(token, params.as_ref())
            .await
            .map_err(|e| PredictFunAgentError::Read(e.to_string()))
    }

    pub async fn list_fills(
        &self,
        signer_address: Address,
    ) -> Result<Vec<PredictFunMatch>, PredictFunAgentError> {
        let params = HashMap::from([("signerAddress".to_string(), format!("{signer_address:#x}"))]);
        self.http
            .get_matches(Some(&params))
            .await
            .map_err(|e| PredictFunAgentError::Read(e.to_string()))
    }

    pub async fn list_positions(
        &self,
        token: &SecretString,
        market_id: Option<u64>,
    ) -> Result<Vec<PredictFunPosition>, PredictFunAgentError> {
        let params =
            market_id.map(|value| HashMap::from([("marketId".to_string(), value.to_string())]));
        self.http
            .get_positions(token, params.as_ref())
            .await
            .map_err(|e| PredictFunAgentError::Read(e.to_string()))
    }

    pub async fn get_market(
        &self,
        market_id: u64,
    ) -> Result<PredictFunMarket, PredictFunAgentError> {
        self.http
            .get_market(market_id)
            .await
            .map_err(|e| PredictFunAgentError::Read(e.to_string()))
    }

    pub async fn reconcile(
        &self,
        token: &SecretString,
    ) -> Result<PredictFunAgentReconciliation, PredictFunAgentError> {
        let orders = retry_reconciliation_read(|| self.http.get_orders(token, None)).await?;
        let match_params = HashMap::from([(
            "signerAddress".to_string(),
            format!("{:#x}", self.account_address),
        )]);
        let fills =
            retry_reconciliation_read(|| self.http.get_matches(Some(&match_params))).await?;
        let positions = retry_reconciliation_read(|| self.http.get_positions(token, None)).await?;
        let activity =
            retry_reconciliation_read(|| self.http.get_account_activity(token, None)).await?;
        Ok(PredictFunAgentReconciliation {
            orders,
            fills,
            positions,
            activity,
        })
    }

    pub async fn collateral_balance(
        &self,
        rpc_url: &SecretString,
    ) -> Result<Decimal, PredictFunAgentError> {
        let balance = collateral_balance(rpc_url, self.environment, self.account_address)
            .await
            .map_err(|e| PredictFunAgentError::Read(e.to_string()))?;
        wei_to_decimal(balance).map_err(|e| PredictFunAgentError::Read(e.to_string()))
    }

    pub async fn cancel_order(
        &self,
        token: &SecretString,
        venue_order_id: &str,
        rpc_url: &SecretString,
        timeout_secs: u64,
    ) -> Result<(), PredictFunAgentError> {
        let records = self.list_orders(token, false).await?;
        let record = records
            .into_iter()
            .find(|record| record.id == venue_order_id)
            .ok_or_else(|| {
                PredictFunAgentError::Read(format!("venue order {venue_order_id} was not found"))
            })?;
        let _ = self
            .http
            .remove_orders(token, vec![venue_order_id.to_string()])
            .await;
        let mut outcomes = cancel_groups(
            vec![CancelRequest {
                venue_order_id: venue_order_id.to_string(),
                order: record.order,
                is_neg_risk: record.is_neg_risk,
                is_yield_bearing: record.is_yield_bearing,
            }],
            rpc_url,
            &self.private_key,
            self.environment,
            self.account_type,
            self.account_address,
            timeout_secs,
        )
        .await;
        outcomes
            .remove(venue_order_id)
            .ok_or_else(|| {
                PredictFunAgentError::UnknownAfterDispatch(
                    "cancellation returned no outcome".to_string(),
                )
            })?
            .map_err(|e| PredictFunAgentError::UnknownAfterDispatch(e.to_string()))
    }
}

async fn retry_reconciliation_read<T, F, Fut>(mut read: F) -> Result<T, PredictFunAgentError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, PredictFunHttpError>>,
{
    let mut retry = 0;
    loop {
        match read().await {
            Ok(value) => return Ok(value),
            Err(e) if e.is_retryable_read() && retry < RECONCILIATION_RETRY_DELAYS.len() => {
                tokio::time::sleep(RECONCILIATION_RETRY_DELAYS[retry]).await;
                retry += 1;
            }
            Err(e) => return Err(PredictFunAgentError::Read(e.to_string())),
        }
    }
}

fn protected_slippage_bps(
    side: PredictFunSide,
    worst_book_price: Decimal,
    protected_price: Decimal,
) -> Result<u32, PredictFunAgentError> {
    if worst_book_price <= Decimal::ZERO || protected_price <= Decimal::ZERO {
        return Err(PredictFunAgentError::Invalid(
            "PRICE_MOVED: book or protected price is invalid".to_string(),
        ));
    }
    let tolerance = match side {
        PredictFunSide::Buy => protected_price
            .checked_div(worst_book_price)
            .and_then(|ratio| ratio.checked_sub(Decimal::ONE)),
        PredictFunSide::Sell => Decimal::ONE.checked_sub(
            protected_price
                .checked_div(worst_book_price)
                .ok_or_else(|| PredictFunAgentError::Invalid("price ratio overflow".into()))?,
        ),
    }
    .ok_or_else(|| PredictFunAgentError::Invalid("price ratio overflow".into()))?;
    if tolerance < Decimal::ZERO {
        return Err(PredictFunAgentError::Invalid(
            "PRICE_MOVED: current book is outside protected price".to_string(),
        ));
    }
    let bps = (tolerance * Decimal::from(10_000_u32)).floor();
    bps.to_u32()
        .map(|value| value.min(10_000))
        .ok_or_else(|| PredictFunAgentError::Invalid("slippage conversion overflow".into()))
}

fn available_sell_shares(
    positions: &[PredictFunPosition],
    active_orders: &[PredictFunOrderRecord],
    token_id: &str,
) -> Result<Decimal, PredictFunAgentError> {
    let inventory = positions
        .iter()
        .filter(|position| position.outcome.on_chain_id == token_id)
        .try_fold(Decimal::ZERO, |total, position| {
            let amount = position.amount.parse::<Decimal>().map_err(|error| {
                PredictFunAgentError::Read(format!(
                    "invalid outcome position amount {}: {error}",
                    position.amount
                ))
            })?;
            total.checked_add(amount).ok_or_else(|| {
                PredictFunAgentError::Read("outcome position amount overflow".to_string())
            })
        })?;
    let locked = active_orders
        .iter()
        .filter(|record| {
            record.order.side == PredictFunSide::Sell && record.order.token_id == token_id
        })
        .try_fold(Decimal::ZERO, |total, record| {
            let amount = record.amount.parse::<Decimal>().map_err(|error| {
                PredictFunAgentError::Read(format!(
                    "invalid active order amount {}: {error}",
                    record.amount
                ))
            })?;
            let filled = record.amount_filled.parse::<Decimal>().map_err(|error| {
                PredictFunAgentError::Read(format!(
                    "invalid active order filled amount {}: {error}",
                    record.amount_filled
                ))
            })?;
            let remaining = amount.checked_sub(filled).ok_or_else(|| {
                PredictFunAgentError::Read("active order remaining amount overflow".to_string())
            })?;
            total
                .checked_add(remaining.max(Decimal::ZERO))
                .ok_or_else(|| {
                    PredictFunAgentError::Read("locked outcome amount overflow".to_string())
                })
        })?;
    inventory.checked_sub(locked).ok_or_else(|| {
        PredictFunAgentError::Invalid(format!(
            "BALANCE_INSUFFICIENT: active SELL orders lock {locked} shares but inventory is {inventory}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;

    const PRIVATE_KEY: &str = "0x59c6995e998f97a5a0044976f0945389dc9e86dae88c7a84119f7378df164fb7";

    #[derive(Debug, Clone)]
    enum SubmitBehavior {
        Accept,
        Reject,
        Ambiguous,
    }

    #[derive(Debug, Clone)]
    struct MockHttp {
        submit_behavior: SubmitBehavior,
        calls: Arc<AtomicUsize>,
        submitted: Arc<Mutex<Vec<PredictFunCreateOrderData>>>,
    }

    impl MockHttp {
        fn new(submit_behavior: SubmitBehavior) -> Self {
            Self {
                submit_behavior,
                calls: Arc::new(AtomicUsize::new(0)),
                submitted: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl PredictFunAgentHttp for MockHttp {
        async fn get_market(
            &self,
            _market_id: u64,
        ) -> Result<PredictFunMarket, PredictFunHttpError> {
            unreachable!("not used by submit tests")
        }

        async fn get_orderbook(
            &self,
            _market_id: u64,
        ) -> Result<PredictFunBook, PredictFunHttpError> {
            unreachable!("not used by submit tests")
        }

        async fn create_order(
            &self,
            _token: &SecretString,
            data: PredictFunCreateOrderData,
        ) -> Result<PredictFunCreateOrderResponse, PredictFunHttpError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let hash = data.order.hash.clone().expect("prepared hash");
            self.submitted.lock().expect("mock lock").push(data);
            match self.submit_behavior {
                SubmitBehavior::Accept => Ok(PredictFunCreateOrderResponse {
                    code: "OK".to_string(),
                    order_id: "venue-1".to_string(),
                    order_hash: hash,
                    removal_locked_until: None,
                }),
                SubmitBehavior::Reject => Err(PredictFunHttpError::Status {
                    status: 400,
                    message: "rejected".to_string(),
                }),
                SubmitBehavior::Ambiguous => Err(PredictFunHttpError::Transport(
                    "timeout after write".to_string(),
                )),
            }
        }

        async fn remove_orders(
            &self,
            _token: &SecretString,
            _ids: Vec<String>,
        ) -> Result<PredictFunRemoveOrdersResponse, PredictFunHttpError> {
            unreachable!("not used by submit tests")
        }

        async fn get_orders(
            &self,
            _token: &SecretString,
            _params: Option<&HashMap<String, String>>,
        ) -> Result<Vec<PredictFunOrderRecord>, PredictFunHttpError> {
            unreachable!("not used by submit tests")
        }

        async fn get_matches(
            &self,
            _params: Option<&HashMap<String, String>>,
        ) -> Result<Vec<PredictFunMatch>, PredictFunHttpError> {
            unreachable!("not used by submit tests")
        }

        async fn get_positions(
            &self,
            _token: &SecretString,
            _params: Option<&HashMap<String, String>>,
        ) -> Result<Vec<PredictFunPosition>, PredictFunHttpError> {
            unreachable!("not used by submit tests")
        }

        async fn get_account_activity(
            &self,
            _token: &SecretString,
            _params: Option<&HashMap<String, String>>,
        ) -> Result<Vec<PredictFunAccountActivity>, PredictFunHttpError> {
            unreachable!("not used by submit tests")
        }
    }

    fn facade(behavior: SubmitBehavior) -> PredictFunAgentFacade<MockHttp> {
        let http = MockHttp::new(behavior);
        let private_key = SecretString::new(PRIVATE_KEY.to_string()).unwrap();
        let signer = PredictFunOrderSigner::new(PRIVATE_KEY).unwrap();
        PredictFunAgentFacade::new(
            http,
            private_key,
            PredictFunEnvironment::Testnet,
            PredictFunAccountType::Eoa,
            signer.address(),
        )
        .unwrap()
    }

    fn exact_five_share_request() -> PredictFunPrepareFokOrder {
        PredictFunPrepareFokOrder {
            client_order_id: "terminal-leg-1".to_string(),
            instrument: PredictFunAgentInstrument {
                token_id: "12345".to_string(),
                price_precision: 2,
                fee_rate_bps: 200,
                is_neg_risk: false,
                is_yield_bearing: false,
            },
            side: PredictFunSide::Buy,
            shares: dec!(5),
            protected_price: dec!(0.42),
        }
    }

    #[rstest]
    fn prepare_exact_five_shares_is_network_free_and_fok_only() {
        let facade = facade(SubmitBehavior::Accept);

        let prepared = facade
            .prepare_fok_order(exact_five_share_request())
            .unwrap();

        assert_eq!(facade.http.calls.load(Ordering::Relaxed), 0);
        assert_eq!(prepared.data.strategy, PredictFunStrategy::Market);
        assert_eq!(prepared.data.is_fill_or_kill, Some(true));
        assert_eq!(prepared.data.order.taker_amount, "5000000000000000000");
        assert!(!format!("{prepared:?}").contains("signature"));
    }

    #[rstest]
    fn prepare_forces_short_expiry_from_agent_clock() {
        let facade = facade(SubmitBehavior::Accept);
        let prepared = facade
            .prepare_fok_order_at(exact_five_share_request(), 1_800_000_000)
            .unwrap();

        assert_eq!(prepared.data.order.expiration, "1800000300");
    }

    #[rstest]
    #[tokio::test]
    async fn submit_prepared_returns_ack_without_detached_success() {
        let facade = facade(SubmitBehavior::Accept);
        let prepared = facade
            .prepare_fok_order(exact_five_share_request())
            .unwrap();
        let token = SecretString::new("jwt".to_string()).unwrap();

        let result = facade.submit_prepared(&token, prepared).await.unwrap();

        assert_eq!(result.venue_order_id, "venue-1");
        assert_eq!(facade.http.calls.load(Ordering::Relaxed), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn definite_rejection_is_not_unknown() {
        let facade = facade(SubmitBehavior::Reject);
        let prepared = facade
            .prepare_fok_order(exact_five_share_request())
            .unwrap();
        let token = SecretString::new("jwt".to_string()).unwrap();

        let error = facade.submit_prepared(&token, prepared).await.unwrap_err();

        assert!(matches!(error, PredictFunAgentError::DefinitiveRejected(_)));
        assert_eq!(facade.http.calls.load(Ordering::Relaxed), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn ambiguous_timeout_is_unknown_and_never_retried() {
        let facade = facade(SubmitBehavior::Ambiguous);
        let prepared = facade
            .prepare_fok_order(exact_five_share_request())
            .unwrap();
        let token = SecretString::new("jwt".to_string()).unwrap();

        let error = facade.submit_prepared(&token, prepared).await.unwrap_err();

        assert!(error.is_unknown_after_dispatch());
        assert_eq!(facade.http.calls.load(Ordering::Relaxed), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn reconciliation_read_recovers_from_transient_server_errors() {
        let calls = AtomicUsize::new(0);

        let value = retry_reconciliation_read(|| {
            let attempt = calls.fetch_add(1, Ordering::Relaxed);
            async move {
                if attempt < 2 {
                    Err(PredictFunHttpError::Status {
                        status: 500,
                        message: "temporary server error".to_string(),
                    })
                } else {
                    Ok("reconciled")
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(value, "reconciled");
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[rstest]
    #[tokio::test]
    async fn reconciliation_read_fails_after_bounded_retries() {
        let calls = AtomicUsize::new(0);

        let error = retry_reconciliation_read(|| {
            calls.fetch_add(1, Ordering::Relaxed);
            async {
                Err::<(), _>(PredictFunHttpError::Status {
                    status: 503,
                    message: "temporarily unavailable".to_string(),
                })
            }
        })
        .await
        .unwrap_err();

        assert!(error.to_string().contains("HTTP 503"));
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[rstest]
    fn active_sell_orders_are_subtracted_from_outcome_inventory() {
        let position = PredictFunPosition {
            id: "position-1".to_string(),
            market: crate::http::models::PredictFunPositionMarket { id: 42 },
            outcome: crate::http::models::PredictFunPositionOutcome {
                name: "YES".to_string(),
                index_set: 1,
                on_chain_id: "12345".to_string(),
            },
            amount: "5".to_string(),
            value_usd: "2.5".to_string(),
            average_buy_price_usd: "0.5".to_string(),
            pnl_usd: "0".to_string(),
        };
        let mut order = exact_five_share_request();
        order.side = PredictFunSide::Sell;
        let prepared = facade(SubmitBehavior::Accept)
            .prepare_fok_order_at(order, 1_800_000_000)
            .unwrap();
        let record = PredictFunOrderRecord {
            id: "order-1".to_string(),
            order_hash: Some(prepared.native_order_hash.clone()),
            market_id: 42,
            status: "OPEN".to_string(),
            amount: "3".to_string(),
            amount_filled: "1".to_string(),
            is_neg_risk: false,
            is_yield_bearing: false,
            strategy: PredictFunStrategy::Market,
            order: prepared.data.order,
        };

        let available = available_sell_shares(&[position], &[record], "12345").unwrap();

        assert_eq!(available, dec!(3));
    }

    #[rstest]
    fn sdk_market_amounts_sign_worst_price_and_publish_vwap() {
        let asks = vec![
            crate::http::models::PredictFunLevel(dec!(0.40), dec!(2)),
            crate::http::models::PredictFunLevel(dec!(0.42), dec!(3)),
        ];
        let amounts =
            market_order_amounts_by_quantity(PredictFunSide::Buy, dec!(5), &[], &asks, 0, false)
                .unwrap();
        let prepared = facade(SubmitBehavior::Accept)
            .prepare_fok_order_with_amounts_at(exact_five_share_request(), &amounts, 1_800_000_000)
            .unwrap();

        assert_eq!(prepared.data.price_per_share, "412000000000000000");
        assert_eq!(prepared.data.order.maker_amount, "2100000000000000000");
        assert_eq!(prepared.data.order.taker_amount, "5000000000000000000");
        assert_eq!(prepared.data.is_fill_or_kill, Some(true));
    }

    #[rstest]
    fn protected_market_buy_uses_sdk_slippage_without_changing_five_share_amount() {
        let asks = vec![crate::http::models::PredictFunLevel(dec!(0.39), dec!(5))];
        let slippage_bps =
            protected_slippage_bps(PredictFunSide::Buy, dec!(0.39), dec!(0.42)).unwrap();
        let amounts = market_order_amounts_by_quantity(
            PredictFunSide::Buy,
            dec!(5),
            &[],
            &asks,
            slippage_bps,
            false,
        )
        .unwrap();
        let prepared = facade(SubmitBehavior::Accept)
            .prepare_fok_order_with_amounts_at(exact_five_share_request(), &amounts, 1_800_000_000)
            .unwrap();

        assert_eq!(slippage_bps, 769);
        assert_eq!(prepared.data.order.taker_amount, "5000000000000000000");
        assert_eq!(prepared.data.order.maker_amount, "2099955000000000000");
        assert_eq!(prepared.data.price_per_share, "390000000000000000");
        assert_eq!(prepared.data.slippage_bps.as_deref(), Some("769"));
        assert_eq!(prepared.data.is_fill_or_kill, Some(true));
        assert_eq!(prepared.data.is_min_amount_out, Some(false));
    }
}
