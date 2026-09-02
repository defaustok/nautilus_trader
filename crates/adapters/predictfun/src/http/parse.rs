use std::str::FromStr;

use jiff::Timestamp;
use nautilus_core::{Params, UnixNanos, datetime::NANOSECONDS_IN_MILLISECOND};
use nautilus_model::{
    enums::{AssetClass, CurrencyType},
    identifiers::{InstrumentId, Symbol},
    instruments::{BinaryOption, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use rust_decimal::Decimal;
use serde_json::json;
use ustr::Ustr;

use super::models::{PredictFunMarket, PredictFunOutcome};
use crate::common::consts::{NAUTILUS_QUANTITY_PRECISION, PREDICTFUN_VENUE};

pub fn instrument_id(outcome: &PredictFunOutcome) -> InstrumentId {
    InstrumentId::new(Symbol::new(&outcome.on_chain_id), *PREDICTFUN_VENUE)
}

pub fn create_instrument(
    market: &PredictFunMarket,
    outcome: &PredictFunOutcome,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    let precision = market.decimal_precision;
    let increment_decimal = Decimal::new(1, u32::from(precision));
    let price_increment = Price::from_decimal_dp(increment_decimal, precision)?;
    let size_increment = Quantity::from("0.0000000000000001");
    let activation_ns = parse_timestamp(market.starts_at.as_deref()).unwrap_or_default();
    // The documented `/markets` response does not include category end times. Treat an
    // unknown expiry as open-ended; Unix epoch would make every listed live market expired.
    let expiration_ns =
        parse_timestamp(market.ends_at.as_deref()).unwrap_or_else(|| UnixNanos::from(u64::MAX));
    let max_price = Price::from_decimal_dp(Decimal::ONE - increment_decimal, precision)?;
    let fee = Decimal::from(market.fee_rate_bps) / Decimal::from(10_000u32);
    let info: Params = serde_json::from_value(json!({
        "marketId": market.id,
        "conditionId": market.condition_id,
        "outcomeIndexSet": outcome.index_set,
        "outcomeOnChainId": outcome.on_chain_id,
        "isNegRisk": market.is_neg_risk,
        "isYieldBearing": market.is_yield_bearing,
        "feeRateBps": market.fee_rate_bps,
        "minimumShares": "0.01",
        "tradingStatus": market.trading_status,
        "categorySlug": market.category_slug,
        "marketVariant": market.market_variant,
        "variantData": market.variant_data,
        "startsAt": market.starts_at,
        "endsAt": market.ends_at,
    }))?;
    let currency = Currency::new_checked(
        "USDT",
        NAUTILUS_QUANTITY_PRECISION,
        0,
        "Tether USD",
        CurrencyType::Crypto,
    )?;

    let instrument = BinaryOption::new_checked(
        instrument_id(outcome),
        Symbol::new(&outcome.on_chain_id),
        AssetClass::Alternative,
        currency,
        activation_ns,
        expiration_ns,
        precision,
        NAUTILUS_QUANTITY_PRECISION,
        price_increment,
        size_increment,
        Some(Ustr::from(outcome.name.as_str())),
        market
            .question
            .as_deref()
            .or(market.title.as_deref())
            .map(Ustr::from),
        None,
        Some(Quantity::from("0.01")),
        None,
        None,
        Some(max_price),
        Some(price_increment),
        None,
        None,
        Some(Decimal::ZERO),
        Some(fee),
        None,
        Some(info),
        ts_init,
        ts_init,
    )?;
    Ok(InstrumentAny::BinaryOption(instrument))
}

pub(crate) fn parse_timestamp(value: Option<&str>) -> Option<UnixNanos> {
    let value = value?;
    if let Ok(milliseconds) = value.parse::<u64>() {
        return milliseconds
            .checked_mul(NANOSECONDS_IN_MILLISECOND)
            .map(UnixNanos::from);
    }
    let timestamp = Timestamp::from_str(value).ok()?;
    u64::try_from(timestamp.as_nanosecond())
        .ok()
        .map(UnixNanos::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_category_expiry_does_not_expire_live_market_at_epoch() {
        let market: PredictFunMarket = serde_json::from_value(json!({
            "id": 42,
            "title": "BTC Up or Down",
            "conditionId": "condition-42",
            "decimalPrecision": 2,
            "feeRateBps": 200,
            "tradingStatus": "OPEN",
            "status": "REGISTERED",
            "outcomes": [{
                "name": "YES",
                "indexSet": 1,
                "onChainId": "123",
                "status": "OPEN"
            }]
        }))
        .unwrap();
        let instrument =
            create_instrument(&market, &market.outcomes[0], UnixNanos::from(1)).unwrap();
        let InstrumentAny::BinaryOption(binary) = instrument else {
            panic!("expected binary option");
        };
        assert_eq!(binary.expiration_ns, UnixNanos::from(u64::MAX));
        assert_eq!(binary.min_quantity, Some(Quantity::from("0.01")));
        assert_eq!(
            binary.info.as_ref().unwrap().get("minimumShares"),
            Some(&serde_json::Value::String("0.01".to_string()))
        );
    }
}
