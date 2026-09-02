use std::cmp::Reverse;

use nautilus_core::{UnixNanos, datetime::NANOSECONDS_IN_MILLISECOND};
use nautilus_model::{
    data::{BookOrder, OrderBookDelta, OrderBookDeltas, QuoteTick},
    enums::{BookAction, OrderSide, RecordFlag},
    identifiers::InstrumentId,
    types::{Price, Quantity},
};
use rust_decimal::Decimal;

use crate::{
    common::precision::{NAUTILUS_QUANTITY_PRECISION, book_quantity},
    http::models::{PredictFunBook, PredictFunLevel},
    websocket::book::validate_book,
};

pub fn parse_book_snapshots(
    book: &PredictFunBook,
    expected_market_id: u64,
    yes_instrument_id: InstrumentId,
    no_instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> anyhow::Result<(OrderBookDeltas, OrderBookDeltas)> {
    let validated = validate_book(book, expected_market_id, price_precision)?;
    let ts_event = UnixNanos::from(
        book.update_timestamp_ms
            .checked_mul(NANOSECONDS_IN_MILLISECOND)
            .ok_or_else(|| anyhow::anyhow!("PredictFun book timestamp overflow"))?,
    );
    let yes = parse_one_book(
        &validated.bids,
        &validated.asks,
        yes_instrument_id,
        price_precision,
        size_precision,
        ts_event,
        ts_init,
        book.version,
    )?;

    let mut no_bids = validated
        .asks
        .iter()
        .map(|level| complement(level, price_precision))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut no_asks = validated
        .bids
        .iter()
        .map(|level| complement(level, price_precision))
        .collect::<anyhow::Result<Vec<_>>>()?;
    no_bids.sort_by_key(|level| Reverse(level.0));
    no_asks.sort_by_key(|level| level.0);
    let no = parse_one_book(
        &no_bids,
        &no_asks,
        no_instrument_id,
        price_precision,
        size_precision,
        ts_event,
        ts_init,
        book.version,
    )?;
    Ok((yes, no))
}

pub fn quote_from_snapshot(
    book: &PredictFunBook,
    expected_market_id: u64,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<QuoteTick>> {
    let validated = validate_book(book, expected_market_id, price_precision)?;
    let (Some(best_bid), Some(best_ask)) = (validated.bids.first(), validated.asks.first()) else {
        return Ok(None);
    };
    let ts_event = UnixNanos::from(
        book.update_timestamp_ms
            .checked_mul(NANOSECONDS_IN_MILLISECOND)
            .ok_or_else(|| anyhow::anyhow!("PredictFun book timestamp overflow"))?,
    );
    Ok(Some(QuoteTick::new(
        instrument_id,
        Price::from_decimal_dp(best_bid.0, price_precision)?,
        Price::from_decimal_dp(best_ask.0, price_precision)?,
        quantity_at_precision(best_bid.1, size_precision)?,
        quantity_at_precision(best_ask.1, size_precision)?,
        ts_event,
        ts_init,
    )))
}

#[allow(clippy::too_many_arguments)]
pub fn quote_pair_from_snapshot(
    book: &PredictFunBook,
    expected_market_id: u64,
    yes_instrument_id: InstrumentId,
    no_instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> anyhow::Result<(Option<QuoteTick>, Option<QuoteTick>)> {
    let validated = validate_book(book, expected_market_id, price_precision)?;
    let (Some(best_bid), Some(best_ask)) = (validated.bids.first(), validated.asks.first()) else {
        return Ok((None, None));
    };
    let ts_event = UnixNanos::from(
        book.update_timestamp_ms
            .checked_mul(NANOSECONDS_IN_MILLISECOND)
            .ok_or_else(|| anyhow::anyhow!("PredictFun book timestamp overflow"))?,
    );
    let yes = QuoteTick::new(
        yes_instrument_id,
        Price::from_decimal_dp(best_bid.0, price_precision)?,
        Price::from_decimal_dp(best_ask.0, price_precision)?,
        quantity_at_precision(best_bid.1, size_precision)?,
        quantity_at_precision(best_ask.1, size_precision)?,
        ts_event,
        ts_init,
    );
    let no_bid = complement(best_ask, price_precision)?;
    let no_ask = complement(best_bid, price_precision)?;
    let no = QuoteTick::new(
        no_instrument_id,
        Price::from_decimal_dp(no_bid.0, price_precision)?,
        Price::from_decimal_dp(no_ask.0, price_precision)?,
        quantity_at_precision(no_bid.1, size_precision)?,
        quantity_at_precision(no_ask.1, size_precision)?,
        ts_event,
        ts_init,
    );
    Ok((Some(yes), Some(no)))
}

/// Returns the venue-native YES levels or the complementary NO levels.
///
/// PredictFun publishes one complete book per binary market. The NO book is
/// therefore derived by complementing prices and swapping sides.
pub fn outcome_book_levels(
    book: &PredictFunBook,
    expected_market_id: u64,
    price_precision: u8,
    is_yes: bool,
) -> anyhow::Result<(Vec<PredictFunLevel>, Vec<PredictFunLevel>)> {
    let validated = validate_book(book, expected_market_id, price_precision)?;
    if is_yes {
        return Ok((validated.bids, validated.asks));
    }
    let mut bids = validated
        .asks
        .iter()
        .map(|level| complement(level, price_precision))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut asks = validated
        .bids
        .iter()
        .map(|level| complement(level, price_precision))
        .collect::<anyhow::Result<Vec<_>>>()?;
    bids.sort_by_key(|level| Reverse(level.0));
    asks.sort_by_key(|level| level.0);
    Ok((bids, asks))
}

fn complement(level: &PredictFunLevel, precision: u8) -> anyhow::Result<PredictFunLevel> {
    let price = Decimal::ONE
        .checked_sub(level.0)
        .ok_or_else(|| anyhow::anyhow!("PredictFun price complement overflow"))?;
    if price.scale() > u32::from(precision) {
        anyhow::bail!(
            "PredictFun price {} exceeds declared precision {precision}",
            level.0
        );
    }
    Ok(PredictFunLevel(price, level.1))
}

#[allow(clippy::too_many_arguments)]
fn parse_one_book(
    bids: &[PredictFunLevel],
    asks: &[PredictFunLevel],
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
    sequence: u64,
) -> anyhow::Result<OrderBookDeltas> {
    let mut deltas = Vec::with_capacity(bids.len() + asks.len() + 1);
    let mut clear = OrderBookDelta::clear(instrument_id, sequence, ts_event, ts_init);
    if bids.is_empty() && asks.is_empty() {
        clear.flags |= RecordFlag::F_LAST as u8;
    }
    deltas.push(clear);
    let total = bids.len() + asks.len();
    for (index, (side, level)) in bids
        .iter()
        .map(|level| (OrderSide::Buy, level))
        .chain(asks.iter().map(|level| (OrderSide::Sell, level)))
        .enumerate()
    {
        let price = Price::from_decimal_dp(level.0, price_precision)?;
        let size = quantity_at_precision(level.1, size_precision)?;
        if size.is_zero() {
            anyhow::bail!("PredictFun full snapshot contains a zero-size level");
        }
        let mut flags = RecordFlag::F_SNAPSHOT as u8;
        if index + 1 == total {
            flags |= RecordFlag::F_LAST as u8;
        }
        deltas.push(OrderBookDelta::new_checked(
            instrument_id,
            BookAction::Add,
            BookOrder::new(side, price, size, 0),
            flags,
            sequence,
            ts_event,
            ts_init,
        )?);
    }
    Ok(OrderBookDeltas::new(instrument_id, deltas))
}

fn quantity_at_precision(value: Decimal, precision: u8) -> anyhow::Result<Quantity> {
    if precision != NAUTILUS_QUANTITY_PRECISION {
        anyhow::bail!(
            "PredictFun requires Nautilus quantity precision {NAUTILUS_QUANTITY_PRECISION}, received {precision}"
        );
    }
    Ok(book_quantity(value)?)
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::InstrumentId;
    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;

    #[rstest]
    fn produces_atomic_yes_and_complementary_no_snapshots() {
        let book = PredictFunBook {
            market_id: 42,
            version: 7,
            update_timestamp_ms: 1_700_000_000_123,
            order_count: 2,
            bids: vec![PredictFunLevel(dec!(0.40), dec!(3))],
            asks: vec![PredictFunLevel(dec!(0.61), dec!(2))],
            settlements_pending: None,
            last_order_settled: None,
        };
        let yes = InstrumentId::from("yes.PREDICTFUN");
        let no = InstrumentId::from("no.PREDICTFUN");
        let (yes_deltas, no_deltas) = parse_book_snapshots(
            &book,
            42,
            yes,
            no,
            2,
            16,
            UnixNanos::from(1_700_000_000_124_000_000),
        )
        .unwrap();

        assert_eq!(yes_deltas.deltas.len(), 3);
        assert_eq!(yes_deltas.deltas[1].sequence, 7);
        assert_eq!(no_deltas.deltas.len(), 3);
        assert_eq!(no_deltas.deltas[1].order.price, Price::from("0.39"));
        assert_eq!(no_deltas.deltas[2].order.price, Price::from("0.60"));
        assert_eq!(
            no_deltas.deltas[2].flags,
            RecordFlag::F_SNAPSHOT as u8 | RecordFlag::F_LAST as u8
        );
    }

    #[rstest]
    fn empty_snapshot_is_clear_and_last() {
        let book = PredictFunBook {
            market_id: 42,
            version: 8,
            update_timestamp_ms: 1,
            order_count: 0,
            bids: vec![],
            asks: vec![],
            settlements_pending: None,
            last_order_settled: None,
        };
        let (yes, _) = parse_book_snapshots(
            &book,
            42,
            InstrumentId::from("yes.PREDICTFUN"),
            InstrumentId::from("no.PREDICTFUN"),
            2,
            16,
            UnixNanos::from(2_000_000),
        )
        .unwrap();
        assert_eq!(yes.deltas.len(), 1);
        assert_ne!(yes.deltas[0].flags & RecordFlag::F_LAST as u8, 0);
    }

    #[test]
    fn derives_no_levels_for_market_execution() {
        let book = PredictFunBook {
            market_id: 42,
            version: 1,
            update_timestamp_ms: 1,
            order_count: 2,
            bids: vec![PredictFunLevel(dec!(0.40), dec!(3))],
            asks: vec![PredictFunLevel(dec!(0.61), dec!(2))],
            settlements_pending: None,
            last_order_settled: None,
        };
        let (bids, asks) = outcome_book_levels(&book, 42, 2, false).unwrap();
        assert_eq!(bids, vec![PredictFunLevel(dec!(0.39), dec!(2))]);
        assert_eq!(asks, vec![PredictFunLevel(dec!(0.60), dec!(3))]);
    }
}
