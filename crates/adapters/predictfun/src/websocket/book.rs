use std::{cmp::Reverse, collections::HashSet};

use rust_decimal::Decimal;

use crate::{
    common::error::PredictFunError,
    http::models::{PredictFunBook, PredictFunLevel},
};

#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedBook {
    pub bids: Vec<PredictFunLevel>,
    pub asks: Vec<PredictFunLevel>,
}

pub fn validate_book(
    book: &PredictFunBook,
    expected_market_id: u64,
    price_precision: u8,
) -> Result<ValidatedBook, PredictFunError> {
    if book.market_id != expected_market_id {
        return Err(PredictFunError::MarketMismatch {
            expected: expected_market_id,
            actual: book.market_id,
        });
    }
    let mut bids = validate_side(&book.bids, "bid", price_precision)?;
    let mut asks = validate_side(&book.asks, "ask", price_precision)?;
    bids.sort_by_key(|level| Reverse(level.0));
    asks.sort_by_key(|level| level.0);
    if let (Some(best_bid), Some(best_ask)) = (bids.first(), asks.first())
        && best_bid.0 >= best_ask.0
    {
        return Err(PredictFunError::CrossedBook {
            best_bid: best_bid.0.to_string(),
            best_ask: best_ask.0.to_string(),
        });
    }
    Ok(ValidatedBook { bids, asks })
}

fn validate_side(
    levels: &[PredictFunLevel],
    side: &'static str,
    price_precision: u8,
) -> Result<Vec<PredictFunLevel>, PredictFunError> {
    let mut prices = HashSet::with_capacity(levels.len());
    for level in levels {
        if level.0 <= Decimal::ZERO || level.0 >= Decimal::ONE {
            return Err(PredictFunError::InvalidPrice {
                side,
                price: level.0.to_string(),
            });
        }
        if level.0.normalize().scale() > u32::from(price_precision) {
            return Err(PredictFunError::InvalidPricePrecision {
                side,
                price: level.0.to_string(),
                precision: price_precision,
            });
        }
        if level.1 <= Decimal::ZERO {
            return Err(PredictFunError::InvalidSize {
                side,
                price: level.0.to_string(),
                size: level.1.to_string(),
            });
        }
        if !prices.insert(level.0.normalize()) {
            return Err(PredictFunError::DuplicatePrice {
                side,
                price: level.0.to_string(),
            });
        }
    }
    Ok(levels.to_vec())
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BookVersionState {
    last_version: Option<u64>,
}

impl BookVersionState {
    pub fn accept(&mut self, version: u64) -> Result<(), PredictFunError> {
        if let Some(previous) = self.last_version {
            if version == previous {
                return Err(PredictFunError::DuplicateBookVersion { actual: version });
            }
            if version < previous {
                return Err(PredictFunError::RegressedBookVersion {
                    previous,
                    actual: version,
                });
            }
            if version != previous + 1 {
                return Err(PredictFunError::BookVersionGap {
                    previous,
                    actual: version,
                });
            }
        }
        self.last_version = Some(version);
        Ok(())
    }

    pub fn reset(&mut self) {
        self.last_version = None;
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    fn book() -> PredictFunBook {
        PredictFunBook {
            market_id: 42,
            version: 7,
            update_timestamp_ms: 1,
            order_count: 2,
            bids: vec![PredictFunLevel(dec!(0.40), dec!(3))],
            asks: vec![PredictFunLevel(dec!(0.61), dec!(2))],
            settlements_pending: None,
            last_order_settled: None,
        }
    }

    #[test]
    fn rejects_wrong_market_and_crossed_books() {
        assert!(matches!(
            validate_book(&book(), 41, 2),
            Err(PredictFunError::MarketMismatch { .. })
        ));
        let mut crossed = book();
        crossed.bids[0].0 = dec!(0.61);
        assert!(matches!(
            validate_book(&crossed, 42, 2),
            Err(PredictFunError::CrossedBook { .. })
        ));
    }

    #[test]
    fn detects_version_discontinuities() {
        let mut state = BookVersionState::default();
        state.accept(10).unwrap();
        state.accept(11).unwrap();
        assert!(matches!(
            state.accept(13),
            Err(PredictFunError::BookVersionGap { .. })
        ));
        state.reset();
        state.accept(13).unwrap();
    }
}
