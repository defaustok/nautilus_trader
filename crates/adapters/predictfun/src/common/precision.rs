use alloy_primitives::U256;
use nautilus_model::types::{Quantity, quantity::QuantityRaw};
use rust_decimal::Decimal;

use super::error::PredictFunError;

pub const PREDICTFUN_WEI_PRECISION: u8 = 18;
pub const NAUTILUS_QUANTITY_PRECISION: u8 = 16;
const WEI_PER_NAUTILUS_RAW: u64 = 100;

pub fn decimal_to_wei(value: Decimal) -> Result<U256, PredictFunError> {
    if value.is_sign_negative() || value.scale() > u32::from(PREDICTFUN_WEI_PRECISION) {
        return Err(PredictFunError::InvalidWei {
            value: value.to_string(),
        });
    }
    let mantissa = u128::try_from(value.mantissa()).map_err(|_| PredictFunError::InvalidWei {
        value: value.to_string(),
    })?;
    let factor = U256::from(10u8)
        .checked_pow(U256::from(PREDICTFUN_WEI_PRECISION - value.scale() as u8))
        .ok_or_else(|| PredictFunError::InvalidWei {
            value: value.to_string(),
        })?;
    U256::from(mantissa)
        .checked_mul(factor)
        .ok_or_else(|| PredictFunError::InvalidWei {
            value: value.to_string(),
        })
}

pub fn book_quantity(value: Decimal) -> Result<Quantity, PredictFunError> {
    let wei = decimal_to_wei(value)?;
    let raw = wei / U256::from(WEI_PER_NAUTILUS_RAW);
    if raw.is_zero() && !wei.is_zero() {
        return Err(PredictFunError::BelowNautilusResolution {
            value: value.to_string(),
        });
    }
    quantity_from_raw(raw, value.to_string())
}

fn quantity_from_raw(raw: U256, source: String) -> Result<Quantity, PredictFunError> {
    let raw = QuantityRaw::try_from(raw).map_err(|_| PredictFunError::QuantityOverflow {
        value: source.clone(),
    })?;
    Quantity::from_raw_checked(raw, NAUTILUS_QUANTITY_PRECISION)
        .map_err(|_| PredictFunError::QuantityOverflow { value: source })
}

/// Projects cumulative 18-decimal venue quantities into Nautilus' 16-decimal
/// fixed-point domain. Computing the difference between cumulative projections
/// preserves every representable increment without rounding each fill separately.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CumulativeQuantityProjector {
    venue_cumulative_wei: U256,
    nautilus_cumulative_raw: U256,
}

impl CumulativeQuantityProjector {
    pub fn project_cumulative(
        &mut self,
        cumulative_wei: U256,
    ) -> Result<Quantity, PredictFunError> {
        if cumulative_wei < self.venue_cumulative_wei {
            return Err(PredictFunError::CumulativeQuantityRegression {
                previous: self.venue_cumulative_wei.to_string(),
                actual: cumulative_wei.to_string(),
            });
        }
        let projected = cumulative_wei / U256::from(WEI_PER_NAUTILUS_RAW);
        let delta = projected - self.nautilus_cumulative_raw;
        self.venue_cumulative_wei = cumulative_wei;
        self.nautilus_cumulative_raw = projected;
        quantity_from_raw(delta, cumulative_wei.to_string())
    }

    pub fn venue_cumulative_wei(&self) -> U256 {
        self.venue_cumulative_wei
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn book_quantity_floors_without_overstating_depth() {
        let quantity = book_quantity(dec!(1.000000000000000199)).unwrap();
        assert_eq!(quantity.raw, 10_000_000_000_000_001);
        assert_eq!(quantity.to_string(), "1.0000000000000001");
    }

    #[test]
    fn cumulative_projection_conserves_representable_increments() {
        let mut projector = CumulativeQuantityProjector::default();
        let first = projector.project_cumulative(U256::from(99u8)).unwrap();
        let second = projector.project_cumulative(U256::from(101u8)).unwrap();
        assert!(first.is_zero());
        assert_eq!(second.raw, 1);
        assert_eq!(projector.venue_cumulative_wei(), U256::from(101u8));
    }

    #[test]
    fn rejects_sub_resolution_book_depth() {
        assert!(matches!(
            book_quantity(dec!(0.000000000000000001)),
            Err(PredictFunError::BelowNautilusResolution { .. })
        ));
    }
}
