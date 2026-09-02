use alloy_primitives::U256;
use rust_decimal::Decimal;

use crate::{
    common::{enums::PredictFunSide, parse::decimal_to_wei},
    http::models::PredictFunLevel,
};

const SIGNED_DECIMALS: usize = 18;
/// Venue-native minimum order quantity: 0.01 outcome shares (18-decimal base units).
pub const MIN_QUANTITY_WEI: u64 = 10_000_000_000_000_000;
const BPS_DENOMINATOR: u64 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictFunOrderAmounts {
    pub price_per_share: U256,
    pub maker_amount: U256,
    pub taker_amount: U256,
    pub amount: U256,
    pub last_price: U256,
    pub slippage_bps: u32,
    pub is_min_amount_out: bool,
}

pub fn limit_order_amounts(
    side: PredictFunSide,
    price: Decimal,
    quantity: Decimal,
) -> anyhow::Result<PredictFunOrderAmounts> {
    let price = retain_significant_digits(decimal_to_wei(price)?, 3)?;
    let quantity = retain_significant_digits(decimal_to_wei(quantity)?, 5)?;
    validate_price_and_quantity(price, quantity)?;
    let precision = ten_pow(SIGNED_DECIMALS)?;
    let notional = checked_mul(price, quantity)? / precision;
    let (maker_amount, taker_amount) = match side {
        PredictFunSide::Buy => (notional, quantity),
        PredictFunSide::Sell => (quantity, notional),
    };
    Ok(PredictFunOrderAmounts {
        price_per_share: price,
        maker_amount,
        taker_amount,
        amount: quantity,
        last_price: price,
        slippage_bps: 0,
        is_min_amount_out: false,
    })
}

pub fn market_order_amounts_by_quantity(
    side: PredictFunSide,
    quantity: Decimal,
    bids: &[PredictFunLevel],
    asks: &[PredictFunLevel],
    slippage_bps: u32,
    is_min_amount_out: bool,
) -> anyhow::Result<PredictFunOrderAmounts> {
    if slippage_bps > BPS_DENOMINATOR as u32 {
        anyhow::bail!("slippage cannot exceed 10000 bps");
    }
    if side == PredictFunSide::Sell && is_min_amount_out {
        anyhow::bail!("is_min_amount_out is only valid for market buys");
    }
    let requested = retain_significant_digits(decimal_to_wei(quantity)?, 5)?;
    if requested < U256::from(MIN_QUANTITY_WEI) {
        anyhow::bail!("PredictFun quantity is below the 0.01-share minimum");
    }
    let depths = match side {
        PredictFunSide::Buy => asks,
        PredictFunSide::Sell => bids,
    };
    let processed = process_book(depths, requested)?;
    if processed.quantity != requested {
        anyhow::bail!(
            "insufficient PredictFun book depth: requested {requested} wei, available {} wei",
            processed.quantity
        );
    }
    let precision = ten_pow(SIGNED_DECIMALS)?;
    let bps = U256::from(BPS_DENOMINATOR);
    let slippage = U256::from(slippage_bps);
    let average = processed.weighted_price / processed.quantity;

    match side {
        PredictFunSide::Buy if is_min_amount_out => {
            let maker_amount = processed.weighted_price / precision;
            let signed_shares = processed.weighted_price / processed.last_price;
            let taker_amount = checked_mul(signed_shares, bps - slippage)? / bps;
            Ok(PredictFunOrderAmounts {
                price_per_share: average,
                maker_amount,
                taker_amount,
                amount: processed.quantity,
                last_price: processed.last_price,
                slippage_bps,
                is_min_amount_out: true,
            })
        }
        PredictFunSide::Buy => {
            let base = checked_mul(processed.last_price, processed.quantity)? / precision;
            let maker_amount = if slippage_bps == 0 {
                base
            } else {
                (checked_mul(base, bps + slippage)? / bps).min(processed.quantity)
            };
            Ok(PredictFunOrderAmounts {
                price_per_share: average,
                maker_amount,
                taker_amount: processed.quantity,
                amount: processed.quantity,
                last_price: processed.last_price,
                slippage_bps,
                is_min_amount_out: false,
            })
        }
        PredictFunSide::Sell => {
            let base = checked_mul(processed.last_price, processed.quantity)? / precision;
            let taker_amount = checked_mul(base, bps - slippage)? / bps;
            Ok(PredictFunOrderAmounts {
                price_per_share: average,
                maker_amount: processed.quantity,
                taker_amount,
                amount: processed.quantity,
                last_price: processed.last_price,
                slippage_bps,
                is_min_amount_out: false,
            })
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ProcessedBook {
    quantity: U256,
    weighted_price: U256,
    last_price: U256,
}

fn process_book(depths: &[PredictFunLevel], requested: U256) -> anyhow::Result<ProcessedBook> {
    let mut result = ProcessedBook {
        quantity: U256::ZERO,
        weighted_price: U256::ZERO,
        last_price: U256::ZERO,
    };
    for level in depths {
        if result.quantity >= requested {
            break;
        }
        let price = decimal_to_wei(level.0)?;
        let level_quantity = decimal_to_wei(level.1)?;
        validate_price(price)?;
        if level_quantity.is_zero() {
            continue;
        }
        let remaining = requested - result.quantity;
        let take = remaining.min(level_quantity);
        result.quantity = result
            .quantity
            .checked_add(take)
            .ok_or_else(|| anyhow::anyhow!("PredictFun market quantity overflow"))?;
        result.weighted_price = result
            .weighted_price
            .checked_add(checked_mul(price, take)?)
            .ok_or_else(|| anyhow::anyhow!("PredictFun weighted price overflow"))?;
        result.last_price = price;
    }
    Ok(result)
}

fn validate_price_and_quantity(price: U256, quantity: U256) -> anyhow::Result<()> {
    validate_price(price)?;
    if quantity < U256::from(MIN_QUANTITY_WEI) {
        anyhow::bail!("PredictFun quantity is below the 0.01-share minimum");
    }
    Ok(())
}

fn validate_price(price: U256) -> anyhow::Result<()> {
    if price.is_zero() || price >= ten_pow(SIGNED_DECIMALS)? {
        anyhow::bail!("PredictFun price must be strictly between zero and one");
    }
    Ok(())
}

fn retain_significant_digits(value: U256, digits: usize) -> anyhow::Result<U256> {
    if value.is_zero() {
        return Ok(value);
    }
    let length = value.to_string().len();
    if length <= digits {
        return Ok(value);
    }
    let divisor = ten_pow(length - digits)?;
    Ok((value / divisor) * divisor)
}

fn ten_pow(exponent: usize) -> anyhow::Result<U256> {
    let exponent = u32::try_from(exponent)?;
    U256::from(10u8)
        .checked_pow(U256::from(exponent))
        .ok_or_else(|| anyhow::anyhow!("power-of-ten overflow for exponent {exponent}"))
}

fn checked_mul(left: U256, right: U256) -> anyhow::Result<U256> {
    left.checked_mul(right)
        .ok_or_else(|| anyhow::anyhow!("PredictFun amount multiplication overflow"))
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn limit_amounts_match_official_sdk_rules() {
        let amounts =
            limit_order_amounts(PredictFunSide::Buy, dec!(0.412345), dec!(10.123456)).unwrap();
        assert_eq!(amounts.price_per_share.to_string(), "412000000000000000");
        assert_eq!(amounts.amount.to_string(), "10123000000000000000");
        assert_eq!(amounts.maker_amount.to_string(), "4170676000000000000");
        assert_eq!(amounts.taker_amount, amounts.amount);
    }

    #[test]
    fn market_buy_uses_worst_tier_for_slippage_ceiling() {
        let asks = vec![
            PredictFunLevel(dec!(0.40), dec!(2)),
            PredictFunLevel(dec!(0.50), dec!(3)),
        ];
        let amounts =
            market_order_amounts_by_quantity(PredictFunSide::Buy, dec!(4), &[], &asks, 100, false)
                .unwrap();
        assert_eq!(amounts.price_per_share.to_string(), "450000000000000000");
        assert_eq!(amounts.last_price.to_string(), "500000000000000000");
        assert_eq!(amounts.maker_amount.to_string(), "2020000000000000000");
        assert_eq!(amounts.taker_amount.to_string(), "4000000000000000000");
    }

    #[test]
    fn market_order_rejects_insufficient_depth() {
        let asks = vec![PredictFunLevel(dec!(0.40), dec!(1))];
        let error =
            market_order_amounts_by_quantity(PredictFunSide::Buy, dec!(2), &[], &asks, 0, false)
                .unwrap_err();
        assert!(error.to_string().contains("insufficient"));
    }
}
