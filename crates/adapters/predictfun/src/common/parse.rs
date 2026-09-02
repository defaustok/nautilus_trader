use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, de::Error as _};

pub fn deserialize_decimal_from_string_or_number<'de, D>(
    deserializer: D,
) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(value) => Decimal::from_str(&value).map_err(D::Error::custom),
        serde_json::Value::Number(value) => {
            Decimal::from_str(&value.to_string()).map_err(D::Error::custom)
        }
        other => Err(D::Error::custom(format!(
            "expected decimal string or JSON number, received {other}"
        ))),
    }
}

pub fn deserialize_string_from_string_or_number<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::String(value) => Ok(value),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        other => Err(D::Error::custom(format!(
            "expected string or JSON number, received {other}"
        ))),
    }
}

pub fn decimal_to_wei(value: Decimal) -> anyhow::Result<alloy_primitives::U256> {
    if value.is_sign_negative() || value.scale() > 18 {
        anyhow::bail!("value {value} cannot be represented as unsigned 18-decimal wei");
    }
    let mantissa = u128::try_from(value.mantissa())?;
    let exponent = 18u32 - value.scale();
    let factor = alloy_primitives::U256::from(10u8)
        .checked_pow(alloy_primitives::U256::from(exponent))
        .ok_or_else(|| anyhow::anyhow!("wei scale overflow for {value}"))?;
    alloy_primitives::U256::from(mantissa)
        .checked_mul(factor)
        .ok_or_else(|| anyhow::anyhow!("wei conversion overflow for {value}"))
}

pub fn wei_to_decimal(value: alloy_primitives::U256) -> anyhow::Result<Decimal> {
    let mantissa = u128::try_from(value)
        .map_err(|_| anyhow::anyhow!("18-decimal wei value exceeds Decimal range"))?;
    let mantissa = i128::try_from(mantissa)
        .map_err(|_| anyhow::anyhow!("18-decimal wei value exceeds signed Decimal range"))?;
    Ok(Decimal::from_i128_with_scale(mantissa, 18))
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Deserialize)]
    struct NumericWire {
        #[serde(deserialize_with = "deserialize_string_from_string_or_number")]
        value: String,
    }

    #[test]
    fn preserves_numeric_response_as_string() {
        let wire: NumericWire = serde_json::from_str(r#"{"value":4102444800}"#).unwrap();
        assert_eq!(wire.value, "4102444800");
    }
}
