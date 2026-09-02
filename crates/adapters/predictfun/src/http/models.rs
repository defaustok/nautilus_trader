use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::common::{
    enums::{PredictFunQuoteType, PredictFunSide, PredictFunSignatureType, PredictFunStrategy},
    parse::{deserialize_decimal_from_string_or_number, deserialize_string_from_string_or_number},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunOutcome {
    pub name: String,
    pub index_set: u64,
    pub on_chain_id: String,
    pub status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunMarket {
    pub id: u64,
    pub title: Option<String>,
    pub question: Option<String>,
    pub condition_id: String,
    pub decimal_precision: u8,
    pub fee_rate_bps: u32,
    #[serde(default)]
    pub is_neg_risk: bool,
    #[serde(default)]
    pub is_yield_bearing: bool,
    pub trading_status: String,
    pub status: String,
    #[serde(default)]
    pub outcomes: Vec<PredictFunOutcome>,
    pub starts_at: Option<String>,
    pub ends_at: Option<String>,
    pub category_slug: Option<String>,
    pub market_variant: Option<String>,
    pub variant_data: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunCategory {
    pub id: u64,
    pub title: Option<String>,
    pub short_title: Option<String>,
    pub starts_at: Option<String>,
    pub ends_at: Option<String>,
    pub status: String,
    pub resolution_provider: Option<String>,
    pub variant_data: Option<serde_json::Value>,
    #[serde(default)]
    pub markets: Vec<PredictFunMarket>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunBook {
    pub market_id: u64,
    #[serde(default)]
    pub version: u64,
    #[serde(default)]
    pub update_timestamp_ms: u64,
    #[serde(default)]
    pub order_count: u64,
    #[serde(default)]
    pub bids: Vec<PredictFunLevel>,
    #[serde(default)]
    pub asks: Vec<PredictFunLevel>,
    pub settlements_pending: Option<serde_json::Value>,
    pub last_order_settled: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunApiResponse<T> {
    pub success: bool,
    pub data: T,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PredictFunAuthMessage {
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PredictFunAuthRequest {
    pub signer: String,
    pub signature: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PredictFunAuthToken {
    pub token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PredictFunCreateOrderRequest {
    pub data: PredictFunCreateOrderData,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PredictFunRemoveOrdersData {
    pub ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PredictFunRemoveOrdersRequest {
    pub data: PredictFunRemoveOrdersData,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PredictFunRemoveOrdersResponse {
    #[serde(default)]
    pub removed: Vec<String>,
    #[serde(default)]
    pub noop: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PredictFunLevel(
    #[serde(deserialize_with = "deserialize_decimal_from_string_or_number")] pub Decimal,
    #[serde(deserialize_with = "deserialize_decimal_from_string_or_number")] pub Decimal,
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunContractOrder {
    #[serde(deserialize_with = "deserialize_string_from_string_or_number")]
    pub salt: String,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    #[serde(deserialize_with = "deserialize_string_from_string_or_number")]
    pub token_id: String,
    #[serde(deserialize_with = "deserialize_string_from_string_or_number")]
    pub maker_amount: String,
    #[serde(deserialize_with = "deserialize_string_from_string_or_number")]
    pub taker_amount: String,
    #[serde(deserialize_with = "deserialize_string_from_string_or_number")]
    pub expiration: String,
    #[serde(deserialize_with = "deserialize_string_from_string_or_number")]
    pub nonce: String,
    #[serde(deserialize_with = "deserialize_string_from_string_or_number")]
    pub fee_rate_bps: String,
    pub side: PredictFunSide,
    pub signature_type: PredictFunSignatureType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunCreateOrderData {
    /// Integer 18-decimal price, encoded as a JSON string as required by the API.
    pub price_per_share: String,
    pub strategy: PredictFunStrategy,
    pub order: PredictFunContractOrder,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slippage_bps: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_fill_or_kill: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_post_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_trade_prevention: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_min_amount_out: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunCreateOrderResponse {
    pub code: String,
    pub order_id: String,
    pub order_hash: String,
    pub removal_locked_until: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunOrderRecord {
    pub id: String,
    pub order_hash: Option<String>,
    pub market_id: u64,
    pub status: String,
    pub amount: String,
    pub amount_filled: String,
    #[serde(default)]
    pub is_neg_risk: bool,
    #[serde(default)]
    pub is_yield_bearing: bool,
    pub strategy: PredictFunStrategy,
    pub order: PredictFunContractOrder,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunFee {
    pub amount_wei: String,
    #[serde(rename = "type")]
    pub asset_type: PredictFunFeeAsset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PredictFunFeeAsset {
    Collateral,
    Shares,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunFill {
    pub executed_price_wei: String,
    pub executed_size_wei: String,
    pub executed_value_wei: String,
    pub fee: Option<PredictFunFee>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunPosition {
    pub id: String,
    pub market: PredictFunPositionMarket,
    pub outcome: PredictFunPositionOutcome,
    pub amount: String,
    pub value_usd: String,
    pub average_buy_price_usd: String,
    pub pnl_usd: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredictFunPositionMarket {
    pub id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunPositionOutcome {
    pub name: String,
    pub index_set: u64,
    pub on_chain_id: String,
}

/// A single maker or taker leg from `GET /orders/matches`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunMatchOrder {
    pub quote_type: PredictFunQuoteType,
    pub amount: String,
    pub price: String,
    pub outcome: PredictFunOutcome,
    pub signer: String,
    pub hash: String,
    pub fee: Option<PredictFunMatchFee>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredictFunMatchFee {
    pub amount: String,
    #[serde(rename = "type")]
    pub asset_type: PredictFunFeeAsset,
}

/// Documented order match event. Large market payload fields are intentionally ignored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunMatch {
    pub market: PredictFunMatchMarket,
    pub taker: PredictFunMatchOrder,
    pub amount_filled: String,
    pub price_executed: String,
    #[serde(default)]
    pub makers: Vec<PredictFunMatchOrder>,
    pub transaction_hash: String,
    /// Absent while a match has not yet received its on-chain settlement ID.
    pub settlement_id: Option<String>,
    pub executed_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredictFunMatchMarket {
    pub id: u64,
}

/// Authenticated account activity from `GET /account/activity`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunAccountActivity {
    pub name: String,
    pub created_at: String,
    #[serde(default)]
    pub order_id: Option<String>,
    #[serde(default)]
    pub order_hash: Option<String>,
    pub transaction_hash: Option<String>,
    pub amount_filled: Option<String>,
    pub price_executed: Option<String>,
    pub order: Option<PredictFunActivityOrder>,
    pub market: Option<PredictFunMatchMarket>,
    pub outcome: Option<PredictFunOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunActivityOrder {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub hash: Option<String>,
    pub quote_type: PredictFunQuoteType,
    pub amount: String,
    pub price: String,
    pub fee: Option<PredictFunMatchFee>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunWalletDetails {
    pub market_id: u64,
    pub outcome_index: u64,
    pub outcome: String,
    pub quote_type: PredictFunQuoteType,
    pub quantity: String,
    pub quantity_filled: String,
    pub price: String,
    pub value: String,
    pub value_filled: String,
    pub strategy_type: PredictFunStrategy,
    pub settlement_id: Option<String>,
    pub fill: Option<PredictFunFill>,
    pub is_maker: Option<bool>,
    pub reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_official_http_orderbook_without_optional_version() {
        let payload = include_str!("../../test_data/http/orderbook.json");
        let response: PredictFunApiResponse<PredictFunBook> =
            serde_json::from_str(payload).unwrap();
        assert!(response.success);
        assert_eq!(response.data.market_id, 1);
        assert_eq!(response.data.version, 0);
        assert_eq!(response.data.bids.len(), 2);
    }

    #[test]
    fn parses_documented_fee_assets_without_fallback() {
        let collateral: PredictFunFee =
            serde_json::from_str(r#"{"amountWei":"1","type":"COLLATERAL"}"#).unwrap();
        assert_eq!(collateral.asset_type, PredictFunFeeAsset::Collateral);
        assert!(
            serde_json::from_str::<PredictFunFee>(r#"{"amountWei":"1","type":"UNKNOWN"}"#).is_err()
        );
    }

    #[test]
    fn parses_documented_match_fixture() {
        let payload = include_str!("../../test_data/http/matches.json");
        let response: PredictFunApiResponse<Vec<PredictFunMatch>> =
            serde_json::from_str(payload).unwrap();
        assert!(response.success);
        assert_eq!(
            response.data[0].settlement_id.as_deref(),
            Some("settlement-1")
        );
        assert_eq!(response.data[0].makers[0].hash, "0xmaker");
    }

    #[test]
    fn parses_unsettled_match() {
        let payload = include_str!("../../test_data/http/matches.json").replace(
            r#""settlementId": "settlement-1""#,
            r#""settlementId": null"#,
        );
        let response: PredictFunApiResponse<Vec<PredictFunMatch>> =
            serde_json::from_str(&payload).unwrap();
        assert!(response.data[0].settlement_id.is_none());
    }

    #[test]
    fn parses_documented_account_activity_fixture() {
        let payload = include_str!("../../test_data/http/account_activity.json");
        let response: PredictFunApiResponse<Vec<PredictFunAccountActivity>> =
            serde_json::from_str(payload).unwrap();
        assert!(response.success);
        assert_eq!(response.data[0].name, "MATCH");
        assert_eq!(response.data[0].market.as_ref().unwrap().id, 42);
    }
}
