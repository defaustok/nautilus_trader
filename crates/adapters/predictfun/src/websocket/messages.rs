use serde::{Deserialize, Serialize};

use crate::http::models::{PredictFunBook, PredictFunWalletDetails};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunWsRequest<T> {
    pub method: &'static str,
    pub request_id: u64,
    pub params: Vec<T>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(super) struct PredictFunHeartbeatRequest {
    method: &'static str,
    data: u64,
}

impl PredictFunHeartbeatRequest {
    pub(super) const fn new(data: u64) -> Self {
        Self {
            method: "heartbeat",
            data,
        }
    }
}

impl<T> PredictFunWsRequest<T> {
    pub fn subscribe(request_id: u64, params: Vec<T>) -> Self {
        Self {
            method: "subscribe",
            request_id,
            params,
        }
    }

    pub fn unsubscribe(request_id: u64, params: Vec<T>) -> Self {
        Self {
            method: "unsubscribe",
            request_id,
            params,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunWsEnvelope {
    #[serde(rename = "type")]
    pub message_type: String,
    pub topic: Option<String>,
    pub request_id: Option<u64>,
    pub success: Option<bool>,
    pub timestamp: Option<u64>,
    pub data: Option<serde_json::Value>,
    pub error: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunWalletEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub order_id: String,
    pub order_hash: String,
    pub wallet_address: String,
    pub timestamp: u64,
    pub details: PredictFunWalletDetails,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunTradingStatusEvent {
    pub kind: String,
    pub ts_ms: u64,
    pub market_id: u64,
    pub trading_status: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictFunMarketStatusEvent {
    pub kind: String,
    pub ts_ms: u64,
    pub market_id: u64,
    pub status: String,
    #[serde(default)]
    pub market_outcomes: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PredictFunWsMessage {
    Response(PredictFunWsEnvelope),
    Heartbeat(u64),
    OrderBook { book: PredictFunBook },
    Wallet { event: Box<PredictFunWalletEvent> },
    TradingStatus(PredictFunTradingStatusEvent),
    MarketStatus(PredictFunMarketStatusEvent),
    Other(PredictFunWsEnvelope),
}

impl PredictFunWsMessage {
    pub fn parse(payload: &str) -> anyhow::Result<Self> {
        let envelope: PredictFunWsEnvelope = serde_json::from_str(payload)?;
        if envelope.message_type == "R" {
            return Ok(Self::Response(envelope));
        }
        if envelope.message_type == "M" && envelope.topic.as_deref() == Some("heartbeat") {
            let timestamp = envelope
                .data
                .as_ref()
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| anyhow::anyhow!("PredictFun heartbeat data is not a timestamp"))?;
            return Ok(Self::Heartbeat(timestamp));
        }
        if let Some(timestamp) = envelope.timestamp
            && envelope.topic.is_none()
            && envelope.data.is_none()
        {
            return Ok(Self::Heartbeat(timestamp));
        }
        let Some(topic) = envelope.topic.clone() else {
            return Ok(Self::Other(envelope));
        };
        let Some(data) = envelope.data.clone() else {
            return Ok(Self::Other(envelope));
        };
        if topic.starts_with("predictOrderbook/") {
            let topic_market_id = topic_parameter(&topic, "predictOrderbook/")?;
            let book: PredictFunBook = serde_json::from_value(data)?;
            if book.market_id != topic_market_id {
                anyhow::bail!(
                    "PredictFun orderbook topic market {topic_market_id} does not match payload market {}",
                    book.market_id
                );
            }
            return Ok(Self::OrderBook { book });
        }
        if topic.starts_with("predictWalletEvents/") {
            return Ok(Self::Wallet {
                event: Box::new(serde_json::from_value(data)?),
            });
        }
        if topic.starts_with("predictTradingStatus/") {
            let topic_market_id = topic_parameter(&topic, "predictTradingStatus/")?;
            let event: PredictFunTradingStatusEvent = serde_json::from_value(data)?;
            ensure_topic_market(topic_market_id, event.market_id, &topic)?;
            return Ok(Self::TradingStatus(event));
        }
        if topic.starts_with("predictMarketStatus/") {
            let topic_market_id = topic_parameter(&topic, "predictMarketStatus/")?;
            let event: PredictFunMarketStatusEvent = serde_json::from_value(data)?;
            ensure_topic_market(topic_market_id, event.market_id, &topic)?;
            return Ok(Self::MarketStatus(event));
        }
        Ok(Self::Other(envelope))
    }
}

fn topic_parameter(topic: &str, prefix: &str) -> anyhow::Result<u64> {
    let value = topic
        .strip_prefix(prefix)
        .ok_or_else(|| anyhow::anyhow!("invalid PredictFun topic"))?;
    if value.is_empty() || value.contains('/') {
        anyhow::bail!("invalid PredictFun topic parameter");
    }
    Ok(value.parse()?)
}

fn ensure_topic_market(
    topic_market_id: u64,
    payload_market_id: u64,
    topic: &str,
) -> anyhow::Result<()> {
    if topic_market_id != payload_market_id {
        anyhow::bail!("PredictFun topic {topic} does not match payload market {payload_market_id}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_cross_market_orderbook_payload() {
        let payload = r#"{"type":"M","topic":"predictOrderbook/41","data":{"marketId":42,"version":7,"updateTimestampMs":1,"bids":[],"asks":[]}}"#;
        assert!(PredictFunWsMessage::parse(payload).is_err());
    }

    #[test]
    fn parses_official_server_heartbeat_and_serializes_exact_response() {
        let payload = r#"{"type":"M","topic":"heartbeat","data":1736696400000}"#;

        assert_eq!(
            PredictFunWsMessage::parse(payload).unwrap(),
            PredictFunWsMessage::Heartbeat(1_736_696_400_000),
        );
        assert_eq!(
            serde_json::to_string(&PredictFunHeartbeatRequest::new(1_736_696_400_000)).unwrap(),
            r#"{"method":"heartbeat","data":1736696400000}"#,
        );
    }

    #[test]
    fn wallet_message_does_not_retain_secret_topic() {
        let payload = r#"{"type":"M","topic":"predictWalletEvents/secret-jwt","data":{"type":"orderAccepted","orderId":"1","orderHash":"0x1","walletAddress":"0x0000000000000000000000000000000000000000","timestamp":1,"details":{"marketId":1,"outcomeIndex":0,"outcome":"YES","quoteType":"BID","quantity":"1","quantityFilled":"0","price":"0.5","value":"0.5","valueFilled":"0","strategyType":"LIMIT"}}}"#;
        let message = PredictFunWsMessage::parse(payload).unwrap();
        assert!(!format!("{message:?}").contains("secret-jwt"));
    }

    #[test]
    fn parses_official_orderbook_fixture() {
        let payload = include_str!("../../test_data/ws/orderbook.json");
        let message = PredictFunWsMessage::parse(payload).unwrap();
        let PredictFunWsMessage::OrderBook { book } = message else {
            panic!("expected orderbook fixture");
        };
        assert_eq!(book.market_id, 123);
        assert_eq!(book.version, 1);
        assert_eq!(book.asks.len(), 2);
        assert!(book.settlements_pending.is_some());
    }
}
