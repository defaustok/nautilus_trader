use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use nautilus_common::live::get_runtime;
use nautilus_network::{
    SocketStateSink,
    ratelimiter::RateLimiter,
    websocket::{
        TransportBackend, WebSocketClient, WebSocketConfig, channel_epoch_message_handler,
    },
};
use tokio::task::JoinHandle;

use super::handler::{FeedHandler, HandlerCommand, PredictFunWsEvent};
use crate::config::SecretString;

#[derive(Clone, Debug)]
pub struct PredictFunWebSocketSubscriptionHandle {
    cmd_tx: tokio::sync::mpsc::UnboundedSender<HandlerCommand>,
}

impl PredictFunWebSocketSubscriptionHandle {
    pub fn subscribe(&self, topic: String) -> anyhow::Result<()> {
        validate_topic(&topic)?;
        self.cmd_tx
            .send(HandlerCommand::Subscribe(topic))
            .map_err(|error| anyhow::anyhow!("PredictFun WebSocket handler stopped: {error}"))
    }

    pub async fn subscribe_confirmed(&self, topic: String) -> anyhow::Result<()> {
        validate_topic(&topic)?;
        let (confirmation_tx, confirmation_rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(HandlerCommand::SubscribeConfirmed(topic, confirmation_tx))
            .map_err(|error| anyhow::anyhow!("PredictFun WebSocket handler stopped: {error}"))?;
        tokio::time::timeout(std::time::Duration::from_secs(15), confirmation_rx)
            .await
            .map_err(|_| anyhow::anyhow!("PredictFun WebSocket subscription ACK timed out"))?
            .map_err(|_| anyhow::anyhow!("PredictFun WebSocket handler stopped before ACK"))?
    }

    pub fn unsubscribe(&self, topic: String) -> anyhow::Result<()> {
        validate_topic(&topic)?;
        self.cmd_tx
            .send(HandlerCommand::Unsubscribe(topic))
            .map_err(|error| anyhow::anyhow!("PredictFun WebSocket handler stopped: {error}"))
    }
}

pub struct PredictFunWebSocketClient {
    url: String,
    api_key: Option<SecretString>,
    backend: TransportBackend,
    signal: Arc<AtomicBool>,
    cmd_tx: tokio::sync::mpsc::UnboundedSender<HandlerCommand>,
    out_rx: Option<tokio::sync::mpsc::UnboundedReceiver<PredictFunWsEvent>>,
    task_handle: Option<JoinHandle<()>>,
}

impl fmt::Debug for PredictFunWebSocketClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct(stringify!(PredictFunWebSocketClient))
            .field("url", &self.url)
            .field("api_key", &self.api_key)
            .field("backend", &self.backend)
            .field("connected", &self.task_handle.is_some())
            .finish()
    }
}

impl PredictFunWebSocketClient {
    pub fn new(
        url: impl Into<String>,
        api_key: Option<SecretString>,
        backend: TransportBackend,
    ) -> Self {
        let (cmd_tx, _) = tokio::sync::mpsc::unbounded_channel();
        Self {
            url: url.into(),
            api_key,
            backend,
            signal: Arc::new(AtomicBool::new(false)),
            cmd_tx,
            out_rx: None,
            task_handle: None,
        }
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        self.connect_with_state_sink(None).await
    }

    pub async fn connect_with_state_sink(
        &mut self,
        state_sink: Option<SocketStateSink>,
    ) -> anyhow::Result<()> {
        if self.task_handle.is_some() {
            return Ok(());
        }
        self.signal.store(false, Ordering::Release);
        let (message_handler, raw_rx) = channel_epoch_message_handler();
        let config = self.websocket_config();
        let client = WebSocketClient::connect_with_rate_limiter_and_epoch_handler_and_state_sink(
            config,
            message_handler,
            None,
            Arc::new(RateLimiter::new_with_quota(None, vec![])),
            state_sink,
        )
        .await?;
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel();
        self.cmd_tx = cmd_tx;
        self.out_rx = Some(out_rx);
        let signal = Arc::clone(&self.signal);
        self.task_handle = Some(get_runtime().spawn(async move {
            FeedHandler::new(signal, client, cmd_rx, raw_rx, out_tx)
                .run()
                .await;
        }));
        Ok(())
    }

    fn websocket_config(&self) -> WebSocketConfig {
        let headers = self
            .api_key
            .as_ref()
            .map(|api_key| vec![("x-api-key".to_string(), api_key.expose().to_string())])
            .unwrap_or_default();
        WebSocketConfig {
            url: self.url.clone(),
            headers,
            heartbeat_interval_secs: None,
            heartbeat_payload: None,
            connect_timeout_ms: Some(15_000),
            reconnect_delay_initial_ms: Some(250),
            reconnect_delay_max_ms: Some(10_000),
            reconnect_backoff_factor: Some(2.0),
            reconnect_jitter_ms: Some(250),
            reconnect_max_attempts: None,
            // Predict sends a server heartbeat every 15 seconds. Three missed probes indicate a
            // silent transport and must drive the shared client's reconnect path.
            heartbeat_timeout_secs: Some(45),
            // An authenticated wallet topic can legitimately be quiet, so do not use application
            // data idleness as a liveness signal. Server heartbeat frames refresh the timeout above.
            idle_timeout_ms: None,
            backend: self.backend,
            proxy_url: None,
        }
    }

    pub fn subscription_handle(&self) -> PredictFunWebSocketSubscriptionHandle {
        PredictFunWebSocketSubscriptionHandle {
            cmd_tx: self.cmd_tx.clone(),
        }
    }

    pub fn take_out_rx(
        &mut self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<PredictFunWsEvent>> {
        self.out_rx.take()
    }

    pub async fn disconnect(&mut self) {
        self.signal.store(true, Ordering::Release);
        let _ = self.cmd_tx.send(HandlerCommand::Disconnect);
        if let Some(task) = self.task_handle.take() {
            let abort = task.abort_handle();
            if tokio::time::timeout(std::time::Duration::from_secs(2), task)
                .await
                .is_err()
            {
                abort.abort();
            }
        }
        self.out_rx = None;
    }
}

fn validate_topic(topic: &str) -> anyhow::Result<()> {
    const PREFIXES: &[&str] = &[
        "predictOrderbook/",
        "predictTradingStatus/",
        "predictMarketStatus/",
        "predictMarketChanged/",
        "predictWalletEvents/",
    ];
    let Some(parameter) = PREFIXES
        .iter()
        .find_map(|prefix| topic.strip_prefix(prefix))
    else {
        anyhow::bail!("unsupported PredictFun WebSocket topic");
    };
    if parameter.is_empty() || parameter.contains('/') {
        anyhow::bail!("invalid PredictFun WebSocket topic parameter");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_credentials() {
        let client = PredictFunWebSocketClient::new(
            "wss://example.test/ws",
            Some(SecretString::new("api-secret".to_string()).unwrap()),
            TransportBackend::Tungstenite,
        );
        assert!(!format!("{client:?}").contains("api-secret"));
    }

    #[test]
    fn quiet_private_wallet_stream_has_no_idle_disconnect() {
        let client = PredictFunWebSocketClient::new(
            "wss://example.test/ws",
            Some(SecretString::new("api-secret".to_string()).unwrap()),
            TransportBackend::Tungstenite,
        );

        assert_eq!(client.websocket_config().idle_timeout_ms, None);
    }

    #[test]
    fn server_driven_heartbeat_has_dead_peer_timeout_without_unsolicited_client_ping() {
        let client = PredictFunWebSocketClient::new(
            "wss://example.test/ws",
            Some(SecretString::new("api-secret".to_string()).unwrap()),
            TransportBackend::Tungstenite,
        );

        let config = client.websocket_config();
        assert_eq!(config.heartbeat_interval_secs, None);
        assert_eq!(config.heartbeat_payload, None);
        assert_eq!(config.heartbeat_timeout_secs, Some(45));
    }
}
