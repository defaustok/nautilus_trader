use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use nautilus_network::{RECONNECTED, websocket::WebSocketClient};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot,
};
use tokio_tungstenite::tungstenite::Message;

use super::messages::{PredictFunHeartbeatRequest, PredictFunWsMessage, PredictFunWsRequest};

#[derive(Debug)]
pub enum HandlerCommand {
    Subscribe(String),
    SubscribeConfirmed(String, oneshot::Sender<anyhow::Result<()>>),
    Unsubscribe(String),
    Disconnect,
}

#[derive(Debug)]
pub enum PredictFunWsEvent {
    Message(PredictFunWsMessage),
    Reconnected,
    Error(String),
}

struct PendingRequest {
    method: &'static str,
    topic: String,
    confirmation: Option<oneshot::Sender<anyhow::Result<()>>>,
}

pub struct FeedHandler {
    signal: Arc<AtomicBool>,
    client: WebSocketClient,
    cmd_rx: UnboundedReceiver<HandlerCommand>,
    raw_rx: UnboundedReceiver<(u64, Message)>,
    out_tx: UnboundedSender<PredictFunWsEvent>,
    subscriptions: BTreeSet<String>,
    pending_requests: BTreeMap<u64, PendingRequest>,
    next_request_id: AtomicU64,
}

impl FeedHandler {
    pub fn new(
        signal: Arc<AtomicBool>,
        client: WebSocketClient,
        cmd_rx: UnboundedReceiver<HandlerCommand>,
        raw_rx: UnboundedReceiver<(u64, Message)>,
        out_tx: UnboundedSender<PredictFunWsEvent>,
    ) -> Self {
        Self {
            signal,
            client,
            cmd_rx,
            raw_rx,
            out_tx,
            subscriptions: BTreeSet::new(),
            pending_requests: BTreeMap::new(),
            next_request_id: AtomicU64::new(1),
        }
    }

    pub async fn run(mut self) {
        while !self.signal.load(Ordering::Acquire) {
            tokio::select! {
                command = self.cmd_rx.recv() => {
                    let Some(command) = command else { break };
                    if !self.handle_command(command).await { break; }
                }
                raw = self.raw_rx.recv() => {
                    let Some((epoch, message)) = raw else { break };
                    self.handle_message(epoch, message).await;
                }
            }
        }
        self.client.disconnect().await;
    }

    async fn handle_command(&mut self, command: HandlerCommand) -> bool {
        match command {
            HandlerCommand::Subscribe(topic) => {
                if self.subscriptions.insert(topic.clone()) {
                    match self.send_request("subscribe", topic.clone(), None).await {
                        Ok(request_id) => {
                            self.pending_requests.insert(
                                request_id,
                                PendingRequest {
                                    method: "subscribe",
                                    topic,
                                    confirmation: None,
                                },
                            );
                        }
                        Err(error) => self.emit_error(error),
                    }
                }
                true
            }
            HandlerCommand::SubscribeConfirmed(topic, confirmation) => {
                if self.subscriptions.insert(topic.clone()) {
                    match self.send_request("subscribe", topic.clone(), None).await {
                        Ok(request_id) => {
                            self.pending_requests.insert(
                                request_id,
                                PendingRequest {
                                    method: "subscribe",
                                    topic,
                                    confirmation: Some(confirmation),
                                },
                            );
                        }
                        Err(error) => {
                            let message = error.to_string();
                            let _ = confirmation.send(Err(error));
                            self.emit_error(message);
                        }
                    }
                } else {
                    let _ = confirmation.send(Ok(()));
                }
                true
            }
            HandlerCommand::Unsubscribe(topic) => {
                if self.subscriptions.remove(&topic) {
                    match self.send_request("unsubscribe", topic.clone(), None).await {
                        Ok(request_id) => {
                            self.pending_requests.insert(
                                request_id,
                                PendingRequest {
                                    method: "unsubscribe",
                                    topic,
                                    confirmation: None,
                                },
                            );
                        }
                        Err(error) => self.emit_error(error),
                    }
                }
                true
            }
            HandlerCommand::Disconnect => false,
        }
    }

    async fn handle_message(&mut self, epoch: u64, message: Message) {
        match message {
            Message::Text(text) if text == RECONNECTED => {
                self.pending_requests.clear();
                // Wallet topics contain a short-lived JWT. The execution session obtains a
                // fresh token and replaces this topic after receiving `Reconnected`; replaying
                // the old topic here can falsely signal private readiness.
                let topics = self
                    .subscriptions
                    .iter()
                    .filter(|topic| !is_private_topic(topic))
                    .cloned()
                    .collect::<Vec<_>>();
                for topic in topics {
                    match self
                        .send_request("subscribe", topic.clone(), Some(epoch))
                        .await
                    {
                        Ok(request_id) => {
                            self.pending_requests.insert(
                                request_id,
                                PendingRequest {
                                    method: "subscribe",
                                    topic,
                                    confirmation: None,
                                },
                            );
                        }
                        Err(error) => self.emit_error(error),
                    }
                }
                let _ = self.out_tx.send(PredictFunWsEvent::Reconnected);
            }
            Message::Text(text) => match PredictFunWsMessage::parse(&text) {
                Ok(PredictFunWsMessage::Heartbeat(timestamp)) => {
                    match serde_json::to_string(&PredictFunHeartbeatRequest::new(timestamp)) {
                        Ok(response) => {
                            if let Err(error) = self
                                .client
                                .send_text_on_connection(response, None, epoch)
                                .await
                            {
                                self.emit_error(error);
                            }
                        }
                        Err(error) => self.emit_error(error),
                    }
                }
                Ok(PredictFunWsMessage::Response(response)) => {
                    let Some(request_id) = response.request_id else {
                        self.emit_error("PredictFun WebSocket response missing requestId");
                        return;
                    };
                    let Some(pending) = self.pending_requests.remove(&request_id) else {
                        self.emit_error(format!(
                            "PredictFun WebSocket response has unknown requestId {request_id}"
                        ));
                        return;
                    };
                    let PendingRequest {
                        method,
                        topic,
                        confirmation,
                    } = pending;
                    if response.success == Some(false) {
                        let display_topic = redact_topic(&topic);
                        let message = format!(
                            "PredictFun WebSocket {method} rejected for {display_topic}: {:?}",
                            response.error
                        );
                        if let Some(confirmation) = confirmation {
                            let _ = confirmation.send(Err(anyhow::anyhow!(message.clone())));
                        }
                        self.emit_error(message);
                    } else if let Some(confirmation) = confirmation {
                        let _ = confirmation.send(Ok(()));
                    }
                    let _ = self.out_tx.send(PredictFunWsEvent::Message(
                        PredictFunWsMessage::Response(response),
                    ));
                }
                Ok(message) => {
                    let _ = self.out_tx.send(PredictFunWsEvent::Message(message));
                }
                Err(error) => self.emit_error(error),
            },
            Message::Ping(payload) => {
                if let Err(error) = self.client.send_pong(payload.to_vec()).await {
                    self.emit_error(error);
                }
            }
            Message::Close(_) => self.signal.store(true, Ordering::Release),
            Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }

    async fn send_request(
        &self,
        method: &'static str,
        topic: String,
        epoch: Option<u64>,
    ) -> anyhow::Result<u64> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let request = match method {
            "subscribe" => PredictFunWsRequest::subscribe(request_id, vec![topic]),
            "unsubscribe" => PredictFunWsRequest::unsubscribe(request_id, vec![topic]),
            _ => anyhow::bail!("unsupported PredictFun WebSocket method"),
        };
        let payload = serde_json::to_string(&request)?;
        match epoch {
            Some(epoch) => {
                self.client
                    .send_text_on_connection(payload, None, epoch)
                    .await?;
            }
            None => self.client.send_text(payload, None).await?,
        }
        Ok(request_id)
    }

    fn emit_error(&self, error: impl std::fmt::Display) {
        let _ = self
            .out_tx
            .send(PredictFunWsEvent::Error(error.to_string()));
    }
}

fn is_private_topic(topic: &str) -> bool {
    topic.starts_with("predictWalletEvents/")
}

fn redact_topic(topic: &str) -> &str {
    if is_private_topic(topic) {
        "predictWalletEvents/<redacted>"
    } else {
        topic
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wallet_topic_requires_fresh_authentication_after_reconnect() {
        assert!(is_private_topic("predictWalletEvents/expired-jwt"));
        assert!(!is_private_topic("predictOrderbook/42"));
    }

    #[test]
    fn wallet_topic_is_redacted_for_errors() {
        assert_eq!(
            redact_topic("predictWalletEvents/secret-jwt"),
            "predictWalletEvents/<redacted>"
        );
        assert_eq!(redact_topic("predictOrderbook/42"), "predictOrderbook/42");
    }
}
