use std::{
    fmt,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use alloy_primitives::Address;
use nautilus_network::{SocketState, SocketStateSink, websocket::TransportBackend};

use crate::{
    common::enums::{PredictFunAccountType, PredictFunEnvironment},
    config::SecretString,
    http::{PredictFunHttpClient, models::PredictFunAuthRequest},
    signing::eip712::PredictFunOrderSigner,
    websocket::{PredictFunWebSocketClient, PredictFunWsEvent},
};

pub struct PredictFunExecutionSession {
    http_client: PredictFunHttpClient,
    ws_client: PredictFunWebSocketClient,
    token: Arc<Mutex<Option<SecretString>>>,
    account_address: Address,
    signer: Arc<PredictFunOrderSigner>,
    environment: PredictFunEnvironment,
    account_type: PredictFunAccountType,
    private_ready: Arc<AtomicBool>,
    event_task: Option<tokio::task::JoinHandle<()>>,
}

impl fmt::Debug for PredictFunExecutionSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct(stringify!(PredictFunExecutionSession))
            .field("http_client", &self.http_client)
            .field("ws_client", &self.ws_client)
            .field("token", &"<redacted>")
            .field("account_address", &self.account_address)
            .finish()
    }
}

impl PredictFunExecutionSession {
    #[allow(clippy::too_many_arguments)]
    pub async fn connect(
        http_client: PredictFunHttpClient,
        websocket_url: &str,
        api_key: Option<SecretString>,
        signer: Arc<PredictFunOrderSigner>,
        environment: PredictFunEnvironment,
        account_type: PredictFunAccountType,
        configured_account_address: Option<&str>,
        transport_backend: TransportBackend,
        token: Arc<Mutex<Option<SecretString>>>,
        private_ready: Arc<AtomicBool>,
    ) -> anyhow::Result<Self> {
        let account_address = match account_type {
            PredictFunAccountType::Eoa => {
                if let Some(configured) = configured_account_address {
                    let configured = Address::from_str(configured)?;
                    if configured != signer.address() {
                        anyhow::bail!("configured EOA account_address does not match private key");
                    }
                }
                signer.address()
            }
            PredictFunAccountType::PredictAccount => {
                Address::from_str(configured_account_address.ok_or_else(|| {
                    anyhow::anyhow!("Predict account execution requires account_address")
                })?)?
            }
        };
        let fresh_token = authenticate(
            &http_client,
            &signer,
            environment,
            account_type,
            account_address,
        )
        .await?;
        let mut ws_client =
            PredictFunWebSocketClient::new(websocket_url, api_key, transport_backend);
        let state_ready = Arc::clone(&private_ready);
        let state_sink = SocketStateSink::new(move |state| {
            update_private_readiness_on_transport_state(&state_ready, state);
        });
        ws_client.connect_with_state_sink(Some(state_sink)).await?;
        ws_client
            .subscription_handle()
            .subscribe_confirmed(wallet_topic(&fresh_token))
            .await?;
        *token
            .lock()
            .map_err(|_| anyhow::anyhow!("PredictFun token lock poisoned"))? = Some(fresh_token);
        private_ready.store(true, Ordering::Release);
        Ok(Self {
            http_client,
            ws_client,
            token,
            account_address,
            signer,
            environment,
            account_type,
            private_ready,
            event_task: None,
        })
    }

    pub fn http_client(&self) -> &PredictFunHttpClient {
        &self.http_client
    }

    pub fn account_address(&self) -> Address {
        self.account_address
    }

    pub fn take_events(
        &mut self,
    ) -> anyhow::Result<tokio::sync::mpsc::UnboundedReceiver<PredictFunWsEvent>> {
        let mut raw_events = self.ws_client.take_out_rx().ok_or_else(|| {
            anyhow::anyhow!("PredictFun execution WebSocket receiver unavailable")
        })?;
        let subscription = self.ws_client.subscription_handle();
        let http_client = self.http_client.clone();
        let signer = Arc::clone(&self.signer);
        let token = Arc::clone(&self.token);
        let private_ready = Arc::clone(&self.private_ready);
        let account_address = self.account_address;
        let environment = self.environment;
        let account_type = self.account_type;
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
        self.event_task = Some(nautilus_common::live::get_runtime().spawn(async move {
            while let Some(event) = raw_events.recv().await {
                if matches!(event, PredictFunWsEvent::Reconnected) {
                    private_ready.store(false, Ordering::Release);
                    let old_topic = token
                        .lock()
                        .ok()
                        .and_then(|guard| guard.as_ref().map(wallet_topic));
                    if let Ok(mut guard) = token.lock() {
                        *guard = None;
                    }
                    let result = async {
                        let fresh = authenticate(
                            &http_client,
                            &signer,
                            environment,
                            account_type,
                            account_address,
                        )
                        .await?;
                        if let Some(old_topic) = old_topic {
                            subscription.unsubscribe(old_topic)?;
                        }
                        subscription
                            .subscribe_confirmed(wallet_topic(&fresh))
                            .await?;
                        *token
                            .lock()
                            .map_err(|_| anyhow::anyhow!("PredictFun token lock poisoned"))? =
                            Some(fresh);
                        private_ready.store(true, Ordering::Release);
                        Ok::<(), anyhow::Error>(())
                    }
                    .await;
                    if let Err(error) = result {
                        let _ = event_tx.send(PredictFunWsEvent::Error(format!(
                            "PredictFun private reauthentication failed: {error}"
                        )));
                        continue;
                    }
                }
                if event_tx.send(event).is_err() {
                    break;
                }
            }
            private_ready.store(false, Ordering::Release);
        }));
        Ok(event_rx)
    }

    pub async fn disconnect(&mut self) {
        self.private_ready.store(false, Ordering::Release);
        self.ws_client.disconnect().await;
        if let Some(task) = self.event_task.take() {
            task.abort();
        }
    }
}

fn update_private_readiness_on_transport_state(private_ready: &AtomicBool, state: SocketState) {
    if state == SocketState::Disconnected {
        private_ready.store(false, Ordering::Release);
    }
}

fn wallet_topic(token: &SecretString) -> String {
    format!("predictWalletEvents/{}", token.expose())
}

async fn authenticate(
    http_client: &PredictFunHttpClient,
    signer: &PredictFunOrderSigner,
    environment: PredictFunEnvironment,
    account_type: PredictFunAccountType,
    account_address: Address,
) -> anyhow::Result<SecretString> {
    let message = http_client.get_auth_message().await?;
    let predict_account =
        (account_type == PredictFunAccountType::PredictAccount).then_some(account_address);
    let signature = signer.sign_auth_message(&message, predict_account, environment)?;
    Ok(http_client
        .authenticate(&PredictFunAuthRequest {
            signer: format!("{account_address:#x}"),
            signature,
            message,
        })
        .await?)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn transport_disconnect_clears_private_readiness() {
        let private_ready = AtomicBool::new(true);

        update_private_readiness_on_transport_state(&private_ready, SocketState::Disconnected);

        assert!(!private_ready.load(Ordering::Acquire));
    }

    #[rstest]
    fn transport_connect_does_not_bypass_private_subscription_gate() {
        let private_ready = AtomicBool::new(false);

        update_private_readiness_on_transport_state(&private_ready, SocketState::Connected);

        assert!(!private_ready.load(Ordering::Acquire));
    }
}
