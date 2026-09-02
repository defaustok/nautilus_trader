//! Read-only PredictFun testnet qualification probe.
//!
//! Set `PREDICTFUN_TESTNET_PRIVATE_KEY_FILE` to a file containing the EOA key.
//! The probe signs authentication locally and never prints the key or JWT.

use std::{collections::HashMap, env, fs, time::Duration};

use nautilus_network::websocket::TransportBackend;
use nautilus_predictfun::{
    common::{consts::PREDICTFUN_TESTNET_API_BASE, enums::PredictFunEnvironment},
    http::{PredictFunHttpClient, models::PredictFunAuthRequest},
    signing::eip712::PredictFunOrderSigner,
    websocket::PredictFunWebSocketClient,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let private_key_path = env::var("PREDICTFUN_TESTNET_PRIVATE_KEY_FILE")?;
    let private_key = fs::read_to_string(private_key_path)?;
    let signer = PredictFunOrderSigner::new(private_key.trim())?;
    let client = PredictFunHttpClient::new(PREDICTFUN_TESTNET_API_BASE, None, 30)?;

    let market_id = env::var("PREDICTFUN_TESTNET_MARKET_ID").unwrap_or_else(|_| "1049".to_string());
    let market_id_number = market_id.parse()?;
    let filters = HashMap::from([("marketId".to_string(), market_id)]);
    let market = tokio::time::timeout(Duration::from_secs(35), client.get_market(market_id_number))
        .await??;
    eprintln!("market read OK");
    let book = tokio::time::timeout(
        Duration::from_secs(35),
        client.get_orderbook(market_id_number),
    )
    .await??;
    eprintln!("order book read OK");

    let message =
        tokio::time::timeout(Duration::from_secs(35), client.get_auth_message()).await??;
    let signature = signer.sign_auth_message(&message, None, PredictFunEnvironment::Testnet)?;
    let token = tokio::time::timeout(
        Duration::from_secs(35),
        client.authenticate(&PredictFunAuthRequest {
            signer: format!("{:#x}", signer.address()),
            signature,
            message,
        }),
    )
    .await??;
    eprintln!("authentication OK");
    let orders =
        tokio::time::timeout(Duration::from_secs(35), client.get_orders(&token, None)).await??;
    eprintln!("orders read OK");
    let positions =
        tokio::time::timeout(Duration::from_secs(35), client.get_positions(&token, None)).await??;
    eprintln!("positions read OK");
    let activity = tokio::time::timeout(
        Duration::from_secs(35),
        client.get_account_activity(&token, None),
    )
    .await??;
    eprintln!("account activity read OK");
    let matches =
        tokio::time::timeout(Duration::from_secs(35), client.get_matches(Some(&filters))).await??;
    eprintln!("matches read OK");

    if let Ok(websocket_url) = env::var("PREDICTFUN_TESTNET_WEBSOCKET_URL") {
        let api_key = env::var("PREDICTFUN_TESTNET_API_KEY_FILE")
            .ok()
            .map(fs::read_to_string)
            .transpose()?
            .map(|value| nautilus_predictfun::config::SecretString::new(value.trim().to_string()))
            .transpose()?;
        let mut websocket =
            PredictFunWebSocketClient::new(websocket_url, api_key, TransportBackend::Tungstenite);
        websocket.connect().await?;
        websocket
            .subscription_handle()
            .subscribe_confirmed(format!("predictWalletEvents/{}", token.expose()))
            .await?;
        websocket.disconnect().await;
        eprintln!("private WebSocket subscription ACK OK");
    }

    println!(
        "testnet probe OK: address={:#x} market={} book_bids={} book_asks={} orders={} positions={} activity={} matches={}",
        signer.address(),
        market.id,
        book.bids.len(),
        book.asks.len(),
        orders.len(),
        positions.len(),
        activity.len(),
        matches.len(),
    );
    Ok(())
}
