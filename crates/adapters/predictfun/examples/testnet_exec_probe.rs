//! Explicitly opt-in, minimally funded PredictFun testnet order probe.
//!
//! This example submits one post-only BUY at the venue's $0.90 minimum value,
//! confirms it via REST, and removes it from the off-chain book. It never
//! prints credentials.

use std::{
    env, fs,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy_primitives::Address;
use nautilus_predictfun::{
    common::{
        consts::PREDICTFUN_TESTNET_API_BASE,
        enums::{
            PredictFunEnvironment, PredictFunSide, PredictFunSignatureType, PredictFunStrategy,
        },
    },
    http::{
        PredictFunHttpClient,
        models::{PredictFunAuthRequest, PredictFunContractOrder, PredictFunCreateOrderData},
    },
    signing::{
        eip712::{PredictFunOrderSigner, order_hash},
        order_builder::{limit_order_amounts, market_order_amounts_by_quantity},
    },
};
use rand::RngExt;
use rust_decimal::Decimal;

const MARKET_ID: u64 = 1049;
const LIMIT_EXPIRATION_SECS: u64 = 4_102_444_800;
const MAX_SALT: u32 = 2_147_483_648;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if env::var("PREDICTFUN_EXEC_TESTER_LIVE").as_deref() != Ok("1") {
        anyhow::bail!("set PREDICTFUN_EXEC_TESTER_LIVE=1 to authorize a testnet order");
    }

    let private_key_path = env::var("PREDICTFUN_TESTNET_PRIVATE_KEY_FILE")?;
    let private_key = fs::read_to_string(private_key_path)?;
    let signer = PredictFunOrderSigner::new(private_key.trim())?;
    let client = PredictFunHttpClient::new(PREDICTFUN_TESTNET_API_BASE, None, 30)?;
    let market = client.get_market(MARKET_ID).await?;
    if market.trading_status != "OPEN" || market.is_neg_risk || market.is_yield_bearing {
        anyhow::bail!("market {MARKET_ID} is not the expected open standard market");
    }
    let outcome = market
        .outcomes
        .iter()
        .find(|outcome| outcome.name.eq_ignore_ascii_case("yes"))
        .ok_or_else(|| anyhow::anyhow!("market {MARKET_ID} has no Yes outcome"))?;

    let message = client.get_auth_message().await?;
    let signature = signer.sign_auth_message(&message, None, PredictFunEnvironment::Testnet)?;
    let token = client
        .authenticate(&PredictFunAuthRequest {
            signer: format!("{:#x}", signer.address()),
            signature,
            message,
        })
        .await?;

    if let Ok(order_id) = env::var("PREDICTFUN_TESTNET_REMOVE_ORDER_ID") {
        let removed = client.remove_orders(&token, vec![order_id]).await?;
        println!(
            "ORDER_REMOVED=removed:{:?},noop:{:?}",
            removed.removed, removed.noop
        );
        return Ok(());
    }

    let market_buy = env::var("PREDICTFUN_TESTNET_MARKET_BUY").as_deref() == Ok("1");
    let (amounts, strategy) = if market_buy {
        let book = client.get_orderbook(MARKET_ID).await?;
        (
            market_order_amounts_by_quantity(
                PredictFunSide::Buy,
                Decimal::ONE,
                &book.bids,
                &book.asks,
                100,
                false,
            )?,
            PredictFunStrategy::Market,
        )
    } else {
        (
            limit_order_amounts(PredictFunSide::Buy, Decimal::new(1, 2), Decimal::new(90, 0))?,
            PredictFunStrategy::Limit,
        )
    };
    let address = format!("{:#x}", signer.address());
    let mut order = PredictFunContractOrder {
        salt: rand::rng().random_range(0..=MAX_SALT).to_string(),
        maker: address.clone(),
        signer: address,
        taker: format!("{:#x}", Address::ZERO),
        token_id: outcome.on_chain_id.clone(),
        maker_amount: amounts.maker_amount.to_string(),
        taker_amount: amounts.taker_amount.to_string(),
        expiration: if market_buy {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_secs()
                .saturating_add(300)
                .to_string()
        } else {
            LIMIT_EXPIRATION_SECS.to_string()
        },
        nonce: "0".to_string(),
        fee_rate_bps: market.fee_rate_bps.to_string(),
        side: PredictFunSide::Buy,
        signature_type: PredictFunSignatureType::Eoa,
        signature: None,
        hash: None,
    };
    let hash = order_hash(&order, PredictFunEnvironment::Testnet, false, false)?;
    order.signature =
        Some(signer.sign_order(&order, PredictFunEnvironment::Testnet, false, false)?);
    order.hash = Some(format!("{hash:#x}"));
    let data = PredictFunCreateOrderData {
        price_per_share: amounts.price_per_share.to_string(),
        strategy,
        order,
        slippage_bps: market_buy.then(|| "100".to_string()),
        is_fill_or_kill: market_buy.then_some(true),
        is_post_only: (!market_buy).then_some(true),
        self_trade_prevention: None,
        is_min_amount_out: market_buy.then_some(false),
    };

    let response = client.create_order(&token, data).await?;
    println!("ORDER_ACCEPTED={}", serde_json::to_string(&response)?);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut record = client.get_order(&token, &response.order_hash).await?;
    if market_buy {
        for _ in 0..15 {
            if record.status != "OPEN" {
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
            record = client.get_order(&token, &response.order_hash).await?;
        }
    }
    println!("ORDER_RECORD={}", serde_json::to_string(&record)?);
    if market_buy {
        let positions = client.get_positions(&token, None).await?;
        let activity = client.get_account_activity(&token, None).await?;
        println!(
            "MARKET_BUY_RECONCILED=status:{},positions:{},activity:{}",
            record.status,
            positions.len(),
            activity.len()
        );
        return Ok(());
    }
    let removed = client
        .remove_orders(&token, vec![response.order_id])
        .await?;
    println!(
        "ORDER_REMOVED=removed:{:?},noop:{:?}",
        removed.removed, removed.noop
    );
    Ok(())
}
