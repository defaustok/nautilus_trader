//! PredictFun testnet `ExecTester` harness. It defaults to dry-run.
//!
//! Required: the four data-harness variables plus `PREDICTFUN_TESTNET_PRIVATE_KEY` and
//! `PREDICTFUN_TESTNET_RPC_URL`. Set `PREDICTFUN_TESTNET_ACCOUNT_TYPE=predict_account` and
//! `PREDICTFUN_TESTNET_ACCOUNT_ADDRESS` for a Predict Account. Setting
//! `PREDICTFUN_EXEC_TESTER_LIVE=1` authorizes testnet orders only. The API key is optional on
//! testnet.

use std::{collections::HashMap, env, str::FromStr};

use nautilus_common::enums::Environment;
use nautilus_live::{config::LiveExecEngineConfig, node::LiveNode};
use nautilus_model::{
    identifiers::{AccountId, ClientId, InstrumentId, StrategyId, TraderId},
    types::Quantity,
};
use nautilus_predictfun::{
    common::{
        consts::PREDICTFUN,
        enums::{PredictFunAccountType, PredictFunEnvironment},
    },
    config::{PredictFunDataClientConfig, PredictFunExecClientConfig, SecretString},
    factories::{PredictFunDataClientFactory, PredictFunExecutionClientFactory},
};
use nautilus_testkit::testers::{ExecTester, ExecTesterConfig};
use nautilus_trading::strategy::StrategyConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let trader_id = TraderId::from("TESTER-001");
    let account_id = AccountId::from("PREDICTFUN-TESTNET-001");
    let api_key = env::var("PREDICTFUN_TESTNET_API_KEY")
        .ok()
        .map(SecretString::new)
        .transpose()?;
    let websocket_url = env::var("PREDICTFUN_TESTNET_WEBSOCKET_URL")?;
    let market_id = env::var("PREDICTFUN_TESTNET_MARKET_ID")?;
    let instrument_id = InstrumentId::from(env::var("PREDICTFUN_TESTNET_INSTRUMENT_ID")?.as_str());
    let account_type = env::var("PREDICTFUN_TESTNET_ACCOUNT_TYPE")
        .ok()
        .map_or(Ok(PredictFunAccountType::Eoa), |value| {
            PredictFunAccountType::from_str(&value.to_ascii_lowercase())
        })?;
    let data_config = PredictFunDataClientConfig::builder()
        .environment(PredictFunEnvironment::Testnet)
        .maybe_api_key(api_key.clone())
        .websocket_url(websocket_url.clone())
        .market_filters(HashMap::from([("marketId".to_string(), market_id)]))
        .build();
    let exec_builder = PredictFunExecClientConfig::builder()
        .trader_id(trader_id)
        .account_id(account_id)
        .environment(PredictFunEnvironment::Testnet)
        .maybe_api_key(api_key)
        .private_key(secret("PREDICTFUN_TESTNET_PRIVATE_KEY")?)
        .rpc_url(secret("PREDICTFUN_TESTNET_RPC_URL")?)
        .websocket_url(websocket_url)
        .account_type(account_type);
    let exec_config = match env::var("PREDICTFUN_TESTNET_ACCOUNT_ADDRESS") {
        Ok(address) => exec_builder.account_address(address).build(),
        Err(_) => exec_builder.build(),
    };
    exec_config.validate()?;
    let mut node = LiveNode::builder(trader_id, Environment::Live)?
        .with_name("PREDICTFUN-EXEC-TESTER-001".to_string())
        .with_exec_engine_config(LiveExecEngineConfig {
            reconciliation_lookback_mins: Some(24 * 60),
            reconciliation_instrument_ids: Some(vec![instrument_id.to_string()]),
            open_check_interval_secs: Some(10.0),
            position_check_interval_secs: Some(30.0),
            ..Default::default()
        })
        .add_data_client(
            None,
            Box::new(PredictFunDataClientFactory::new()),
            Box::new(data_config),
        )?
        .add_exec_client(
            None,
            Box::new(PredictFunExecutionClientFactory::new()),
            Box::new(exec_config),
        )?
        .with_reconciliation(true)
        .with_delay_post_stop_secs(5)
        .build()?;
    let dry_run = env::var("PREDICTFUN_EXEC_TESTER_LIVE").as_deref() != Ok("1");
    let quantity = Quantity::from("0.01");
    node.add_strategy(ExecTester::new(
        ExecTesterConfig::builder()
            .base(StrategyConfig {
                strategy_id: Some(StrategyId::from("EXEC_TESTER-001")),
                external_order_claims: Some(vec![instrument_id]),
                ..Default::default()
            })
            .instrument_id(instrument_id)
            .client_id(ClientId::new(PREDICTFUN))
            .order_qty(quantity)
            .subscribe_quotes(true)
            .subscribe_trades(false)
            .subscribe_book(false)
            .enable_limit_buys(true)
            .enable_limit_sells(false)
            .enable_stop_buys(false)
            .enable_stop_sells(false)
            .use_post_only(true)
            .cancel_orders_on_stop(true)
            .close_positions_on_stop(false)
            .dry_run(dry_run)
            .log_data(false)
            .build()?,
    ))?;
    node.run().await?;
    Ok(())
}

fn secret(name: &str) -> anyhow::Result<SecretString> {
    SecretString::new(env::var(name)?)
}
