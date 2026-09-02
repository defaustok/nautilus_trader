//! PredictFun `DataTester` harness (testnet by default; mainnet is read-only).
//!
//! Required: `PREDICTFUN_WEBSOCKET_URL`, `PREDICTFUN_MARKET_ID`, and
//! `PREDICTFUN_INSTRUMENT_ID`. Set `PREDICTFUN_ENVIRONMENT=MAINNET` and
//! `PREDICTFUN_API_KEY` for a mainnet data-only qualification run.

use std::{collections::HashMap, env, str::FromStr};

use nautilus_common::enums::Environment;
use nautilus_live::node::LiveNode;
use nautilus_model::identifiers::{ClientId, InstrumentId, TraderId};
use nautilus_predictfun::{
    common::{consts::PREDICTFUN, enums::PredictFunEnvironment},
    config::{PredictFunDataClientConfig, SecretString},
    factories::PredictFunDataClientFactory,
};
use nautilus_testkit::testers::{DataTester, DataTesterConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let environment = env::var("PREDICTFUN_ENVIRONMENT")
        .ok()
        .map(|value| PredictFunEnvironment::from_str(&value.to_ascii_uppercase()))
        .transpose()?
        .unwrap_or(PredictFunEnvironment::Testnet);
    let api_key = env::var("PREDICTFUN_API_KEY")
        .or_else(|_| env::var("PREDICTFUN_TESTNET_API_KEY"))
        .ok()
        .map(SecretString::new)
        .transpose()?;
    let websocket_url = env::var("PREDICTFUN_WEBSOCKET_URL")
        .or_else(|_| env::var("PREDICTFUN_TESTNET_WEBSOCKET_URL"))?;
    let market_id =
        env::var("PREDICTFUN_MARKET_ID").or_else(|_| env::var("PREDICTFUN_TESTNET_MARKET_ID"))?;
    let instrument_id = InstrumentId::from(
        env::var("PREDICTFUN_INSTRUMENT_ID")
            .or_else(|_| env::var("PREDICTFUN_TESTNET_INSTRUMENT_ID"))?
            .as_str(),
    );
    let config = PredictFunDataClientConfig::builder()
        .environment(environment)
        .maybe_api_key(api_key)
        .websocket_url(websocket_url)
        .market_filters(HashMap::from([("marketId".to_string(), market_id)]))
        .build();
    let mut node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Live)?
        .with_name("PREDICTFUN-DATA-TESTER-001".to_string())
        .with_delay_post_stop_secs(2)
        .add_data_client(
            None,
            Box::new(PredictFunDataClientFactory::new()),
            Box::new(config),
        )?
        .build()?;
    node.add_actor(DataTester::new(
        DataTesterConfig::builder()
            .client_id(ClientId::new(PREDICTFUN))
            .instrument_ids(vec![instrument_id])
            .request_instruments(true)
            .subscribe_book_deltas(true)
            .subscribe_quotes(true)
            .manage_book(true)
            .build()?,
    ))?;
    node.run().await?;
    Ok(())
}
