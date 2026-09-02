use std::str::FromStr;

use nautilus_core::python::to_pyvalue_err;
use nautilus_model::identifiers::{AccountId, TraderId};
use nautilus_network::websocket::TransportBackend;
use pyo3::{PyResult, pymethods};

use crate::{
    common::enums::{PredictFunAccountType, PredictFunEnvironment},
    config::{PredictFunDataClientConfig, PredictFunExecClientConfig, SecretString},
};

fn secret(value: Option<String>) -> PyResult<Option<SecretString>> {
    value
        .map(SecretString::new)
        .transpose()
        .map_err(|error| to_pyvalue_err(error.to_string()))
}

fn parse_environment(
    value: Option<String>,
    default: PredictFunEnvironment,
) -> PyResult<PredictFunEnvironment> {
    value.map_or(Ok(default), |value| {
        PredictFunEnvironment::from_str(&value.to_ascii_uppercase())
            .map_err(|error| to_pyvalue_err(error.to_string()))
    })
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PredictFunDataClientConfig {
    #[new]
    #[pyo3(signature = (environment=None, api_key=None, api_url=None, websocket_url=None, market_filters=None, update_instruments_interval_mins=None, request_timeout_secs=None, transport_backend=None))]
    fn py_new(
        environment: Option<String>,
        api_key: Option<String>,
        api_url: Option<String>,
        websocket_url: Option<String>,
        market_filters: Option<std::collections::HashMap<String, String>>,
        update_instruments_interval_mins: Option<u64>,
        request_timeout_secs: Option<u64>,
        transport_backend: Option<TransportBackend>,
    ) -> PyResult<Self> {
        let default = Self::default();
        Ok(Self {
            environment: parse_environment(environment, default.environment)?,
            api_key: secret(api_key)?,
            api_url,
            websocket_url,
            market_filters,
            update_instruments_interval_mins,
            request_timeout_secs: request_timeout_secs.unwrap_or(default.request_timeout_secs),
            transport_backend: transport_backend.unwrap_or(default.transport_backend),
        })
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PredictFunExecClientConfig {
    #[new]
    #[expect(clippy::too_many_arguments)]
    #[pyo3(signature = (trader_id=None, account_id=None, environment=None, api_key=None, private_key=None, account_address=None, account_type=None, api_url=None, websocket_url=None, rpc_url=None, market_slippage_bps=None, request_timeout_secs=None, transport_backend=None))]
    fn py_new(
        trader_id: Option<String>,
        account_id: Option<String>,
        environment: Option<String>,
        api_key: Option<String>,
        private_key: Option<String>,
        account_address: Option<String>,
        account_type: Option<String>,
        api_url: Option<String>,
        websocket_url: Option<String>,
        rpc_url: Option<String>,
        market_slippage_bps: Option<u32>,
        request_timeout_secs: Option<u64>,
        transport_backend: Option<TransportBackend>,
    ) -> PyResult<Self> {
        let default = Self::default();
        let account_type = account_type.map_or(Ok(default.account_type), |value| {
            PredictFunAccountType::from_str(&value.to_ascii_lowercase())
                .map_err(|error| to_pyvalue_err(error.to_string()))
        })?;
        let config = Self {
            trader_id: trader_id.map_or(default.trader_id, |value| TraderId::from(value.as_str())),
            account_id: account_id
                .map_or(default.account_id, |value| AccountId::from(value.as_str())),
            environment: parse_environment(environment, default.environment)?,
            api_key: secret(api_key)?,
            private_key: secret(private_key)?,
            account_address,
            account_type,
            api_url,
            websocket_url,
            rpc_url: secret(rpc_url)?,
            market_slippage_bps: market_slippage_bps.unwrap_or(default.market_slippage_bps),
            request_timeout_secs: request_timeout_secs.unwrap_or(default.request_timeout_secs),
            transport_backend: transport_backend.unwrap_or(default.transport_backend),
        };
        config
            .validate()
            .map_err(|error| to_pyvalue_err(error.to_string()))?;
        Ok(config)
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}
