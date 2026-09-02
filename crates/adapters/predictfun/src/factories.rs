use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::ClientId,
};

use crate::{
    common::consts::{PREDICTFUN, PREDICTFUN_VENUE, usdt},
    config::{PredictFunDataClientConfig, PredictFunExecClientConfig},
    data::PredictFunDataClient,
    execution::PredictFunExecutionClient,
};

impl ClientConfig for PredictFunDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.predictfun", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.predictfun")
)]
#[derive(Debug, Clone, Default)]
pub struct PredictFunDataClientFactory;

impl PredictFunDataClientFactory {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl DataClientFactory for PredictFunDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<PredictFunDataClientConfig>()
            .ok_or_else(|| anyhow::anyhow!("PredictFun data factory received invalid config"))?;
        Ok(Box::new(PredictFunDataClient::new(
            ClientId::from(name),
            config.clone(),
        )?))
    }

    fn name(&self) -> &'static str {
        PREDICTFUN
    }

    fn config_type(&self) -> &'static str {
        "PredictFunDataClientConfig"
    }
}

impl ClientConfig for PredictFunExecClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.predictfun", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.predictfun")
)]
#[derive(Debug, Clone, Default)]
pub struct PredictFunExecutionClientFactory;

impl PredictFunExecutionClientFactory {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl ExecutionClientFactory for PredictFunExecutionClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let config = config
            .as_any()
            .downcast_ref::<PredictFunExecClientConfig>()
            .ok_or_else(|| anyhow::anyhow!("PredictFun execution factory received invalid config"))?
            .clone();
        let core = ExecutionClientCore::new(
            config.trader_id,
            ClientId::from(name),
            *PREDICTFUN_VENUE,
            OmsType::Netting,
            config.account_id,
            AccountType::Cash,
            Some(usdt()),
            cache,
        );
        Ok(Box::new(PredictFunExecutionClient::new(core, config)?))
    }

    fn name(&self) -> &'static str {
        PREDICTFUN
    }

    fn config_type(&self) -> &'static str {
        "PredictFunExecClientConfig"
    }
}
