pub mod config;
pub mod factories;

use nautilus_common::factories::{ClientConfig, DataClientFactory, ExecutionClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    common::consts::{PREDICTFUN, PREDICTFUN_VENUE},
    config::{PredictFunDataClientConfig, PredictFunExecClientConfig},
    factories::{PredictFunDataClientFactory, PredictFunExecutionClientFactory},
};

pyo3_stub_gen::module_variable!("nautilus_trader.adapters.predictfun", "PREDICTFUN", String);
pyo3_stub_gen::module_variable!(
    "nautilus_trader.adapters.predictfun",
    "PREDICTFUN_VENUE",
    nautilus_model::identifiers::Venue
);

#[expect(clippy::needless_pass_by_value)]
fn extract_data_factory(py: Python<'_>, value: Py<PyAny>) -> PyResult<Box<dyn DataClientFactory>> {
    value
        .extract::<PredictFunDataClientFactory>(py)
        .map(|factory| Box::new(factory) as Box<dyn DataClientFactory>)
        .map_err(|error| to_pyvalue_err(error.to_string()))
}

#[expect(clippy::needless_pass_by_value)]
fn extract_exec_factory(
    py: Python<'_>,
    value: Py<PyAny>,
) -> PyResult<Box<dyn ExecutionClientFactory>> {
    value
        .extract::<PredictFunExecutionClientFactory>(py)
        .map(|factory| Box::new(factory) as Box<dyn ExecutionClientFactory>)
        .map_err(|error| to_pyvalue_err(error.to_string()))
}

#[expect(clippy::needless_pass_by_value)]
fn extract_data_config(py: Python<'_>, value: Py<PyAny>) -> PyResult<Box<dyn ClientConfig>> {
    value
        .extract::<PredictFunDataClientConfig>(py)
        .map(|config| Box::new(config) as Box<dyn ClientConfig>)
        .map_err(|error| to_pyvalue_err(error.to_string()))
}

#[expect(clippy::needless_pass_by_value)]
fn extract_exec_config(py: Python<'_>, value: Py<PyAny>) -> PyResult<Box<dyn ClientConfig>> {
    value
        .extract::<PredictFunExecClientConfig>(py)
        .map(|config| Box::new(config) as Box<dyn ClientConfig>)
        .map_err(|error| to_pyvalue_err(error.to_string()))
}

#[pymodule]
pub fn predictfun(_: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add(stringify!(PREDICTFUN), PREDICTFUN)?;
    module.add(stringify!(PREDICTFUN_VENUE), *PREDICTFUN_VENUE)?;
    module.add_class::<PredictFunDataClientConfig>()?;
    module.add_class::<PredictFunExecClientConfig>()?;
    module.add_class::<PredictFunDataClientFactory>()?;
    module.add_class::<PredictFunExecutionClientFactory>()?;

    let registry = get_global_pyo3_registry();
    registry
        .register_factory_extractor(PREDICTFUN.to_string(), extract_data_factory)
        .map_err(|error| to_pyruntime_err(error.to_string()))?;
    registry
        .register_exec_factory_extractor(PREDICTFUN.to_string(), extract_exec_factory)
        .map_err(|error| to_pyruntime_err(error.to_string()))?;
    registry
        .register_config_extractor(
            "PredictFunDataClientConfig".to_string(),
            extract_data_config,
        )
        .map_err(|error| to_pyruntime_err(error.to_string()))?;
    registry
        .register_config_extractor(
            "PredictFunExecClientConfig".to_string(),
            extract_exec_config,
        )
        .map_err(|error| to_pyruntime_err(error.to_string()))?;
    Ok(())
}
