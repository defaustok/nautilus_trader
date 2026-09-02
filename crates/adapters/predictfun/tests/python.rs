// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

#![cfg(feature = "python")]

use nautilus_predictfun::{common::consts::PREDICTFUN, python};
use pyo3::{
    Python,
    types::{PyAnyMethods, PyModule, PyStringMethods},
};

#[test]
fn python_module_registers_public_surface_without_exposing_secrets() {
    Python::initialize();

    Python::attach(|py| {
        let module = PyModule::new(py, "predictfun").expect("module should be created");
        python::predictfun(py, &module).expect("module should register");

        assert_eq!(
            module
                .getattr("PREDICTFUN")
                .expect("PREDICTFUN constant")
                .extract::<String>()
                .expect("string constant"),
            PREDICTFUN,
        );
        for name in [
            "PREDICTFUN_VENUE",
            "PredictFunDataClientConfig",
            "PredictFunDataClientFactory",
            "PredictFunExecClientConfig",
            "PredictFunExecutionClientFactory",
        ] {
            assert!(
                module.getattr(name).is_ok(),
                "missing Python export: {name}"
            );
        }
        let data_config = module
            .getattr("PredictFunDataClientConfig")
            .expect("data config")
            .call0()
            .expect("data config should construct without execution credentials");
        let representation = data_config
            .repr()
            .expect("config repr")
            .to_string_lossy()
            .into_owned();
        assert!(!representation.contains("api-secret"));
    });
}
