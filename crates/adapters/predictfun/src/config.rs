use std::{collections::HashMap, fmt};

use nautilus_model::identifiers::{AccountId, TraderId};
use nautilus_network::websocket::TransportBackend;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use zeroize::Zeroizing;

use crate::common::{
    consts::{PREDICTFUN_API_BASE, PREDICTFUN_TESTNET_API_BASE, PREDICTFUN_WS_URL},
    enums::{PredictFunAccountType, PredictFunEnvironment},
};

#[derive(Clone)]
pub struct SecretString(Zeroizing<String>);

impl SecretString {
    pub fn new(value: String) -> anyhow::Result<Self> {
        if value.trim().is_empty() {
            anyhow::bail!("secret cannot be empty");
        }
        Ok(Self(Zeroizing::new(value)))
    }

    pub fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

impl Serialize for SecretString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.expose())
    }
}

impl<'de> Deserialize<'de> for SecretString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.predictfun", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.predictfun")
)]
#[serde(default, deny_unknown_fields)]
pub struct PredictFunDataClientConfig {
    #[builder(default = PredictFunEnvironment::Mainnet)]
    pub environment: PredictFunEnvironment,
    pub api_key: Option<SecretString>,
    pub api_url: Option<String>,
    pub websocket_url: Option<String>,
    /// Optional server-side `/markets` filters used during instrument discovery.
    pub market_filters: Option<HashMap<String, String>>,
    /// Interval in minutes for reloading instruments and publishing newly discovered markets.
    /// Disabled when `None` or zero.
    pub update_instruments_interval_mins: Option<u64>,
    #[builder(default = 60)]
    pub request_timeout_secs: u64,
    #[builder(default)]
    pub transport_backend: TransportBackend,
}

impl Default for PredictFunDataClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl PredictFunDataClientConfig {
    pub fn api_url(&self) -> &str {
        self.api_url.as_deref().unwrap_or(match self.environment {
            PredictFunEnvironment::Mainnet => PREDICTFUN_API_BASE,
            PredictFunEnvironment::Testnet => PREDICTFUN_TESTNET_API_BASE,
        })
    }

    pub fn websocket_url(&self) -> anyhow::Result<&str> {
        if let Some(url) = self.websocket_url.as_deref() {
            return Ok(url);
        }
        match self.environment {
            PredictFunEnvironment::Mainnet => Ok(PREDICTFUN_WS_URL),
            PredictFunEnvironment::Testnet => {
                anyhow::bail!("testnet websocket_url must be configured explicitly")
            }
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.environment == PredictFunEnvironment::Mainnet && self.api_key.is_none() {
            anyhow::bail!("mainnet data requires api_key");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.predictfun", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.predictfun")
)]
#[serde(default, deny_unknown_fields)]
pub struct PredictFunExecClientConfig {
    #[builder(default = TraderId::from("TRADER-001"))]
    pub trader_id: TraderId,
    #[builder(default = AccountId::from("PREDICTFUN-001"))]
    pub account_id: AccountId,
    #[builder(default = PredictFunEnvironment::Testnet)]
    pub environment: PredictFunEnvironment,
    pub api_key: Option<SecretString>,
    pub private_key: Option<SecretString>,
    pub account_address: Option<String>,
    #[builder(default = PredictFunAccountType::Eoa)]
    pub account_type: PredictFunAccountType,
    pub api_url: Option<String>,
    pub websocket_url: Option<String>,
    /// BNB Chain JSON-RPC endpoint used for authoritative on-chain cancellation.
    pub rpc_url: Option<SecretString>,
    #[builder(default = 50)]
    pub market_slippage_bps: u32,
    #[builder(default = 60)]
    pub request_timeout_secs: u64,
    #[builder(default)]
    pub transport_backend: TransportBackend,
}

impl Default for PredictFunExecClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl PredictFunExecClientConfig {
    pub fn api_url(&self) -> &str {
        self.api_url.as_deref().unwrap_or(match self.environment {
            PredictFunEnvironment::Mainnet => PREDICTFUN_API_BASE,
            PredictFunEnvironment::Testnet => PREDICTFUN_TESTNET_API_BASE,
        })
    }

    pub fn websocket_url(&self) -> anyhow::Result<&str> {
        if let Some(url) = self.websocket_url.as_deref() {
            return Ok(url);
        }
        match self.environment {
            PredictFunEnvironment::Mainnet => Ok(PREDICTFUN_WS_URL),
            PredictFunEnvironment::Testnet => {
                anyhow::bail!("testnet websocket_url must be configured explicitly")
            }
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.market_slippage_bps > 10_000 {
            anyhow::bail!("market_slippage_bps cannot exceed 10000");
        }
        if self.environment == PredictFunEnvironment::Mainnet && self.api_key.is_none() {
            anyhow::bail!("mainnet execution requires api_key");
        }
        if self.private_key.is_none() {
            anyhow::bail!("execution requires private_key");
        }
        if self.rpc_url.is_none() {
            anyhow::bail!("execution requires rpc_url for authoritative cancellation");
        }
        if self.account_type == PredictFunAccountType::PredictAccount
            && self.account_address.is_none()
        {
            anyhow::bail!("Predict account execution requires account_address");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_are_redacted_from_debug() {
        let config = PredictFunExecClientConfig::builder()
            .api_key(SecretString::new("api-secret".to_string()).unwrap())
            .private_key(SecretString::new("wallet-secret".to_string()).unwrap())
            .rpc_url(SecretString::new("https://rpc.example.test/key".to_string()).unwrap())
            .build();
        let debug = format!("{config:?}");
        assert!(!debug.contains("api-secret"));
        assert!(!debug.contains("wallet-secret"));
        assert!(!debug.contains("rpc.example.test"));
    }

    #[test]
    fn testnet_websocket_is_never_inferred() {
        let config = PredictFunDataClientConfig::builder()
            .environment(PredictFunEnvironment::Testnet)
            .build();
        assert!(config.websocket_url().is_err());
    }

    #[test]
    fn execution_requires_rpc_url() {
        let config = PredictFunExecClientConfig::builder()
            .api_key(SecretString::new("api-secret".to_string()).unwrap())
            .private_key(SecretString::new("wallet-secret".to_string()).unwrap())
            .build();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("rpc_url")
        );
    }

    #[test]
    fn testnet_does_not_require_api_key() {
        let data = PredictFunDataClientConfig::builder()
            .environment(PredictFunEnvironment::Testnet)
            .build();
        assert!(data.validate().is_ok());

        let exec = PredictFunExecClientConfig::builder()
            .environment(PredictFunEnvironment::Testnet)
            .private_key(SecretString::new("wallet-secret".to_string()).unwrap())
            .rpc_url(SecretString::new("https://rpc.example.test".to_string()).unwrap())
            .build();
        assert!(exec.validate().is_ok());
    }

    #[test]
    fn mainnet_requires_api_key() {
        let data = PredictFunDataClientConfig::builder()
            .environment(PredictFunEnvironment::Mainnet)
            .build();
        assert!(data.validate().is_err());

        let exec = PredictFunExecClientConfig::builder()
            .environment(PredictFunEnvironment::Mainnet)
            .private_key(SecretString::new("wallet-secret".to_string()).unwrap())
            .rpc_url(SecretString::new("https://rpc.example.test".to_string()).unwrap())
            .build();
        assert!(exec.validate().is_err());
    }
}
