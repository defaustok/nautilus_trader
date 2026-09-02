//! Deposit Wallet transport for Polymarket's gasless Builder Relayer.
//!
//! CLOB orders remain owned by Nautilus. This module only covers the CTF calls which are absent
//! from Nautilus' execution model. Secrets are read by the live binary and never serialized into
//! runner configuration or the event store.

use std::{
    collections::BTreeMap,
    env,
    fmt::{Debug, Formatter},
    fs::{self, File, OpenOptions},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    common::{credential::Credential, enums::SignatureType},
    http::{
        clob::PolymarketClobHttpClient,
        data_api::PolymarketDataApiHttpClient,
        models::DataApiPosition,
        query::{AssetType, GetBalanceAllowanceParams, GetOrdersParams},
    },
};
use alloy::{
    signers::{SignerSync, local::PrivateKeySigner},
    sol_types::{SolCall, SolStruct, eip712_domain},
};
use alloy_primitives::{Address, B256, Bytes, U256, address, hex, keccak256};
use anyhow::{Context, anyhow, bail, ensure};
use nautilus_network::websocket::proxy::ProxyUrl;
use reqwest::{Client, StatusCode};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use nautilus_core::UnixNanos;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
enum CtfOperation {
    Split {
        condition_id: String,
        quantity: Decimal,
        neg_risk: bool,
    },
    Merge {
        condition_id: String,
        quantity: Decimal,
        neg_risk: bool,
    },
    Redeem {
        condition_id: String,
        neg_risk: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CtfCommand {
    command_id: String,
    lifecycle_id: String,
    operation: CtfOperation,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct CtfBalances {
    pusd: Decimal,
    up: Decimal,
    down: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CtfReceipt {
    tx_hash: String,
    balances: CtfBalances,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CtfPollResult {
    Pending,
    Completed(CtfReceipt),
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
enum CtfTransportError {
    #[error("retryable lifecycle transport failure: {0}")]
    Retryable(String),
    #[error("lifecycle operation failed: {0}")]
    Failed(String),
    #[error("lifecycle operation outcome is ambiguous: {0}")]
    Ambiguous(String),
}

/// Strategy-neutral values needed to derive a live relayer market context.
pub trait RelayerMarketSpec {
    fn condition_id(&self) -> &str;
    fn up_instrument_id(&self) -> String;
    fn down_instrument_id(&self) -> String;
    fn neg_risk(&self) -> anyhow::Result<bool>;
    fn planned_split_quantity(&self) -> anyhow::Result<Decimal>;
}

const POLYGON_CHAIN_ID: u64 = 137;
const PUSD_DECIMALS: u32 = 6;
const DEFAULT_RELAYER_URL: &str = "https://relayer-v2.polymarket.com";
const DEFAULT_CLOB_URL: &str = "https://clob.polymarket.com";
const DEFAULT_DATA_API_URL: &str = "https://data-api.polymarket.com";
const DEFAULT_GEO_BLOCK_URL: &str = "https://polymarket.com/api/geoblock";
#[cfg_attr(test, allow(dead_code))]
const WALLET_LOCK_DIR: &str = "defaust-polymarket-wallet-locks";
const DEFAULT_POLL_INTERVAL_MS: u64 = 2_000;
const DEFAULT_MAX_POLLS: u32 = 60;
// Polymarket's official Deposit Wallet example uses a four-minute expiry. Add one minute of
// transport/signing margin so the deadline is still at least four minutes away when the Relayer
// validates it; shorter horizons are rejected before a tx ID is assigned.
const DEFAULT_DEADLINE_SECS: u64 = 300;
const HTTP_CONNECT_TIMEOUT_SECS: u64 = 5;
const HTTP_REQUEST_TIMEOUT_SECS: u64 = 10;
const REQUIRED_CONFIRMATIONS: u64 = 2;

const PUSD: Address = address!("0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB");
const CONDITIONAL_TOKENS: Address = address!("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045");
// https://docs.polymarket.com/resources/contracts is the address source of truth.
const CTF_COLLATERAL_ADAPTER: Address = address!("0xAdA100Db00Ca00073811820692005400218FcE1f");
const NEG_RISK_CTF_COLLATERAL_ADAPTER: Address =
    address!("0xadA2005600Dec949baf300f4C6120000bDB6eAab");
const CTF_EXCHANGE: Address = address!("0xE111180000d2663C0091e4f400237545B87B996B");
const NEG_RISK_CTF_EXCHANGE: Address = address!("0xe2222d279d744050d28e00520010520000310F59");
const DEPOSIT_WALLET_FACTORY: Address = address!("0x00000000000Fb5C9ADea0298D729A0CB3823Cc07");
const DEPOSIT_WALLET_BEACON: Address = address!("0x7A18EDfe055488A3128f01F563e5B479D92ffc3a");
// Solady v0.1.26 `LibClone.initCodeHashERC1967Beacon` constants, mirrored from Polymarket's
// official builder-relayer-client `derive.ts`.
const ERC1967_BEACON_PREFIX: [u8; 10] =
    [0x61, 0x00, 0x52, 0x3d, 0x81, 0x60, 0x23, 0x3d, 0x39, 0x73];
const ERC1967_BEACON_CONST1: [u8; 32] =
    hex!("b3582b35133d50545afa5036515af43d6000803e604d573d6000fd5b3d6000f3");
const ERC1967_BEACON_CONST2: [u8; 32] =
    hex!("1b60e01b36527fa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6c");
const ERC1967_BEACON_CONST3: [u8; 23] = hex!("60195155f3363d3d373d3d363d602036600436635c60da");

alloy::sol! {
    // EIP-712 struct names are consensus-critical and must match the documented Deposit Wallet
    // schema exactly. Renaming either Rust-side Solidity struct changes the type hash.
    struct Call {
        address target;
        uint256 value;
        bytes data;
    }

    struct Batch {
        address wallet;
        uint256 nonce;
        uint256 deadline;
        Call[] calls;
    }

    function splitPosition(
        address collateralToken,
        bytes32 parentCollectionId,
        bytes32 conditionId,
        uint256[] partition,
        uint256 amount
    );

    function mergePositions(
        address collateralToken,
        bytes32 parentCollectionId,
        bytes32 conditionId,
        uint256[] partition,
        uint256 amount
    );

    function redeemPositions(
        address collateralToken,
        bytes32 parentCollectionId,
        bytes32 conditionId,
        uint256[] indexSets
    );

    function balanceOf(address account) external view returns (uint256);
    function allowance(address owner, address spender) external view returns (uint256);
    function balanceOf(address account, uint256 id) external view returns (uint256);
    function isApprovedForAll(address account, address operator) external view returns (bool);
    function approve(address spender, uint256 amount) external returns (bool);
    function setApprovalForAll(address operator, bool approved);
}

/// Token identifiers and collateral-adapter routing for one market lifecycle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelayerMarketContext {
    pub condition_id: B256,
    pub up_token_id: U256,
    pub down_token_id: U256,
    pub neg_risk: bool,
    pub planned_split_quantity: Decimal,
}

impl RelayerMarketContext {
    /// Builds a wallet context from the exact token IDs carried by native Polymarket instruments.
    pub fn from_market(market: &impl RelayerMarketSpec) -> anyhow::Result<Self> {
        let condition_id = B256::from_str(market.condition_id())
            .with_context(|| format!("invalid condition id {}", market.condition_id()))?;
        let up_token_id = token_id_from_instrument(&market.up_instrument_id())?;
        let down_token_id = token_id_from_instrument(&market.down_instrument_id())?;
        let planned_split_quantity = market.planned_split_quantity()?;
        Ok(Self {
            condition_id,
            up_token_id,
            down_token_id,
            neg_risk: market.neg_risk()?,
            planned_split_quantity,
        })
    }

    /// Compatibility constructor retained for existing strategy crates.
    pub fn from_strategy(config: &impl RelayerMarketSpec) -> anyhow::Result<Self> {
        Self::from_market(config)
    }
}

/// Live-only secrets and endpoints. Debug output intentionally redacts all credentials.
#[derive(Clone)]
pub struct RelayerConfig {
    pub private_key: String,
    pub wallet_address: Address,
    pub relayer_api_key: String,
    pub relayer_api_key_address: Address,
    pub polygon_rpc_url: String,
    pub relayer_url: String,
    pub clob_url: String,
    pub data_api_url: String,
    pub geoblock_url: String,
    /// Optional credential-bearing HTTP(S) proxy shared by every live transport.
    pub proxy_url: Option<String>,
    pub clob_api_key: String,
    pub clob_api_secret: String,
    pub clob_passphrase: String,
    pub poll_interval: Duration,
    pub max_polls: u32,
    pub deadline: Duration,
    /// Canonical CTF wallet exposure ceiling derived by LiveRunner from the strategy bundle.
    /// It is deliberately not an environment setting, so secret provisioning cannot silently
    /// change live risk.
    pub max_outstanding_pusd: Option<Decimal>,
}

impl Debug for RelayerConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RelayerConfig")
            .field("private_key", &"***")
            .field("wallet_address", &self.wallet_address)
            .field("relayer_api_key", &"***")
            .field("relayer_api_key_address", &self.relayer_api_key_address)
            .field("polygon_rpc_url", &"***")
            .field("relayer_url", &self.relayer_url)
            .field("clob_url", &self.clob_url)
            .field("data_api_url", &self.data_api_url)
            .field("geoblock_url", &self.geoblock_url)
            .field("proxy_url", &self.proxy_url.as_ref().map(|_| "***"))
            .field("clob_l2_credentials", &"***")
            .field("poll_interval", &self.poll_interval)
            .field("max_polls", &self.max_polls)
            .field("deadline", &self.deadline)
            .field("max_outstanding_pusd", &self.max_outstanding_pusd)
            .finish()
    }
}

impl RelayerConfig {
    /// Loads the secret-bearing live boundary. No environment value is logged on failure.
    ///
    /// `POLYMARKET_PK` is the canonical private-key setting used by Nautilus' CLOB adapter.
    /// The legacy relayer name is accepted only when it identifies exactly the same EOA, so a
    /// split/merge signer can never diverge from the order signer.
    pub fn from_env() -> anyhow::Result<Self> {
        let private_key = required_env("POLYMARKET_PK")?;
        if let Ok(legacy_private_key) = env::var("POLYMARKET_PRIVATE_KEY") {
            ensure_same_private_key(&private_key, &legacy_private_key)?;
        }
        // Resolve these at the live boundary even though Nautilus owns CLOB request signing. It
        // prevents a Relayer-only configuration from passing preflight and then failing after a
        // complete-set split has already been submitted.
        let clob_api_key = required_env("POLYMARKET_API_KEY")?;
        let clob_api_secret = required_env("POLYMARKET_API_SECRET")?;
        let clob_passphrase = required_env("POLYMARKET_PASSPHRASE")?;
        let wallet_address = parse_env_address("POLYMARKET_FUNDER")?;
        let relayer_api_key = required_env("POLYMARKET_RELAYER_API_KEY")?;
        let relayer_api_key_address = parse_env_address("POLYMARKET_RELAYER_API_KEY_ADDRESS")?;
        let polygon_rpc_url = required_env("POLYGON_RPC_URL")?;
        let relayer_url = env::var("POLYMARKET_RELAYER_URL")
            .unwrap_or_else(|_| DEFAULT_RELAYER_URL.to_string())
            .trim_end_matches('/')
            .to_string();
        let clob_url = env::var("POLYMARKET_CLOB_URL")
            .unwrap_or_else(|_| DEFAULT_CLOB_URL.to_string())
            .trim_end_matches('/')
            .to_string();
        let data_api_url = env::var("POLYMARKET_DATA_API_URL")
            .unwrap_or_else(|_| DEFAULT_DATA_API_URL.to_string())
            .trim_end_matches('/')
            .to_string();
        let geoblock_url = env::var("POLYMARKET_GEOBLOCK_URL")
            .unwrap_or_else(|_| DEFAULT_GEO_BLOCK_URL.to_string());
        let proxy_url = env::var("POLYMARKET_PROXY_URL")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if let Some(value) = &proxy_url {
            ProxyUrl::parse(value.clone())
                .map_err(|_| anyhow!("POLYMARKET_PROXY_URL is not a valid HTTP(S) proxy"))?;
        }
        ensure!(
            relayer_url.starts_with("https://"),
            "POLYMARKET_RELAYER_URL must use HTTPS"
        );
        ensure!(
            polygon_rpc_url.starts_with("https://"),
            "POLYGON_RPC_URL must use HTTPS"
        );
        ensure!(
            clob_url.starts_with("https://"),
            "POLYMARKET_CLOB_URL must use HTTPS"
        );
        ensure!(
            data_api_url.starts_with("https://"),
            "POLYMARKET_DATA_API_URL must use HTTPS"
        );
        ensure!(
            geoblock_url.starts_with("https://"),
            "POLYMARKET_GEOBLOCK_URL must use HTTPS"
        );
        Ok(Self {
            private_key,
            wallet_address,
            relayer_api_key,
            relayer_api_key_address,
            polygon_rpc_url,
            relayer_url,
            clob_url,
            data_api_url,
            geoblock_url,
            proxy_url,
            clob_api_key,
            clob_api_secret,
            clob_passphrase,
            poll_interval: Duration::from_millis(DEFAULT_POLL_INTERVAL_MS),
            max_polls: DEFAULT_MAX_POLLS,
            deadline: Duration::from_secs(DEFAULT_DEADLINE_SECS),
            max_outstanding_pusd: None,
        })
    }

    /// Injects the immutable bundle-derived outstanding-collateral cap at the live boundary.
    pub fn with_max_outstanding_pusd(
        mut self,
        max_outstanding_pusd: Decimal,
    ) -> anyhow::Result<Self> {
        ensure!(
            max_outstanding_pusd > Decimal::ZERO,
            "max outstanding pUSD must be positive"
        );
        self.max_outstanding_pusd = Some(max_outstanding_pusd);
        Ok(self)
    }

    fn required_wallet_cap(&self) -> anyhow::Result<Decimal> {
        self.max_outstanding_pusd.ok_or_else(|| {
            anyhow!(
                "live Relayer requires a bundle-derived max outstanding pUSD cap; refusing implicit exposure"
            )
        })
    }
}

/// Native Rust transport for Deposit Wallet split/merge/redeem operations.
pub struct RelayerCtfTransport {
    config: RelayerConfig,
    signer: PrivateKeySigner,
    client: Client,
    clob_client: PolymarketClobHttpClient,
    data_api_client: PolymarketDataApiHttpClient,
    // Held from the first preflight or submit until transport drop. This is deliberately an OS
    // lock (rather than a process-global registry) because multiple native nodes can otherwise
    // submit from the same Deposit Wallet.
    wallet_lock: Mutex<Option<WalletLock>>,
    markets: BTreeMap<String, RelayerMarketContext>,
}

#[derive(Debug)]
struct WalletLock {
    _file: File,
}

pub(super) fn derive_beacon_deposit_wallet(owner: Address) -> Address {
    // abi.encode(factory, bytes32(owner)): both values are full 32-byte ABI words.
    let mut args = Vec::with_capacity(64);
    args.extend_from_slice(&[0_u8; 12]);
    args.extend_from_slice(DEPOSIT_WALLET_FACTORY.as_slice());
    args.extend_from_slice(&[0_u8; 12]);
    args.extend_from_slice(owner.as_slice());
    let salt = keccak256(&args);

    // The published reference computes `PREFIX + (args.length << 56)`.  This is an addition to
    // the ten-byte word -- not a replacement of the PUSH2 immediate.  For the canonical 64-byte
    // ABI argument block it therefore produces `0x6100923d8160233d3973`.
    let args_len = u8::try_from(args.len()).expect("Deposit Wallet ABI args fit u8");
    let mut prefix = ERC1967_BEACON_PREFIX;
    prefix[2] = prefix[2]
        .checked_add(args_len)
        .expect("Deposit Wallet ABI argument length does not overflow prefix");
    let mut init_code = Vec::with_capacity(
        prefix.len()
            + DEPOSIT_WALLET_BEACON.as_slice().len()
            + ERC1967_BEACON_CONST3.len()
            + ERC1967_BEACON_CONST2.len()
            + ERC1967_BEACON_CONST1.len()
            + args.len(),
    );
    init_code.extend_from_slice(&prefix);
    init_code.extend_from_slice(DEPOSIT_WALLET_BEACON.as_slice());
    init_code.extend_from_slice(&ERC1967_BEACON_CONST3);
    init_code.extend_from_slice(&ERC1967_BEACON_CONST2);
    init_code.extend_from_slice(&ERC1967_BEACON_CONST1);
    init_code.extend_from_slice(&args);
    let init_code_hash = keccak256(init_code);

    let mut create2 = Vec::with_capacity(1 + 20 + 32 + 32);
    create2.push(0xff);
    create2.extend_from_slice(DEPOSIT_WALLET_FACTORY.as_slice());
    create2.extend_from_slice(salt.as_slice());
    create2.extend_from_slice(init_code_hash.as_slice());
    let hash = keccak256(create2);
    Address::from_slice(&hash.as_slice()[12..])
}

impl Debug for RelayerCtfTransport {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RelayerCtfTransport")
            .field("config", &self.config)
            .field("signer", &"***")
            .field("clob_client", &"***")
            .field(
                "wallet_lock_held",
                &self.wallet_lock.lock().is_ok_and(|lock| lock.is_some()),
            )
            .field("market_count", &self.markets.len())
            .finish()
    }
}

mod agent;
mod approvals;
mod client_1;
mod client_2;
mod wire;

use wire::*;

pub use approvals::{
    APPROVAL_WRITE_CONFIRMATION, PolymarketApprovalPlan, PolymarketApprovalSubmission,
    PreparedPolymarketApprovals,
};

#[cfg(test)]
mod tests;
