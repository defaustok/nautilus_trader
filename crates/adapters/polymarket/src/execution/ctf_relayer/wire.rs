//! Relayer wire models, validation, numeric conversion, and wallet locking.

use super::*;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct DepositWalletBatchRequest {
    #[serde(rename = "type")]
    pub(super) transaction_type: String,
    pub(super) from: Address,
    pub(super) to: Address,
    pub(super) nonce: String,
    pub(super) signature: String,
    pub(super) metadata: String,
    pub(super) deposit_wallet_params: DepositWalletParams,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct DepositWalletParams {
    pub(super) deposit_wallet: Address,
    pub(super) deadline: String,
    pub(super) calls: Vec<DepositWalletCallJson>,
}

#[derive(Debug, Serialize)]
pub(super) struct DepositWalletCallJson {
    pub(super) target: Address,
    pub(super) value: String,
    pub(super) data: String,
}

impl From<Call> for DepositWalletCallJson {
    fn from(call: Call) -> Self {
        Self {
            target: call.target,
            value: call.value.to_string(),
            data: format!("0x{}", hex::encode(call.data)),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct NonceResponse {
    #[serde(rename = "address")]
    pub(super) relayer_address: Address,
    pub(super) nonce: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct SubmitResponse {
    #[serde(rename = "transactionID")]
    pub(super) transaction_id: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct RelayerTransaction {
    pub(super) transaction_id: String,
    #[serde(default)]
    pub(super) transaction_hash: Option<String>,
    pub(super) state: String,
    #[serde(default)]
    pub(super) error_msg: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct GeoBlockResponse {
    pub(super) blocked: bool,
}

#[derive(Debug, Deserialize)]
pub(super) struct RpcResponse {
    pub(super) result: Option<serde_json::Value>,
    pub(super) error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
pub(super) struct RpcError {
    pub(super) code: i64,
    pub(super) message: String,
}

pub(super) fn adapter_for(context: &RelayerMarketContext) -> Address {
    if context.neg_risk {
        NEG_RISK_CTF_COLLATERAL_ADAPTER
    } else {
        CTF_COLLATERAL_ADAPTER
    }
}

pub(super) fn exchange_for(context: &RelayerMarketContext) -> Address {
    if context.neg_risk {
        NEG_RISK_CTF_EXCHANGE
    } else {
        CTF_EXCHANGE
    }
}

pub(super) fn validate_command_context(
    condition_id: &str,
    neg_risk: bool,
    context: &RelayerMarketContext,
) -> anyhow::Result<()> {
    ensure!(
        condition_id.to_ascii_lowercase() == format!("{:#x}", context.condition_id),
        "command condition does not match relayer context"
    );
    ensure!(
        neg_risk == context.neg_risk,
        "command negRisk does not match causal market metadata"
    );
    Ok(())
}

pub(super) fn token_id_from_instrument(instrument_id: &str) -> anyhow::Result<U256> {
    let symbol = instrument_id
        .strip_suffix(".POLYMARKET")
        .ok_or_else(|| anyhow!("instrument is not a Polymarket instrument"))?;
    let (_, token) = symbol
        .split_once('-')
        .ok_or_else(|| anyhow!("Polymarket instrument does not contain a token ID"))?;
    U256::from_str(token).context("Polymarket token ID is not a uint256")
}

pub(super) fn decimal_to_atomic(value: Decimal) -> anyhow::Result<U256> {
    ensure!(value > Decimal::ZERO, "CTF quantity must be positive");
    let factor = Decimal::from(10_u64.pow(PUSD_DECIMALS));
    let atomic = value * factor;
    ensure!(
        atomic.fract().is_zero(),
        "CTF quantity has sub-atomic precision"
    );
    U256::from_str(&atomic.trunc().to_string()).context("CTF quantity does not fit uint256")
}

pub(super) fn atomic_to_decimal(value: U256) -> anyhow::Result<Decimal> {
    let atomic = Decimal::from_str(&value.to_string()).context("balance exceeds Decimal range")?;
    Ok(atomic / Decimal::from(10_u64.pow(PUSD_DECIMALS)))
}

pub(super) fn verify_balance_delta(
    command: &CtfCommand,
    before: &CtfBalances,
    after: &CtfBalances,
) -> Result<(), CtfTransportError> {
    match &command.operation {
        CtfOperation::Split { quantity, .. } => {
            let expected_up = before.up + *quantity;
            let expected_down = before.down + *quantity;
            if after.up != expected_up || after.down != expected_down {
                return Err(CtfTransportError::Ambiguous(format!(
                    "split balance delta mismatch: expected UP={expected_up} DOWN={expected_down}, got UP={} DOWN={}",
                    after.up, after.down
                )));
            }
            if after.pusd > before.pusd - *quantity {
                return Err(CtfTransportError::Ambiguous(format!(
                    "split pUSD was not debited by {quantity}: before={} after={}",
                    before.pusd, after.pusd
                )));
            }
        }
        CtfOperation::Merge { quantity, .. } => {
            let expected_up = before.up - *quantity;
            let expected_down = before.down - *quantity;
            if after.up != expected_up || after.down != expected_down {
                return Err(CtfTransportError::Ambiguous(format!(
                    "merge balance delta mismatch: expected UP={expected_up} DOWN={expected_down}, got UP={} DOWN={}",
                    after.up, after.down
                )));
            }
            if after.pusd < before.pusd + *quantity {
                return Err(CtfTransportError::Ambiguous(format!(
                    "merge pUSD was not credited by {quantity}: before={} after={}",
                    before.pusd, after.pusd
                )));
            }
        }
        CtfOperation::Redeem { .. } => {
            let outcome_decreased = after.up < before.up || after.down < before.down;
            // A resolved losing token is still redeemable: the adapter burns it but returns zero
            // pUSD.  Therefore a verified cleanup requires a strict outcome-token decrease and
            // forbids any collateral debit; it cannot require a positive payout.  Winning-token
            // redemptions naturally satisfy the same invariant with an increased pUSD balance.
            if after.up > before.up
                || after.down > before.down
                || !outcome_decreased
                || after.pusd < before.pusd
            {
                return Err(CtfTransportError::Ambiguous(format!(
                    "redeem balance delta is not a verified burn/payout: before={before:?}, after={after:?}"
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn clob_refresh_params(
    command: &CtfCommand,
    context: &RelayerMarketContext,
) -> Vec<GetBalanceAllowanceParams> {
    let signature_type = Some(SignatureType::Poly1271);
    let mut refreshes = vec![GetBalanceAllowanceParams {
        asset_type: Some(AssetType::Collateral),
        signature_type,
        ..Default::default()
    }];
    match &command.operation {
        CtfOperation::Split { .. } | CtfOperation::Merge { .. } | CtfOperation::Redeem { .. } => {
            refreshes.push(GetBalanceAllowanceParams {
                asset_type: Some(AssetType::Conditional),
                token_id: Some(context.up_token_id.to_string()),
                signature_type,
            });
            refreshes.push(GetBalanceAllowanceParams {
                asset_type: Some(AssetType::Conditional),
                token_id: Some(context.down_token_id.to_string()),
                signature_type,
            });
        }
    }
    refreshes
}

/// Verifies that a Data API response cannot conceal unrelated CTF inventory under the live
/// wallet.  A binary condition's collateral exposure is the larger of its two outcome balances;
/// summing that maximum across configured conditions is conservative even when a prior process
/// left an imbalanced leg behind.
pub(super) fn validate_wallet_positions(
    positions: &[DataApiPosition],
    markets: &BTreeMap<String, RelayerMarketContext>,
    wallet_cap: Decimal,
) -> anyhow::Result<()> {
    let mut outcome_sizes = BTreeMap::<String, (Decimal, Decimal)>::new();
    for position in positions {
        let size = position.size.abs();
        if size.is_zero() {
            continue;
        }
        let condition_id = position.condition_id.to_ascii_lowercase();
        let context = markets.get(&condition_id).ok_or_else(|| {
            anyhow!(
                "nonzero CTF position for condition {} is outside the configured live bundle",
                position.condition_id
            )
        })?;
        let outcome_sizes = outcome_sizes.entry(condition_id).or_default();
        if position.asset == context.up_token_id.to_string() {
            outcome_sizes.0 += size;
        } else if position.asset == context.down_token_id.to_string() {
            outcome_sizes.1 += size;
        } else {
            bail!(
                "nonzero CTF position token {} is not one of the configured outcomes for condition {}",
                position.asset,
                position.condition_id
            );
        }
    }
    let outstanding = outcome_sizes
        .into_values()
        .map(|(up, down)| up.max(down))
        .sum::<Decimal>();
    ensure!(
        outstanding <= wallet_cap,
        "wallet CTF exposure {outstanding} exceeds live wallet cap {wallet_cap}"
    );
    Ok(())
}

pub(super) fn validate_open_order_scope(
    market: &str,
    asset_id: &str,
    markets: &BTreeMap<String, RelayerMarketContext>,
) -> anyhow::Result<()> {
    let condition_id = market.to_ascii_lowercase();
    let context = markets.get(&condition_id).ok_or_else(|| {
        anyhow!("open CLOB order market {market} is outside the configured live bundle")
    })?;
    ensure!(
        asset_id == context.up_token_id.to_string()
            || asset_id == context.down_token_id.to_string(),
        "open CLOB order asset {asset_id} is outside configured outcomes for condition {market}"
    );
    Ok(())
}

pub(super) fn parse_split_geoblock_response(bytes: &[u8]) -> Result<(), CtfTransportError> {
    let response = serde_json::from_slice::<GeoBlockResponse>(bytes).map_err(|error| {
        CtfTransportError::Failed(format!(
            "Polymarket geoblock response was malformed before split submission: {error}"
        ))
    })?;
    if response.blocked {
        return Err(CtfTransportError::Failed(
            "Polymarket geoblock is active; split submission is forbidden".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn operation_metadata(command: &CtfCommand) -> String {
    let operation = match &command.operation {
        CtfOperation::Split { .. } => "split",
        CtfOperation::Merge { .. } => "merge",
        CtfOperation::Redeem { .. } => "redeem",
    };
    format!("polymarket-split:{operation}:{}", command.command_id)
}

pub(super) fn parse_hex_u64(value: &str) -> anyhow::Result<u64> {
    u64::from_str_radix(value.trim_start_matches("0x"), 16)
        .with_context(|| format!("{value} is not a hexadecimal uint64"))
}

pub(super) fn ensure_same_private_key(primary: &str, legacy: &str) -> anyhow::Result<()> {
    let primary = PrivateKeySigner::from_str(primary.trim_start_matches("0x"))
        .context("POLYMARKET_PK is invalid")?;
    let legacy = PrivateKeySigner::from_str(legacy.trim_start_matches("0x"))
        .context("POLYMARKET_PRIVATE_KEY is invalid")?;
    ensure!(
        primary.address() == legacy.address(),
        "POLYMARKET_PK and POLYMARKET_PRIVATE_KEY identify different EOA signers"
    );
    Ok(())
}

pub(super) fn unix_seconds() -> anyhow::Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

pub(super) fn required_env(name: &'static str) -> anyhow::Result<String> {
    let value = env::var(name)
        .with_context(|| format!("required environment variable {name} is missing"))?;
    ensure!(
        !value.trim().is_empty(),
        "required environment variable {name} is empty"
    );
    Ok(value)
}

#[cfg_attr(test, allow(dead_code))]
pub(super) fn acquire_wallet_lock(funder: Address) -> anyhow::Result<WalletLock> {
    let runtime = env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("XDG_RUNTIME_DIR is required for the live Deposit Wallet lock"))?;
    ensure!(
        runtime.is_absolute(),
        "XDG_RUNTIME_DIR must be an absolute path for the live Deposit Wallet lock"
    );
    acquire_wallet_lock_in(&runtime, funder)
}

#[cfg_attr(test, allow(dead_code))]
pub(super) fn acquire_wallet_lock_in(
    runtime: &Path,
    funder: Address,
) -> anyhow::Result<WalletLock> {
    ensure_secure_owned_directory(runtime, "XDG_RUNTIME_DIR")?;
    let lock_dir = runtime.join(WALLET_LOCK_DIR);
    match fs::DirBuilder::new().mode(0o700).create(&lock_dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    ensure_secure_owned_directory(&lock_dir, "Deposit Wallet lock directory")?;

    let lock_path = lock_dir.join(format!("{funder:#x}.lock"));
    match fs::symlink_metadata(&lock_path) {
        Ok(metadata) => ensure_secure_owned_lock_file(&metadata, &lock_path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true).mode(0o600);
    let file = options.open(&lock_path)?;
    // `metadata` operates on the acquired descriptor, so a filename race cannot replace a
    // validated regular file after O_NOFOLLOW has resolved it.
    ensure_secure_owned_lock_file(&file.metadata()?, &lock_path)?;
    if let Err(error) = file.try_lock() {
        return Err(error).map_err(|error| {
            anyhow!("Deposit Wallet lock is already held or unavailable for {funder:#x}: {error}")
        });
    }
    File::open(&lock_dir)?.sync_all()?;
    Ok(WalletLock { _file: file })
}

#[cfg_attr(test, allow(dead_code))]
pub(super) fn ensure_secure_owned_directory(path: &Path, label: &str) -> anyhow::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        !metadata.file_type().is_symlink() && metadata.is_dir(),
        "{label} must be a non-symlink directory: {}",
        path.display()
    );
    let effective_uid = fs::metadata("/proc/self")?.uid();
    ensure!(
        metadata.uid() == effective_uid,
        "{label} must be owned by the current user: {}",
        path.display()
    );
    ensure!(
        metadata.mode() & 0o777 == 0o700,
        "{label} must have mode 0700: {}",
        path.display()
    );
    Ok(())
}

#[cfg_attr(test, allow(dead_code))]
pub(super) fn ensure_secure_owned_lock_file(
    metadata: &fs::Metadata,
    path: &Path,
) -> anyhow::Result<()> {
    ensure!(
        !metadata.file_type().is_symlink() && metadata.is_file(),
        "Deposit Wallet lock must be a regular non-symlink file: {}",
        path.display()
    );
    let effective_uid = fs::metadata("/proc/self")?.uid();
    ensure!(
        metadata.uid() == effective_uid,
        "Deposit Wallet lock must be owned by the current user: {}",
        path.display()
    );
    ensure!(
        metadata.mode() & 0o777 == 0o600,
        "Deposit Wallet lock must have mode 0600: {}",
        path.display()
    );
    Ok(())
}

pub(super) fn parse_env_address(name: &'static str) -> anyhow::Result<Address> {
    let value = required_env(name)?;
    Address::from_str(&value).with_context(|| format!("{name} is not an Ethereum address"))
}

/// Converts a read-only pre-submit failure into a safely retryable, credential-free diagnostic.
/// HTTP/RPC client errors can embed complete credential-bearing URLs, so their raw text must
/// never cross the live telemetry boundary.
pub(super) fn retryable_pre_submit(stage: &str, _error: impl ToString) -> CtfTransportError {
    CtfTransportError::Retryable(format!("{stage} failed"))
}
