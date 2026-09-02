// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

use std::{
    str::FromStr,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use alloy_primitives::Signature;
use axum::{
    Json, Router,
    body::Bytes as BodyBytes,
    extract::State,
    http::{Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{any, get},
};
use rust_decimal_macros::dec;
use serde_json::{Value, json};

use super::*;
use crate::execution::agent_lifecycle::{
    PolymarketLifecycleBackend, PolymarketLifecycleBackendError, PolymarketLifecycleOperation,
    PolymarketLifecycleReconciliation,
};

const TEST_KEY: &str = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

fn context() -> RelayerMarketContext {
    RelayerMarketContext {
        condition_id: B256::repeat_byte(0x11),
        up_token_id: U256::from(123),
        down_token_id: U256::from(456),
        neg_risk: false,
        planned_split_quantity: dec!(5),
    }
}

fn transport() -> RelayerCtfTransport {
    let signer = PrivateKeySigner::from_str(TEST_KEY).unwrap();
    RelayerCtfTransport::new(
        RelayerConfig {
            private_key: TEST_KEY.to_string(),
            wallet_address: address!("0x1000000000000000000000000000000000000000"),
            relayer_api_key: "fixture".to_string(),
            relayer_api_key_address: signer.address(),
            polygon_rpc_url: "https://rpc.invalid".to_string(),
            relayer_url: "https://relayer.invalid".to_string(),
            clob_url: "https://clob.invalid".to_string(),
            data_api_url: "https://data.invalid".to_string(),
            geoblock_url: "https://geo.invalid".to_string(),
            proxy_url: None,
            clob_api_key: "fixture".to_string(),
            clob_api_secret: "Zml4dHVyZQ==".to_string(),
            clob_passphrase: "fixture".to_string(),
            poll_interval: Duration::ZERO,
            max_polls: 1,
            deadline: Duration::from_secs(300),
            max_outstanding_pusd: Some(dec!(10)),
        },
        [context()],
    )
    .unwrap()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SubmitMode {
    Success,
    Reject,
    UnreadableSuccess,
}

#[derive(Clone)]
struct SimulatorState {
    submit_mode: SubmitMode,
    approvals_ready: bool,
    confirmed: Arc<AtomicBool>,
    requests: Arc<StdMutex<Vec<(Method, String)>>>,
    submit_count: Arc<AtomicUsize>,
    poll_count: Arc<AtomicUsize>,
}

impl SimulatorState {
    fn new(submit_mode: SubmitMode, approvals_ready: bool) -> Self {
        Self {
            submit_mode,
            approvals_ready,
            confirmed: Arc::new(AtomicBool::new(false)),
            requests: Arc::new(StdMutex::new(Vec::new())),
            submit_count: Arc::new(AtomicUsize::new(0)),
            poll_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

async fn simulator_handler(
    State(state): State<SimulatorState>,
    method: Method,
    uri: Uri,
    body: BodyBytes,
) -> Response {
    let path = uri.path().to_string();
    state
        .requests
        .lock()
        .unwrap()
        .push((method.clone(), path.clone()));

    if method == Method::POST && path == "/" {
        let request: Value = serde_json::from_slice(&body).unwrap();
        let rpc_method = request["method"].as_str().unwrap();
        let result = match rpc_method {
            "eth_getCode" => json!("0x01"),
            "eth_getTransactionReceipt" => json!({"status":"0x1","blockNumber":"0x10"}),
            "eth_blockNumber" => json!("0x11"),
            "eth_call" => {
                let call = &request["params"][0];
                let target = call["to"].as_str().unwrap().to_ascii_lowercase();
                let data = call["data"].as_str().unwrap();
                let is_approval_query = data.starts_with(&format!(
                    "0x{}",
                    hex::encode(
                        &isApprovedForAllCall {
                            account: Address::ZERO,
                            operator: Address::ZERO,
                        }
                        .abi_encode()[..4]
                    )
                ));
                let is_allowance_query = data.starts_with(&format!(
                    "0x{}",
                    hex::encode(
                        &allowanceCall {
                            owner: Address::ZERO,
                            spender: Address::ZERO,
                        }
                        .abi_encode()[..4]
                    )
                ));
                let amount = if is_approval_query {
                    usize::from(state.approvals_ready)
                } else if is_allowance_query {
                    if state.approvals_ready { 10_000_000 } else { 0 }
                } else if target == format!("{PUSD:#x}") {
                    if state.confirmed.load(Ordering::SeqCst) {
                        5_000_000
                    } else {
                        10_000_000
                    }
                } else if state.confirmed.load(Ordering::SeqCst) {
                    5_000_000
                } else {
                    0
                };
                json!(format!("0x{amount:064x}"))
            }
            other => panic!("unexpected RPC method {other}"),
        };
        return Json(json!({"jsonrpc":"2.0","id":1,"result":result})).into_response();
    }
    if method == Method::GET && path == "/balance-allowance" {
        return Json(json!({"balance":"10000000","allowances":{}})).into_response();
    }
    if method == Method::GET && path == "/data/orders" {
        return Json(json!({"data":[],"next_cursor":"LTE="})).into_response();
    }
    if method == Method::GET && path == "/positions" {
        return Json(json!([])).into_response();
    }
    if method == Method::GET && path == "/balance-allowance/update" {
        return StatusCode::NO_CONTENT.into_response();
    }
    if method == Method::GET && path == "/geoblock" {
        return Json(json!({"blocked":false})).into_response();
    }
    if method == Method::GET && path == "/v1/account/transactions/params" {
        return Json(json!({
            "address":"0x7552e88a9fdb6d25b96c1933f33ec21dcf586d0f",
            "nonce":"7"
        }))
        .into_response();
    }
    if method == Method::POST && path == "/submit" {
        state.submit_count.fetch_add(1, Ordering::SeqCst);
        return match state.submit_mode {
            SubmitMode::Success => Json(json!({"transactionID":"fixture-tx"})).into_response(),
            SubmitMode::Reject => (
                StatusCode::BAD_REQUEST,
                Json(json!({"message":"fixture rejection"})),
            )
                .into_response(),
            SubmitMode::UnreadableSuccess => (StatusCode::OK, "not-json").into_response(),
        };
    }
    if method == Method::GET && path == "/v1/account/transactions/fixture-tx" {
        let poll = state.poll_count.fetch_add(1, Ordering::SeqCst);
        if poll == 0 {
            return Json(json!({
                "transaction_id":"fixture-tx",
                "state":"STATE_MINED"
            }))
            .into_response();
        }
        state.confirmed.store(true, Ordering::SeqCst);
        return Json(json!({
            "transaction_id":"fixture-tx",
            "transaction_hash":"0xabc",
            "state":"STATE_CONFIRMED"
        }))
        .into_response();
    }
    panic!("unexpected simulator request {method} {uri}")
}

async fn simulator(
    submit_mode: SubmitMode,
    approvals_ready: bool,
) -> (RelayerCtfTransport, SimulatorState) {
    let signer = PrivateKeySigner::from_str(TEST_KEY).unwrap();
    let state = SimulatorState::new(submit_mode, approvals_ready);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let base = format!("http://{address}");
    let app = Router::new()
        .route("/geoblock", get(simulator_handler))
        .fallback(any(simulator_handler))
        .with_state(state.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let transport = RelayerCtfTransport::new(
        RelayerConfig {
            private_key: TEST_KEY.to_string(),
            wallet_address: derive_beacon_deposit_wallet(signer.address()),
            relayer_api_key: "fixture".to_string(),
            relayer_api_key_address: signer.address(),
            polygon_rpc_url: base.clone(),
            relayer_url: base.clone(),
            clob_url: base.clone(),
            data_api_url: base.clone(),
            geoblock_url: format!("{base}/geoblock"),
            proxy_url: None,
            clob_api_key: "fixture".to_string(),
            clob_api_secret: "Zml4dHVyZQ==".to_string(),
            clob_passphrase: "fixture".to_string(),
            poll_interval: Duration::from_secs(60),
            max_polls: 60,
            deadline: Duration::from_secs(300),
            max_outstanding_pusd: Some(dec!(5)),
        },
        [context()],
    )
    .unwrap();
    (transport, state)
}

fn split_operation() -> PolymarketLifecycleOperation {
    PolymarketLifecycleOperation::Split {
        idempotency_key: "fixture-spread:split:5".to_string(),
        condition_id: format!("{:#x}", context().condition_id),
        quantity: dec!(5),
        neg_risk: false,
    }
}

#[tokio::test]
async fn end_to_end_readiness_prepare_submit_pending_confirmed_has_one_write() {
    let (transport, state) = simulator(SubmitMode::Success, true).await;

    let readiness = transport.readiness().await.unwrap();
    assert_eq!(readiness.chain_id, 137);
    assert!(readiness.wallet_deployed && readiness.relayer_authenticated);
    let prepared = transport.prepare(&split_operation()).await.unwrap();
    assert_eq!(state.submit_count.load(Ordering::SeqCst), 0);

    let submission = transport.submit_prepared(prepared).await.unwrap();
    assert_eq!(state.submit_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        transport.reconcile(&submission).await.unwrap(),
        PolymarketLifecycleReconciliation::Pending,
    );
    assert!(matches!(
        transport.reconcile(&submission).await.unwrap(),
        PolymarketLifecycleReconciliation::Confirmed { transaction_hash }
            if transaction_hash == "0xabc"
    ));
    assert_eq!(state.submit_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn definitive_submit_rejection_is_not_unknown_and_is_not_retried() {
    let (transport, state) = simulator(SubmitMode::Reject, true).await;
    let prepared = transport.prepare(&split_operation()).await.unwrap();

    let error = transport.submit_prepared(prepared).await.unwrap_err();
    assert!(matches!(
        error,
        PolymarketLifecycleBackendError::Rejected(_)
    ));
    assert_eq!(state.submit_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unreadable_success_is_unknown_and_never_retried() {
    let (transport, state) = simulator(SubmitMode::UnreadableSuccess, true).await;
    let prepared = transport.prepare(&split_operation()).await.unwrap();

    let error = transport.submit_prepared(prepared).await.unwrap_err();
    assert!(matches!(error, PolymarketLifecycleBackendError::Unknown(_)));
    assert_eq!(state.submit_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn post_write_disconnect_is_unknown_and_never_retried() {
    use tokio::io::AsyncReadExt;

    let (mut transport, _) = simulator(SubmitMode::Success, true).await;
    let prepared = transport.prepare(&split_operation()).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    transport.config.relayer_url = format!("http://{address}");
    let writes = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&writes);
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = vec![0; 16 * 1024];
        let read = socket.read(&mut bytes).await.unwrap();
        assert!(
            bytes[..read]
                .windows(12)
                .any(|value| value == b"POST /submit")
        );
        observed.fetch_add(1, Ordering::SeqCst);
        drop(socket);
    });

    let error = transport.submit_prepared(prepared).await.unwrap_err();
    assert!(matches!(error, PolymarketLifecycleBackendError::Unknown(_)));
    assert_eq!(writes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn approval_dry_run_does_not_write_and_exact_confirmation_writes_once() {
    let (transport, state) = simulator(SubmitMode::Success, false).await;

    let plan = transport.plan_scoped_approvals().await.unwrap();
    assert_eq!(plan.len(), 3);
    assert_eq!(state.submit_count.load(Ordering::SeqCst), 0);
    assert!(
        transport
            .prepare_scoped_approvals_confirmed("wrong")
            .await
            .is_err()
    );
    assert_eq!(state.submit_count.load(Ordering::SeqCst), 0);

    let prepared = transport
        .prepare_scoped_approvals_confirmed(APPROVAL_WRITE_CONFIRMATION)
        .await
        .unwrap();
    assert_eq!(prepared.plan(), plan);
    assert_eq!(state.submit_count.load(Ordering::SeqCst), 0);
    let submission = transport.submit_prepared_approvals(prepared).await.unwrap();
    assert_eq!(submission.plan, plan);
    assert_eq!(state.submit_count.load(Ordering::SeqCst), 1);
}

fn command(operation: CtfOperation) -> CtfCommand {
    CtfCommand {
        command_id: "fixture-idempotency-key".to_string(),
        lifecycle_id: "fixture-lifecycle".to_string(),
        operation,
        ts_event: 1.into(),
        ts_init: 1.into(),
    }
}

#[test]
fn exact_five_share_split_is_encoded_in_six_decimal_atoms() {
    let transport = transport();
    let context = transport.markets.values().next().unwrap();
    let command = command(CtfOperation::Split {
        condition_id: format!("{:#x}", context.condition_id),
        quantity: dec!(5),
        neg_risk: false,
    });
    let call = transport.operation_call(&command, context).unwrap();
    let decoded = splitPositionCall::abi_decode(&call.data).unwrap();

    assert_eq!(call.target, CTF_COLLATERAL_ADAPTER);
    assert_eq!(decoded.amount, U256::from(5_000_000));
    assert_eq!(decoded.partition, vec![U256::from(1), U256::from(2)]);
}

#[test]
fn signed_deposit_wallet_batch_recovers_the_type3_owner() {
    let transport = transport();
    let context = transport.markets.values().next().unwrap();
    let command = command(CtfOperation::Redeem {
        condition_id: format!("{:#x}", context.condition_id),
        neg_risk: false,
    });
    let call = transport.operation_call(&command, context).unwrap();
    let request = transport
        .signed_batch_request(&command, U256::from(7), 1_000, call.clone())
        .unwrap();
    let batch = Batch {
        wallet: transport.config.wallet_address,
        nonce: U256::from(7),
        deadline: U256::from(1_000),
        calls: vec![call],
    };
    let domain = eip712_domain! {
        name: "DepositWallet",
        version: "1",
        chain_id: POLYGON_CHAIN_ID,
        verifying_contract: transport.config.wallet_address,
    };
    let signature = Signature::from_str(&request.signature).unwrap();

    assert_eq!(
        signature
            .recover_address_from_prehash(&batch.eip712_signing_hash(&domain))
            .unwrap(),
        transport.signer.address(),
    );
    assert_ne!(transport.signer.address(), transport.config.wallet_address);
}

#[test]
fn ambiguous_submit_boundary_maps_to_unknown_without_retry_authority() {
    assert_eq!(
        agent::backend_error(CtfTransportError::Ambiguous(
            "timeout after write".to_string()
        )),
        PolymarketLifecycleBackendError::Unknown("timeout after write".to_string()),
    );
}

#[test]
fn deposit_wallet_derivation_matches_official_vector() {
    let owner = address!("0x1111111111111111111111111111111111111111");
    assert_eq!(
        format!("{:#x}", derive_beacon_deposit_wallet(owner)),
        "0x574548bc296a44a39a7828343fc262244f37a7e5",
    );
}

#[test]
fn config_debug_redacts_credentials() {
    let debug = format!("{:?}", transport().config);
    assert!(!debug.contains(TEST_KEY));
    assert!(!debug.contains("Zml4dHVyZQ=="));
    assert!(debug.contains("***"));
}
