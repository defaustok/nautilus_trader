use std::{collections::HashMap, str::FromStr, time::Duration};

use alloy::{
    network::{EthereumWallet, ReceiptResponse},
    primitives::{Address, B256, Bytes, U256},
    providers::{Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
    sol_types::SolCall,
};
use alloy_primitives::address;

use crate::{
    common::{
        consts::{BNB_MAINNET_CHAIN_ID, BNB_TESTNET_CHAIN_ID},
        enums::{PredictFunAccountType, PredictFunEnvironment},
    },
    config::SecretString,
    http::models::PredictFunContractOrder,
    signing::eip712::exchange_contract,
};

alloy::sol! {
    struct ExchangeOrder {
        uint256 salt;
        address maker;
        address signer;
        address taker;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint256 expiration;
        uint256 nonce;
        uint256 feeRateBps;
        uint8 side;
        uint8 signatureType;
        bytes signature;
    }

    #[sol(rpc)]
    interface CtfExchange {
        function cancelOrders(ExchangeOrder[] orders) external;
        function getOrderStatus(bytes32 orderHash)
            external
            view
            returns (bool isFilledOrCancelled, uint256 remaining);
    }

    #[sol(rpc)]
    interface Kernel {
        function execute(bytes32 execMode, bytes executionCalldata) external payable;
    }

    #[sol(rpc)]
    interface Erc20 {
        function balanceOf(address account) external view returns (uint256);
    }
}

const MAINNET_USDT: Address = address!("0x55d398326f99059fF775485246999027B3197955");
const TESTNET_USDT: Address = address!("0xB32171ecD878607FFc4F8FC0bCcE6852BB3149E0");

#[derive(Debug, Clone)]
pub(super) struct CancelRequest {
    pub venue_order_id: String,
    pub order: PredictFunContractOrder,
    pub is_neg_risk: bool,
    pub is_yield_bearing: bool,
}

pub(super) async fn verify_rpc(
    rpc_url: &SecretString,
    environment: PredictFunEnvironment,
) -> anyhow::Result<()> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.expose().parse()?);
    let actual = provider.get_chain_id().await?;
    let expected = chain_id(environment);
    if actual != expected {
        anyhow::bail!("PredictFun RPC chain ID mismatch: expected {expected}, received {actual}");
    }
    Ok(())
}

pub(super) async fn collateral_balance(
    rpc_url: &SecretString,
    environment: PredictFunEnvironment,
    account: Address,
) -> anyhow::Result<U256> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.expose().parse()?);
    let actual = provider.get_chain_id().await?;
    let expected = chain_id(environment);
    if actual != expected {
        anyhow::bail!("PredictFun RPC chain ID mismatch: expected {expected}, received {actual}");
    }
    let contract = match environment {
        PredictFunEnvironment::Mainnet => MAINNET_USDT,
        PredictFunEnvironment::Testnet => TESTNET_USDT,
    };
    Ok(Erc20::new(contract, provider)
        .balanceOf(account)
        .call()
        .await?)
}

pub(super) async fn cancel_groups(
    requests: Vec<CancelRequest>,
    rpc_url: &SecretString,
    private_key: &SecretString,
    environment: PredictFunEnvironment,
    account_type: PredictFunAccountType,
    account_address: Address,
    timeout_secs: u64,
) -> HashMap<String, anyhow::Result<()>> {
    let mut groups: HashMap<(bool, bool), Vec<CancelRequest>> = HashMap::new();
    for request in requests {
        groups
            .entry((request.is_neg_risk, request.is_yield_bearing))
            .or_default()
            .push(request);
    }

    let mut outcomes = HashMap::new();
    for ((is_neg_risk, is_yield_bearing), requests) in groups {
        let ids = requests
            .iter()
            .map(|request| request.venue_order_id.clone())
            .collect::<Vec<_>>();
        let result = cancel_group(
            &requests,
            rpc_url,
            private_key,
            environment,
            account_type,
            account_address,
            is_neg_risk,
            is_yield_bearing,
            timeout_secs,
        )
        .await;
        match result {
            Ok(()) => {
                for id in ids {
                    outcomes.insert(id, Ok(()));
                }
            }
            Err(error) => {
                let message = error.to_string();
                for id in ids {
                    outcomes.insert(id, Err(anyhow::anyhow!(message.clone())));
                }
            }
        }
    }
    outcomes
}

#[expect(clippy::too_many_arguments)]
async fn cancel_group(
    requests: &[CancelRequest],
    rpc_url: &SecretString,
    private_key: &SecretString,
    environment: PredictFunEnvironment,
    account_type: PredictFunAccountType,
    account_address: Address,
    is_neg_risk: bool,
    is_yield_bearing: bool,
    timeout_secs: u64,
) -> anyhow::Result<()> {
    let key = private_key
        .expose()
        .strip_prefix("0x")
        .unwrap_or(private_key.expose());
    let signer = PrivateKeySigner::from_str(key)?;
    let provider = ProviderBuilder::new()
        .with_chain_id(chain_id(environment))
        .wallet(EthereumWallet::from(signer))
        .connect_http(rpc_url.expose().parse()?);
    let actual_chain = provider.get_chain_id().await?;
    if actual_chain != chain_id(environment) {
        anyhow::bail!("PredictFun RPC chain changed before cancellation");
    }

    let orders = requests
        .iter()
        .map(|request| exchange_order(&request.order))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let exchange_address = exchange_contract(environment, is_neg_risk, is_yield_bearing);
    let receipt = match account_type {
        PredictFunAccountType::Eoa => {
            let exchange = CtfExchange::new(exchange_address, provider.clone());
            let call = exchange.cancelOrders(orders);
            let gas = call.estimate_gas().await?;
            let pending = call.gas(gas.saturating_mul(125) / 100).send().await?;
            tokio::time::timeout(Duration::from_secs(timeout_secs), pending.get_receipt())
                .await
                .map_err(|_| anyhow::anyhow!("PredictFun cancellation receipt timed out"))??
        }
        PredictFunAccountType::PredictAccount => {
            let encoded = CtfExchange::cancelOrdersCall { orders }.abi_encode();
            let execution = execution_calldata(exchange_address, &encoded);
            let kernel = Kernel::new(account_address, provider.clone());
            let call = kernel.execute(B256::ZERO, Bytes::from(execution));
            let gas = call.estimate_gas().await?;
            let pending = call.gas(gas.saturating_mul(125) / 100).send().await?;
            tokio::time::timeout(Duration::from_secs(timeout_secs), pending.get_receipt())
                .await
                .map_err(|_| anyhow::anyhow!("PredictFun cancellation receipt timed out"))??
        }
    };
    receipt.ensure_success()?;

    let exchange = CtfExchange::new(exchange_address, provider);
    for request in requests {
        let hash = request
            .order
            .hash
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("PredictFun cancellation order is missing hash"))?
            .parse::<B256>()?;
        let status = exchange.getOrderStatus(hash).call().await?;
        if !status.isFilledOrCancelled {
            anyhow::bail!(
                "PredictFun order {} remained active after confirmed cancellation",
                request.venue_order_id
            );
        }
    }
    Ok(())
}

fn chain_id(environment: PredictFunEnvironment) -> u64 {
    match environment {
        PredictFunEnvironment::Mainnet => BNB_MAINNET_CHAIN_ID,
        PredictFunEnvironment::Testnet => BNB_TESTNET_CHAIN_ID,
    }
}

fn exchange_order(order: &PredictFunContractOrder) -> anyhow::Result<ExchangeOrder> {
    let signature = order
        .signature
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("PredictFun cancellation order is missing signature"))?;
    Ok(ExchangeOrder {
        salt: U256::from_str(&order.salt)?,
        maker: Address::from_str(&order.maker)?,
        signer: Address::from_str(&order.signer)?,
        taker: Address::from_str(&order.taker)?,
        tokenId: U256::from_str(&order.token_id)?,
        makerAmount: U256::from_str(&order.maker_amount)?,
        takerAmount: U256::from_str(&order.taker_amount)?,
        expiration: U256::from_str(&order.expiration)?,
        nonce: U256::from_str(&order.nonce)?,
        feeRateBps: U256::from_str(&order.fee_rate_bps)?,
        side: order.side as u8,
        signatureType: order.signature_type as u8,
        signature: Bytes::from(alloy::hex::decode(signature.trim_start_matches("0x"))?),
    })
}

fn execution_calldata(target: Address, call: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(20 + 32 + call.len());
    encoded.extend_from_slice(target.as_slice());
    encoded.extend_from_slice(&[0; 32]);
    encoded.extend_from_slice(call);
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_execution_calldata_matches_official_layout() {
        let target = Address::repeat_byte(0x11);
        let encoded = execution_calldata(target, &[0xaa, 0xbb]);
        assert_eq!(&encoded[..20], target.as_slice());
        assert_eq!(&encoded[20..52], &[0; 32]);
        assert_eq!(&encoded[52..], &[0xaa, 0xbb]);
    }

    #[tokio::test]
    #[ignore = "mutates BNB testnet; requires explicit PredictFun live-test environment"]
    async fn live_eoa_cancel_confirms_contract_status() {
        use crate::{
            common::consts::PREDICTFUN_TESTNET_API_BASE,
            http::{PredictFunHttpClient, models::PredictFunAuthRequest},
            signing::eip712::PredictFunOrderSigner,
        };

        let result: anyhow::Result<()> = async {
            assert_eq!(
                std::env::var("PREDICTFUN_EXEC_TESTER_LIVE").as_deref(),
                Ok("1")
            );
            let key =
                std::fs::read_to_string(std::env::var("PREDICTFUN_TESTNET_PRIVATE_KEY_FILE")?)?;
            let private_key = SecretString::new(key.trim().to_string())?;
            let signer = PredictFunOrderSigner::new(private_key.expose())?;
            let order_hash = std::env::var("PREDICTFUN_TESTNET_ORDER_HASH")?;
            let venue_order_id = std::env::var("PREDICTFUN_TESTNET_ORDER_ID")?;
            let client = PredictFunHttpClient::new(PREDICTFUN_TESTNET_API_BASE, None, 30)?;
            let message = client.get_auth_message().await?;
            let signature =
                signer.sign_auth_message(&message, None, PredictFunEnvironment::Testnet)?;
            let token = client
                .authenticate(&PredictFunAuthRequest {
                    signer: format!("{:#x}", signer.address()),
                    signature,
                    message,
                })
                .await?;
            let record = client.get_order(&token, &order_hash).await?;
            cancel_group(
                &[CancelRequest {
                    venue_order_id,
                    order: record.order,
                    is_neg_risk: false,
                    is_yield_bearing: false,
                }],
                &SecretString::new("https://bsc-testnet.publicnode.com".to_string())?,
                &private_key,
                PredictFunEnvironment::Testnet,
                PredictFunAccountType::Eoa,
                signer.address(),
                false,
                false,
                120,
            )
            .await
        }
        .await;
        assert!(result.is_ok(), "{result:?}");
    }

    #[test]
    fn exchange_order_preserves_signature_and_contract_fields() {
        let order = PredictFunContractOrder {
            salt: "1".to_string(),
            maker: format!("{:#x}", Address::repeat_byte(0x11)),
            signer: format!("{:#x}", Address::repeat_byte(0x22)),
            taker: format!("{:#x}", Address::ZERO),
            token_id: "2".to_string(),
            maker_amount: "3".to_string(),
            taker_amount: "4".to_string(),
            expiration: "5".to_string(),
            nonce: "6".to_string(),
            fee_rate_bps: "7".to_string(),
            side: crate::common::enums::PredictFunSide::Sell,
            signature_type: crate::common::enums::PredictFunSignatureType::Eoa,
            signature: Some("0xaabb".to_string()),
            hash: Some(format!("{:#x}", B256::repeat_byte(0x33))),
        };
        let converted = exchange_order(&order).unwrap();
        assert_eq!(converted.tokenId, U256::from(2));
        assert_eq!(converted.side, 1);
        assert_eq!(converted.signature.as_ref(), &[0xaa, 0xbb]);
    }
}
