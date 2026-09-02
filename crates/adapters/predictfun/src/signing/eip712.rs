use std::str::FromStr;

use alloy::{
    signers::{SignerSync, local::PrivateKeySigner},
    sol_types::{SolStruct, eip712_domain},
};
use alloy_primitives::{Address, B256, U256, address, eip191_hash_message, keccak256};

use crate::{
    common::{
        consts::{BNB_MAINNET_CHAIN_ID, BNB_TESTNET_CHAIN_ID},
        enums::{PredictFunEnvironment, PredictFunSignatureType},
    },
    http::models::PredictFunContractOrder,
};

const DOMAIN_NAME: &str = "predict.fun CTF Exchange";
const DOMAIN_VERSION: &str = "1";
const KERNEL_DOMAIN_NAME: &str = "Kernel";
const KERNEL_DOMAIN_VERSION: &str = "0.3.1";
const ECDSA_VALIDATOR: Address = address!("0x845ADb2C711129d4f3966735eD98a9F09fC4cE57");

const MAINNET_CTF: Address = address!("0x8BC070BEdAB741406F4B1Eb65A72bee27894B689");
const MAINNET_NEG_RISK: Address = address!("0x365fb81bd4A24D6303cd2F19c349dE6894D8d58A");
const MAINNET_YIELD_CTF: Address = address!("0x6bEb5a40C032AFc305961162d8204CDA16DECFa5");
const MAINNET_YIELD_NEG_RISK: Address = address!("0x8A289d458f5a134bA40015085A8F50Ffb681B41d");
const TESTNET_CTF: Address = address!("0x2A6413639BD3d73a20ed8C95F634Ce198ABbd2d7");
const TESTNET_NEG_RISK: Address = address!("0xd690b2bd441bE36431F6F6639D7Ad351e7B29680");
const TESTNET_YIELD_CTF: Address = address!("0x8a6B4Fa700A1e310b106E7a48bAFa29111f66e89");
const TESTNET_YIELD_NEG_RISK: Address = address!("0x95D5113bc50eD201e319101bbca3e0E250662fCC");

alloy::sol! {
    struct Order {
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
    }
}

pub fn exchange_contract(
    environment: PredictFunEnvironment,
    is_neg_risk: bool,
    is_yield_bearing: bool,
) -> Address {
    match (environment, is_neg_risk, is_yield_bearing) {
        (PredictFunEnvironment::Mainnet, false, false) => MAINNET_CTF,
        (PredictFunEnvironment::Mainnet, true, false) => MAINNET_NEG_RISK,
        (PredictFunEnvironment::Mainnet, false, true) => MAINNET_YIELD_CTF,
        (PredictFunEnvironment::Mainnet, true, true) => MAINNET_YIELD_NEG_RISK,
        (PredictFunEnvironment::Testnet, false, false) => TESTNET_CTF,
        (PredictFunEnvironment::Testnet, true, false) => TESTNET_NEG_RISK,
        (PredictFunEnvironment::Testnet, false, true) => TESTNET_YIELD_CTF,
        (PredictFunEnvironment::Testnet, true, true) => TESTNET_YIELD_NEG_RISK,
    }
}

pub fn order_hash(
    order: &PredictFunContractOrder,
    environment: PredictFunEnvironment,
    is_neg_risk: bool,
    is_yield_bearing: bool,
) -> anyhow::Result<B256> {
    let eip712_order = to_eip712_order(order)?;
    let contract = exchange_contract(environment, is_neg_risk, is_yield_bearing);
    let chain_id = match environment {
        PredictFunEnvironment::Mainnet => BNB_MAINNET_CHAIN_ID,
        PredictFunEnvironment::Testnet => BNB_TESTNET_CHAIN_ID,
    };
    let domain = eip712_domain! {
        name: DOMAIN_NAME,
        version: DOMAIN_VERSION,
        chain_id: chain_id,
        verifying_contract: contract,
    };
    Ok(eip712_order.eip712_signing_hash(&domain))
}

#[derive(Debug)]
pub struct PredictFunOrderSigner {
    signer: PrivateKeySigner,
}

impl PredictFunOrderSigner {
    pub fn new(private_key: &str) -> anyhow::Result<Self> {
        let private_key = private_key.strip_prefix("0x").unwrap_or(private_key);
        Ok(Self {
            signer: PrivateKeySigner::from_str(private_key)?,
        })
    }

    pub fn address(&self) -> Address {
        self.signer.address()
    }

    pub fn sign_order(
        &self,
        order: &PredictFunContractOrder,
        environment: PredictFunEnvironment,
        is_neg_risk: bool,
        is_yield_bearing: bool,
    ) -> anyhow::Result<String> {
        if order.signature_type != PredictFunSignatureType::Eoa {
            anyhow::bail!("Predict smart-account signature wrapping is not implemented yet");
        }
        let signer = Address::from_str(&order.signer)?;
        let maker = Address::from_str(&order.maker)?;
        if signer != self.signer.address() || maker != signer {
            anyhow::bail!("EOA order maker and signer must match the local signer");
        }
        let hash = order_hash(order, environment, is_neg_risk, is_yield_bearing)?;
        let signature = self.signer.sign_hash_sync(&hash)?;
        Ok(format!(
            "0x{}",
            alloy_primitives::hex::encode(signature.as_bytes())
        ))
    }

    pub fn sign_order_for_predict_account(
        &self,
        order: &PredictFunContractOrder,
        predict_account: Address,
        environment: PredictFunEnvironment,
        is_neg_risk: bool,
        is_yield_bearing: bool,
    ) -> anyhow::Result<String> {
        let signer = Address::from_str(&order.signer)?;
        let maker = Address::from_str(&order.maker)?;
        if signer != predict_account || maker != predict_account {
            anyhow::bail!(
                "Predict account order maker and signer must match the configured smart account"
            );
        }
        let order_hash = order_hash(order, environment, is_neg_risk, is_yield_bearing)?;
        let digest = kernel_wrapped_hash(order_hash, predict_account, environment);
        // The official SDK calls signMessage on the 32-byte Kernel digest, so
        // this deliberately applies the EIP-191 personal-message prefix.
        let signature = self.signer.sign_message_sync(digest.as_slice())?;
        let mut wrapped = Vec::with_capacity(1 + Address::len_bytes() + 65);
        wrapped.push(0x01);
        wrapped.extend_from_slice(ECDSA_VALIDATOR.as_slice());
        wrapped.extend_from_slice(&signature.as_bytes());
        Ok(format!("0x{}", alloy_primitives::hex::encode(wrapped)))
    }

    pub fn sign_auth_message(
        &self,
        message: &str,
        predict_account: Option<Address>,
        environment: PredictFunEnvironment,
    ) -> anyhow::Result<String> {
        let signature = match predict_account {
            Some(account) => {
                let message_hash = eip191_hash_message(message.as_bytes());
                let digest = kernel_wrapped_hash(message_hash, account, environment);
                let signature = self.signer.sign_message_sync(digest.as_slice())?;
                let mut wrapped = Vec::with_capacity(86);
                wrapped.push(0x01);
                wrapped.extend_from_slice(ECDSA_VALIDATOR.as_slice());
                wrapped.extend_from_slice(&signature.as_bytes());
                wrapped
            }
            None => self
                .signer
                .sign_message_sync(message.as_bytes())?
                .as_bytes()
                .to_vec(),
        };
        Ok(format!("0x{}", alloy_primitives::hex::encode(signature)))
    }
}

pub fn kernel_wrapped_hash(
    message_hash: B256,
    predict_account: Address,
    environment: PredictFunEnvironment,
) -> B256 {
    let chain_id = match environment {
        PredictFunEnvironment::Mainnet => BNB_MAINNET_CHAIN_ID,
        PredictFunEnvironment::Testnet => BNB_TESTNET_CHAIN_ID,
    };
    let domain = eip712_domain! {
        name: KERNEL_DOMAIN_NAME,
        version: KERNEL_DOMAIN_VERSION,
        chain_id: chain_id,
        verifying_contract: predict_account,
    };
    let type_hash = keccak256("Kernel(bytes32 hash)".as_bytes());
    let mut encoded = [0u8; 64];
    encoded[..32].copy_from_slice(type_hash.as_slice());
    encoded[32..].copy_from_slice(message_hash.as_slice());
    let kernel_hash = keccak256(encoded);
    let mut wrapped = [0u8; 66];
    wrapped[..2].copy_from_slice(&[0x19, 0x01]);
    wrapped[2..34].copy_from_slice(domain.separator().as_slice());
    wrapped[34..].copy_from_slice(kernel_hash.as_slice());
    keccak256(wrapped)
}

fn to_eip712_order(order: &PredictFunContractOrder) -> anyhow::Result<Order> {
    Ok(Order {
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::enums::PredictFunSide;

    fn official_vector_order() -> PredictFunContractOrder {
        PredictFunContractOrder {
            salt: "123456789".to_string(),
            maker: "0x1234567890123456789012345678901234567890".to_string(),
            signer: "0x1234567890123456789012345678901234567890".to_string(),
            taker: "0x0000000000000000000000000000000000000000".to_string(),
            token_id: "12345".to_string(),
            maker_amount: "1000000000000000000".to_string(),
            taker_amount: "2000000000000000000".to_string(),
            expiration: "4102444800".to_string(),
            nonce: "0".to_string(),
            fee_rate_bps: "100".to_string(),
            side: PredictFunSide::Buy,
            signature_type: PredictFunSignatureType::Eoa,
            signature: None,
            hash: None,
        }
    }

    #[test]
    fn matches_official_cross_sdk_mainnet_hash_vector() {
        let hash = order_hash(
            &official_vector_order(),
            PredictFunEnvironment::Mainnet,
            false,
            false,
        )
        .unwrap();
        assert_eq!(
            format!("{hash:#x}"),
            "0x814000c89efa61ae42a2bcc4c98e06e90c11480b95a12edea00e3411ec76821d"
        );
    }

    #[test]
    fn maps_every_official_exchange_contract_variant() {
        let cases = [
            (PredictFunEnvironment::Mainnet, false, false, MAINNET_CTF),
            (
                PredictFunEnvironment::Mainnet,
                true,
                false,
                MAINNET_NEG_RISK,
            ),
            (
                PredictFunEnvironment::Mainnet,
                false,
                true,
                MAINNET_YIELD_CTF,
            ),
            (
                PredictFunEnvironment::Mainnet,
                true,
                true,
                MAINNET_YIELD_NEG_RISK,
            ),
            (PredictFunEnvironment::Testnet, false, false, TESTNET_CTF),
            (
                PredictFunEnvironment::Testnet,
                true,
                false,
                TESTNET_NEG_RISK,
            ),
            (
                PredictFunEnvironment::Testnet,
                false,
                true,
                TESTNET_YIELD_CTF,
            ),
            (
                PredictFunEnvironment::Testnet,
                true,
                true,
                TESTNET_YIELD_NEG_RISK,
            ),
        ];
        for (environment, neg_risk, yield_bearing, expected) in cases {
            assert_eq!(
                exchange_contract(environment, neg_risk, yield_bearing),
                expected
            );
        }
    }

    #[test]
    fn matches_official_python_sdk_kernel_wrap_vector() {
        let message_hash =
            B256::from_str("0x814000c89efa61ae42a2bcc4c98e06e90c11480b95a12edea00e3411ec76821d")
                .unwrap();
        let predict_account =
            Address::from_str("0x1234567890123456789012345678901234567890").unwrap();
        let digest = kernel_wrapped_hash(
            message_hash,
            predict_account,
            PredictFunEnvironment::Mainnet,
        );
        assert_eq!(
            format!("{digest:#x}"),
            "0x5907657290476011027724b54013dae4ac3b6b350c4544ea8e037b4f22049b24"
        );
    }
}
