use std::sync::LazyLock;

use nautilus_model::{enums::CurrencyType, identifiers::Venue, types::Currency};
use ustr::Ustr;

pub const PREDICTFUN: &str = "PREDICTFUN";
pub const PREDICTFUN_API_BASE: &str = "https://api.predict.fun/v1";
pub const PREDICTFUN_TESTNET_API_BASE: &str = "https://api-testnet.predict.fun/v1";
pub const PREDICTFUN_WS_URL: &str = "wss://ws.predict.fun/ws";
pub const BNB_MAINNET_CHAIN_ID: u64 = 56;
pub const BNB_TESTNET_CHAIN_ID: u64 = 97;
pub const WEI_SCALE: u32 = 18;
/// Maximum decimal precision representable by the pinned Nautilus fixed-point model.
pub const NAUTILUS_QUANTITY_PRECISION: u8 = 16;

pub static PREDICTFUN_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(PREDICTFUN)));

pub fn usdt() -> Currency {
    Currency::new_checked(
        "USDT",
        NAUTILUS_QUANTITY_PRECISION,
        0,
        "Tether USD",
        CurrencyType::Crypto,
    )
    .expect("hard-coded PredictFun USDT currency must be valid")
}
