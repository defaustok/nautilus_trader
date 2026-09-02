use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PredictFunError {
    #[error("PredictFun book market {actual} does not match subscribed market {expected}")]
    MarketMismatch { expected: u64, actual: u64 },
    #[error("PredictFun {side} price {price} must be strictly between 0 and 1")]
    InvalidPrice { side: &'static str, price: String },
    #[error("PredictFun {side} price {price} is not aligned to precision {precision}")]
    InvalidPricePrecision {
        side: &'static str,
        price: String,
        precision: u8,
    },
    #[error("PredictFun {side} level at {price} has non-positive size {size}")]
    InvalidSize {
        side: &'static str,
        price: String,
        size: String,
    },
    #[error("PredictFun {side} contains duplicate price {price}")]
    DuplicatePrice { side: &'static str, price: String },
    #[error("PredictFun book is crossed: best bid {best_bid} >= best ask {best_ask}")]
    CrossedBook { best_bid: String, best_ask: String },
    #[error("PredictFun value {value} cannot be represented as unsigned 18-decimal wei")]
    InvalidWei { value: String },
    #[error("PredictFun quantity {value} is below Nautilus 16-decimal resolution")]
    BelowNautilusResolution { value: String },
    #[error("PredictFun quantity {value} exceeds the Nautilus quantity range")]
    QuantityOverflow { value: String },
    #[error("PredictFun cumulative quantity regressed from {previous} wei to {actual} wei")]
    CumulativeQuantityRegression { previous: String, actual: String },
    #[error("PredictFun book version {actual} duplicated the current version")]
    DuplicateBookVersion { actual: u64 },
    #[error("PredictFun book version regressed from {previous} to {actual}")]
    RegressedBookVersion { previous: u64, actual: u64 },
    #[error("PredictFun book version jumped from {previous} to {actual}; resnapshot required")]
    BookVersionGap { previous: u64, actual: u64 },
}
