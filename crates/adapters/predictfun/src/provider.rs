use std::collections::HashMap;

use async_trait::async_trait;
use nautilus_common::providers::{InstrumentProvider, InstrumentStore};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};

use crate::{http::PredictFunHttpClient, http::parse::create_instrument};

#[derive(Debug)]
pub struct PredictFunInstrumentProvider {
    store: InstrumentStore,
    http_client: PredictFunHttpClient,
    metadata: HashMap<InstrumentId, PredictFunInstrumentMeta>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PredictFunInstrumentMeta {
    pub market_id: u64,
    pub price_precision: u8,
    pub is_yes: bool,
}

impl PredictFunInstrumentProvider {
    pub fn new(http_client: PredictFunHttpClient) -> Self {
        Self {
            store: InstrumentStore::new(),
            http_client,
            metadata: HashMap::new(),
        }
    }

    pub fn metadata(&self) -> &HashMap<InstrumentId, PredictFunInstrumentMeta> {
        &self.metadata
    }
}

pub(crate) async fn fetch_instruments(
    http_client: &PredictFunHttpClient,
    filters: Option<&HashMap<String, String>>,
) -> anyhow::Result<(
    Vec<InstrumentAny>,
    HashMap<InstrumentId, PredictFunInstrumentMeta>,
)> {
    let mut markets = Vec::new();
    if let Some(market_id) = filters.and_then(|values| values.get("marketId")) {
        let market_id = market_id
            .parse::<u64>()
            .map_err(|error| anyhow::anyhow!("invalid PredictFun marketId {market_id}: {error}"))?;
        markets.push(http_client.get_market(market_id).await?);
    } else {
        let categories = http_client.get_categories(filters).await?;
        for category in categories {
            for mut market in category.markets {
                market.starts_at = category.starts_at.clone();
                market.ends_at = category.ends_at.clone();
                market.category_slug = category.title.clone().or(category.short_title.clone());
                if market.variant_data.is_none() {
                    market.variant_data = category.variant_data.clone();
                }
                markets.push(market);
            }
        }
    }
    let ts_init = get_atomic_clock_realtime().get_time_ns();
    let mut instruments = Vec::new();
    let mut metadata = HashMap::new();
    for market in markets {
        for outcome in &market.outcomes {
            match create_instrument(&market, outcome, ts_init) {
                Ok(instrument) => {
                    metadata.insert(
                        instrument.id(),
                        PredictFunInstrumentMeta {
                            market_id: market.id,
                            price_precision: market.decimal_precision,
                            is_yes: outcome.name.eq_ignore_ascii_case("yes")
                                || outcome.index_set == 1,
                        },
                    );
                    instruments.push(instrument);
                }
                Err(error) => log::warn!(
                    "Skipping invalid PredictFun market {} outcome {}: {error}",
                    market.id,
                    outcome.name
                ),
            }
        }
    }
    Ok((instruments, metadata))
}

#[async_trait(?Send)]
impl InstrumentProvider for PredictFunInstrumentProvider {
    fn store(&self) -> &InstrumentStore {
        &self.store
    }

    fn store_mut(&mut self) -> &mut InstrumentStore {
        &mut self.store
    }

    async fn load_all(&mut self, filters: Option<&HashMap<String, String>>) -> anyhow::Result<()> {
        let (instruments, metadata) = fetch_instruments(&self.http_client, filters).await?;
        self.store.clear();
        self.metadata = metadata;
        self.store.add_bulk(instruments);
        self.store.set_initialized();
        Ok(())
    }

    async fn load_ids(
        &mut self,
        instrument_ids: &[InstrumentId],
        filters: Option<&HashMap<String, String>>,
    ) -> anyhow::Result<()> {
        if instrument_ids.iter().all(|id| self.store.contains(id)) {
            return Ok(());
        }
        self.load_all(filters).await?;
        let missing: Vec<_> = instrument_ids
            .iter()
            .filter(|id| !self.store.contains(id))
            .collect();
        if missing.is_empty() {
            Ok(())
        } else {
            anyhow::bail!("PredictFun instruments not found: {missing:?}")
        }
    }

    async fn load(
        &mut self,
        instrument_id: &InstrumentId,
        filters: Option<&HashMap<String, String>>,
    ) -> anyhow::Result<()> {
        self.load_ids(&[*instrument_id], filters).await
    }
}
