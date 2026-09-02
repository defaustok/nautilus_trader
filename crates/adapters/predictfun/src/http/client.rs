use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
    sync::LazyLock,
};

use nautilus_core::consts::NAUTILUS_USER_AGENT;
use nautilus_network::http::{HttpClient, Method, USER_AGENT};
use nautilus_network::ratelimiter::quota::Quota;
use serde::{Serialize, de::DeserializeOwned};

use super::{
    error::PredictFunHttpError,
    models::{
        PredictFunAccountActivity, PredictFunApiResponse, PredictFunAuthMessage,
        PredictFunAuthRequest, PredictFunAuthToken, PredictFunBook, PredictFunCategory,
        PredictFunCreateOrderRequest, PredictFunCreateOrderResponse, PredictFunMarket,
        PredictFunMatch, PredictFunOrderRecord, PredictFunPosition, PredictFunRemoveOrdersData,
        PredictFunRemoveOrdersRequest, PredictFunRemoveOrdersResponse,
    },
};
use crate::config::SecretString;

const PAGE_SIZE: usize = 100;
const CATEGORY_PAGE_LIMIT: usize = 10;

/// PredictFun's documented default REST allowance.
pub static PREDICTFUN_REST_QUOTA: LazyLock<Quota> =
    LazyLock::new(|| Quota::per_minute(NonZeroU32::new(240).expect("non-zero")));

#[derive(Debug, Clone)]
pub struct PredictFunHttpClient {
    client: HttpClient,
    base_url: String,
}

impl PredictFunHttpClient {
    pub fn new(
        base_url: impl Into<String>,
        api_key: Option<&SecretString>,
        timeout_secs: u64,
    ) -> Result<Self, PredictFunHttpError> {
        let mut headers = HashMap::from([
            (USER_AGENT.to_string(), NAUTILUS_USER_AGENT.to_string()),
            ("Content-Type".to_string(), "application/json".to_string()),
        ]);
        if let Some(api_key) = api_key {
            headers.insert("x-api-key".to_string(), api_key.expose().to_string());
        }
        Ok(Self {
            client: HttpClient::new(
                headers,
                vec![],
                vec![],
                Some(*PREDICTFUN_REST_QUOTA),
                Some(timeout_secs),
                None,
            )?,
            base_url: base_url.into().trim_end_matches('/').to_string(),
        })
    }

    pub async fn get_markets(
        &self,
        filters: Option<&HashMap<String, String>>,
    ) -> Result<Vec<PredictFunMarket>, PredictFunHttpError> {
        let mut markets = Vec::new();
        let mut cursor: Option<String> = None;
        let mut seen = HashSet::new();
        loop {
            let mut params = filters.cloned().unwrap_or_default();
            params.insert("first".to_string(), PAGE_SIZE.to_string());
            if let Some(after) = &cursor {
                params.insert("after".to_string(), after.clone());
            }
            let response: PredictFunApiResponse<Vec<PredictFunMarket>> =
                self.get("/markets", Some(&params), None).await?;
            if !response.success {
                return Err(PredictFunHttpError::Unsuccessful {
                    endpoint: "/markets".to_string(),
                });
            }
            markets.extend(response.data);
            let Some(next) = response.cursor.filter(|value| !value.is_empty()) else {
                break;
            };
            if !seen.insert(next.clone()) {
                return Err(PredictFunHttpError::RepeatedCursor(next));
            }
            cursor = Some(next);
        }
        Ok(markets)
    }

    pub async fn get_categories(
        &self,
        filters: Option<&HashMap<String, String>>,
    ) -> Result<Vec<PredictFunCategory>, PredictFunHttpError> {
        let mut categories = Vec::new();
        let mut cursor: Option<String> = None;
        let mut seen = HashSet::new();
        for _ in 0..CATEGORY_PAGE_LIMIT {
            let mut params = filters.cloned().unwrap_or_default();
            params.insert("first".to_string(), PAGE_SIZE.to_string());
            if let Some(after) = &cursor {
                params.insert("after".to_string(), after.clone());
            }
            let response: PredictFunApiResponse<Vec<PredictFunCategory>> =
                self.get("/categories", Some(&params), None).await?;
            if !response.success {
                return Err(PredictFunHttpError::Unsuccessful {
                    endpoint: "/categories".to_string(),
                });
            }
            categories.extend(response.data);
            let Some(next) = response.cursor.filter(|value| !value.is_empty()) else {
                break;
            };
            if !seen.insert(next.clone()) {
                return Err(PredictFunHttpError::RepeatedCursor(next));
            }
            cursor = Some(next);
        }
        Ok(categories)
    }

    pub async fn get_market(
        &self,
        market_id: u64,
    ) -> Result<PredictFunMarket, PredictFunHttpError> {
        let endpoint = format!("/markets/{market_id}");
        let response: PredictFunApiResponse<PredictFunMarket> =
            self.get(&endpoint, None, None).await?;
        if !response.success {
            return Err(PredictFunHttpError::Unsuccessful { endpoint });
        }
        Ok(response.data)
    }

    pub async fn get_orderbook(
        &self,
        market_id: u64,
    ) -> Result<PredictFunBook, PredictFunHttpError> {
        let endpoint = format!("/markets/{market_id}/orderbook");
        let response: PredictFunApiResponse<PredictFunBook> =
            self.get(&endpoint, None, None).await?;
        if !response.success {
            return Err(PredictFunHttpError::Unsuccessful { endpoint });
        }
        Ok(response.data)
    }

    pub async fn get_auth_message(&self) -> Result<String, PredictFunHttpError> {
        let response: PredictFunApiResponse<PredictFunAuthMessage> =
            self.get("/auth/message", None, None).await?;
        if !response.success {
            return Err(PredictFunHttpError::Unsuccessful {
                endpoint: "/auth/message".to_string(),
            });
        }
        Ok(response.data.message)
    }

    pub async fn authenticate(
        &self,
        request: &PredictFunAuthRequest,
    ) -> Result<SecretString, PredictFunHttpError> {
        let response: PredictFunApiResponse<PredictFunAuthToken> =
            self.post("/auth", request, None).await?;
        if !response.success {
            return Err(PredictFunHttpError::Unsuccessful {
                endpoint: "/auth".to_string(),
            });
        }
        SecretString::new(response.data.token)
            .map_err(|error| PredictFunHttpError::Transport(error.to_string()))
    }

    pub async fn create_order(
        &self,
        token: &SecretString,
        data: crate::http::models::PredictFunCreateOrderData,
    ) -> Result<PredictFunCreateOrderResponse, PredictFunHttpError> {
        let response: PredictFunApiResponse<PredictFunCreateOrderResponse> = self
            .post(
                "/orders",
                &PredictFunCreateOrderRequest { data },
                Some(Self::bearer_headers(token)),
            )
            .await?;
        if !response.success {
            return Err(PredictFunHttpError::Unsuccessful {
                endpoint: "/orders".to_string(),
            });
        }
        Ok(response.data)
    }

    pub async fn remove_orders(
        &self,
        token: &SecretString,
        ids: Vec<String>,
    ) -> Result<PredictFunRemoveOrdersResponse, PredictFunHttpError> {
        let request = PredictFunRemoveOrdersRequest {
            data: PredictFunRemoveOrdersData { ids },
        };
        self.post(
            "/orders/remove",
            &request,
            Some(Self::bearer_headers(token)),
        )
        .await
    }

    pub async fn get_order(
        &self,
        token: &SecretString,
        order_hash: &str,
    ) -> Result<PredictFunOrderRecord, PredictFunHttpError> {
        let endpoint = format!("/orders/{order_hash}");
        let response: PredictFunApiResponse<PredictFunOrderRecord> = self
            .get(&endpoint, None, Some(Self::bearer_headers(token)))
            .await?;
        if !response.success {
            return Err(PredictFunHttpError::Unsuccessful { endpoint });
        }
        Ok(response.data)
    }

    pub async fn get_orders(
        &self,
        token: &SecretString,
        params: Option<&HashMap<String, String>>,
    ) -> Result<Vec<PredictFunOrderRecord>, PredictFunHttpError> {
        self.get_paginated_authenticated("/orders", token, params)
            .await
    }

    pub async fn get_positions(
        &self,
        token: &SecretString,
        params: Option<&HashMap<String, String>>,
    ) -> Result<Vec<PredictFunPosition>, PredictFunHttpError> {
        self.get_paginated_authenticated("/positions", token, params)
            .await
    }

    pub async fn get_matches(
        &self,
        params: Option<&HashMap<String, String>>,
    ) -> Result<Vec<PredictFunMatch>, PredictFunHttpError> {
        self.get_paginated("/orders/matches", params).await
    }

    pub async fn get_account_activity(
        &self,
        token: &SecretString,
        params: Option<&HashMap<String, String>>,
    ) -> Result<Vec<PredictFunAccountActivity>, PredictFunHttpError> {
        self.get_paginated_authenticated("/account/activity", token, params)
            .await
    }

    async fn get_paginated<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        filters: Option<&HashMap<String, String>>,
    ) -> Result<Vec<T>, PredictFunHttpError> {
        let mut records = Vec::new();
        let mut cursor: Option<String> = None;
        let mut seen = HashSet::new();
        loop {
            let mut params = filters.cloned().unwrap_or_default();
            params.insert("first".to_string(), PAGE_SIZE.to_string());
            if let Some(after) = &cursor {
                params.insert("after".to_string(), after.clone());
            }
            let response: PredictFunApiResponse<Vec<T>> =
                self.get(endpoint, Some(&params), None).await?;
            if !response.success {
                return Err(PredictFunHttpError::Unsuccessful {
                    endpoint: endpoint.to_string(),
                });
            }
            records.extend(response.data);
            let Some(next) = response.cursor.filter(|value| !value.is_empty()) else {
                break;
            };
            if !seen.insert(next.clone()) {
                return Err(PredictFunHttpError::RepeatedCursor(next));
            }
            cursor = Some(next);
        }
        Ok(records)
    }

    async fn get_paginated_authenticated<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        token: &SecretString,
        filters: Option<&HashMap<String, String>>,
    ) -> Result<Vec<T>, PredictFunHttpError> {
        let mut records = Vec::new();
        let mut cursor: Option<String> = None;
        let mut seen = HashSet::new();
        loop {
            let mut params = filters.cloned().unwrap_or_default();
            params.insert("first".to_string(), PAGE_SIZE.to_string());
            if let Some(after) = &cursor {
                params.insert("after".to_string(), after.clone());
            }
            let response: PredictFunApiResponse<Vec<T>> = self
                .get(endpoint, Some(&params), Some(Self::bearer_headers(token)))
                .await?;
            if !response.success {
                return Err(PredictFunHttpError::Unsuccessful {
                    endpoint: endpoint.to_string(),
                });
            }
            records.extend(response.data);
            let Some(next) = response.cursor.filter(|value| !value.is_empty()) else {
                break;
            };
            if !seen.insert(next.clone()) {
                return Err(PredictFunHttpError::RepeatedCursor(next));
            }
            cursor = Some(next);
        }
        Ok(records)
    }

    async fn get<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        params: Option<&HashMap<String, String>>,
        headers: Option<HashMap<String, String>>,
    ) -> Result<T, PredictFunHttpError> {
        let response = self
            .client
            .request_with_params(
                Method::GET,
                format!("{}{endpoint}", self.base_url),
                params,
                headers,
                None,
                None,
                None,
            )
            .await?;
        if !response.status.is_success() {
            let message = String::from_utf8_lossy(&response.body);
            return Err(PredictFunHttpError::Status {
                status: response.status.as_u16(),
                message: message.chars().take(512).collect(),
            });
        }
        Ok(serde_json::from_slice(&response.body)?)
    }

    async fn post<B: Serialize, T: DeserializeOwned>(
        &self,
        endpoint: &str,
        body: &B,
        headers: Option<HashMap<String, String>>,
    ) -> Result<T, PredictFunHttpError> {
        let response = self
            .client
            .request(
                Method::POST,
                format!("{}{endpoint}", self.base_url),
                None,
                headers,
                Some(serde_json::to_vec(body)?),
                None,
                None,
            )
            .await?;
        if !response.status.is_success() {
            return Err(PredictFunHttpError::Status {
                status: response.status.as_u16(),
                message: String::from_utf8_lossy(&response.body)
                    .chars()
                    .take(512)
                    .collect(),
            });
        }
        Ok(serde_json::from_slice(&response.body)?)
    }

    fn bearer_headers(token: &SecretString) -> HashMap<String, String> {
        HashMap::from([(
            "Authorization".to_string(),
            format!("Bearer {}", token.expose()),
        )])
    }
}
