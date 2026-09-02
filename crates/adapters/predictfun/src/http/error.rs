use nautilus_network::http::HttpClientError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PredictFunHttpError {
    #[error("PredictFun HTTP transport error: {0}")]
    Transport(String),
    #[error("PredictFun HTTP {status}: {message}")]
    Status { status: u16, message: String },
    #[error("PredictFun returned success=false for {endpoint}")]
    Unsuccessful { endpoint: String },
    #[error("PredictFun response decode error: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("PredictFun pagination cursor repeated: {0}")]
    RepeatedCursor(String),
}

impl From<HttpClientError> for PredictFunHttpError {
    fn from(error: HttpClientError) -> Self {
        Self::Transport(error.to_string())
    }
}

impl PredictFunHttpError {
    /// Returns true when an idempotent read can be retried safely.
    #[must_use]
    pub(crate) const fn is_retryable_read(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Status { status, .. } => matches!(*status, 408 | 425 | 429) || *status >= 500,
            Self::Unsuccessful { .. } | Self::Decode(_) | Self::RepeatedCursor(_) => false,
        }
    }

    /// Returns true only when the venue definitively rejected a request.
    /// Transport failures, timeouts, throttling and server errors are ambiguous
    /// for state-changing commands and must be reconciled before retrying.
    #[must_use]
    pub fn is_definitive_rejection(&self) -> bool {
        match self {
            Self::Status { status, .. } => {
                (400..500).contains(status) && !matches!(*status, 408 | 409 | 425 | 429)
            }
            Self::Unsuccessful { .. } => true,
            Self::Transport(_) | Self::Decode(_) | Self::RepeatedCursor(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[test]
    fn classifies_state_changing_http_outcomes_conservatively() {
        assert!(
            PredictFunHttpError::Status {
                status: 400,
                message: "bad order".to_string(),
            }
            .is_definitive_rejection()
        );
        for status in [408, 409, 425, 429, 500, 503] {
            assert!(
                !PredictFunHttpError::Status {
                    status,
                    message: "ambiguous".to_string(),
                }
                .is_definitive_rejection()
            );
        }
        assert!(!PredictFunHttpError::Transport("timeout".to_string()).is_definitive_rejection());
    }

    #[rstest]
    fn classifies_only_transient_read_failures_as_retryable() {
        for status in [408, 425, 429, 500, 503] {
            assert!(
                PredictFunHttpError::Status {
                    status,
                    message: "transient".to_string(),
                }
                .is_retryable_read()
            );
        }
        assert!(PredictFunHttpError::Transport("timeout".to_string()).is_retryable_read());
        assert!(
            !PredictFunHttpError::Status {
                status: 400,
                message: "invalid request".to_string(),
            }
            .is_retryable_read()
        );
        assert!(
            !PredictFunHttpError::Unsuccessful {
                endpoint: "/v1/orders".to_string(),
            }
            .is_retryable_read()
        );
    }
}
