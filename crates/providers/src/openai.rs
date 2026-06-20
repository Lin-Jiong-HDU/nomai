//! OpenAI-compatible provider implementations.

use async_trait::async_trait;
use reqwest::Client;

use crate::error::{ProviderError, ProviderErrorKind};
use crate::traits::LlmProvider;
use crate::types::{CompletionRequest, CompletionResponse, MessageRole};

pub struct OpenAiCompatibleLlm {
    client: Client,
    base_url: String,
    api_key: String,
    model: String,
}

impl OpenAiCompatibleLlm {
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: api_key.into(),
            model: model.into(),
        }
    }
}

#[async_trait]
impl LlmProvider for OpenAiCompatibleLlm {
    async fn complete(
        &self,
        req: CompletionRequest,
    ) -> Result<CompletionResponse, ProviderError> {
        let mut messages: Vec<serde_json::Value> = Vec::with_capacity(req.messages.len() + 1);
        if let Some(system) = req.system {
            messages.push(serde_json::json!({"role": "system", "content": system}));
        }
        for m in &req.messages {
            let role = match m.role {
                MessageRole::System => "system",
                MessageRole::User => "user",
                MessageRole::Assistant => "assistant",
            };
            messages.push(serde_json::json!({"role": role, "content": m.content}));
        }

        let body = serde_json::json!({
            "model": self.model,
            "messages": messages,
            "max_tokens": req.max_tokens,
            "temperature": req.temperature,
        });

        let url = format!("{}/chat/completions", self.base_url);
        let resp = self
            .client
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(map_reqwest_error)?;

        let status = resp.status();
        if !status.is_success() {
            return Err(map_status_error(status, resp).await);
        }

        let json: serde_json::Value = resp.json().await.map_err(map_reqwest_error)?;
        let content = json["choices"][0]["message"]["content"]
            .as_str()
            .ok_or_else(|| {
                ProviderError::new(
                    ProviderErrorKind::Server,
                    "response missing choices[0].message.content",
                    None,
                )
            })?
            .to_string();

        Ok(CompletionResponse { content })
    }

    fn name(&self) -> &str {
        "openai-compatible"
    }
}

fn map_reqwest_error(e: reqwest::Error) -> ProviderError {
    if e.is_connect() || e.is_timeout() {
        ProviderError::new(ProviderErrorKind::Network, e.to_string(), None)
    } else {
        ProviderError::new(ProviderErrorKind::Unknown, e.to_string(), None)
    }
}

async fn map_status_error(status: reqwest::StatusCode, resp: reqwest::Response) -> ProviderError {
    let code = status.as_u16();
    let kind = match code {
        401 | 403 => ProviderErrorKind::Auth,
        429 => ProviderErrorKind::RateLimit,
        500..=599 => ProviderErrorKind::Server,
        _ => ProviderErrorKind::Unknown,
    };
    let body = resp.text().await.unwrap_or_default();
    ProviderError::new(kind, format!("HTTP {code}: {body}"), Some(code))
}

/// Stub — replaced with a concrete struct in Task 4.
pub enum OpenAiCompatibleEmbed {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ChatMessage;
    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn llm(uri: String) -> OpenAiCompatibleLlm {
        OpenAiCompatibleLlm::new(uri, "test-key", "gpt-4o-mini")
    }

    #[tokio::test]
    async fn complete_returns_content_on_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(header("authorization", "Bearer test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "choices": [{
                    "message": {"role": "assistant", "content": "hello there"}
                }]
            })))
            .mount(&server)
            .await;

        let resp = llm(server.uri())
            .complete(CompletionRequest {
                system: Some("be brief".into()),
                messages: vec![ChatMessage {
                    role: MessageRole::User,
                    content: "hi".into(),
                }],
                max_tokens: Some(16),
                temperature: None,
            })
            .await
            .unwrap();
        assert_eq!(resp.content, "hello there");
    }

    #[tokio::test]
    async fn complete_maps_401_to_auth() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
            .mount(&server)
            .await;

        let err = llm(server.uri())
            .complete(CompletionRequest {
                system: None,
                messages: vec![ChatMessage {
                    role: MessageRole::User,
                    content: "hi".into(),
                }],
                max_tokens: None,
                temperature: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind, ProviderErrorKind::Auth);
        assert_eq!(err.status, Some(401));
    }

    #[tokio::test]
    async fn complete_maps_429_to_rate_limit() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(429))
            .mount(&server)
            .await;

        let err = llm(server.uri())
            .complete(CompletionRequest {
                system: None,
                messages: vec![ChatMessage {
                    role: MessageRole::User,
                    content: "hi".into(),
                }],
                max_tokens: None,
                temperature: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind, ProviderErrorKind::RateLimit);
    }

    #[tokio::test]
    async fn complete_maps_5xx_to_server() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let err = llm(server.uri())
            .complete(CompletionRequest {
                system: None,
                messages: vec![ChatMessage {
                    role: MessageRole::User,
                    content: "hi".into(),
                }],
                max_tokens: None,
                temperature: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind, ProviderErrorKind::Server);
        assert_eq!(err.status, Some(503));
    }

    #[tokio::test]
    async fn complete_maps_connection_failure_to_network() {
        // Point at a port that's not listening — reqwest returns a connection error.
        let llm = OpenAiCompatibleLlm::new(
            "http://127.0.0.1:1",
            "test-key",
            "gpt-4o-mini",
        );
        let err = llm
            .complete(CompletionRequest {
                system: None,
                messages: vec![ChatMessage {
                    role: MessageRole::User,
                    content: "hi".into(),
                }],
                max_tokens: None,
                temperature: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.kind, ProviderErrorKind::Network);
        assert!(err.status.is_none());
    }
}
