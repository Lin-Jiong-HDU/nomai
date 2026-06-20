//! OpenAI-compatible provider implementations.

use async_trait::async_trait;
use reqwest::Client;

use crate::error::{ProviderError, ProviderErrorKind};
use crate::traits::{EmbeddingProvider, LlmProvider};
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

pub struct OpenAiCompatibleEmbed {
    client: Client,
    base_url: String,
    api_key: String,
    model: String,
    dim: usize,
}

impl OpenAiCompatibleEmbed {
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
        dim: usize,
    ) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: api_key.into(),
            model: model.into(),
            dim,
        }
    }
}

#[async_trait]
impl EmbeddingProvider for OpenAiCompatibleEmbed {
    async fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, ProviderError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let body = serde_json::json!({
            "model": self.model,
            "input": texts,
        });

        let url = format!("{}/embeddings", self.base_url);
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
        let data = json["data"].as_array().ok_or_else(|| {
            ProviderError::new(
                ProviderErrorKind::Server,
                "response missing data array",
                None,
            )
        })?;

        let mut out = Vec::with_capacity(data.len());
        for item in data {
            let arr = item["embedding"].as_array().ok_or_else(|| {
                ProviderError::new(
                    ProviderErrorKind::Server,
                    "response item missing embedding",
                    None,
                )
            })?;
            let v: Vec<f32> = arr
                .iter()
                .map(|x| x.as_f64().unwrap_or(0.0) as f32)
                .collect();
            out.push(v);
        }
        Ok(out)
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn name(&self) -> &str {
        "openai-compatible"
    }
}

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

    use crate::traits::EmbeddingProvider;

    fn embed(uri: String) -> OpenAiCompatibleEmbed {
        OpenAiCompatibleEmbed::new(uri, "test-key", "text-embedding-3-small", 1536)
    }

    #[tokio::test]
    async fn embed_returns_vectors_in_input_order() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .and(header("authorization", "Bearer test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [
                    {"index": 0, "embedding": [0.1, 0.2, 0.3]},
                    {"index": 1, "embedding": [0.4, 0.5, 0.6]}
                ]
            })))
            .mount(&server)
            .await;

        let vecs = embed(server.uri())
            .embed(&["first", "second"])
            .await
            .unwrap();
        assert_eq!(vecs.len(), 2);
        assert_eq!(vecs[0], vec![0.1_f32, 0.2, 0.3]);
        assert_eq!(vecs[1], vec![0.4_f32, 0.5, 0.6]);
    }

    #[tokio::test]
    async fn embed_empty_input_returns_empty_without_http() {
        let server = MockServer::start().await;
        // No mock mounted: if the impl hits the network, this test fails.
        let vecs = embed(server.uri()).embed(&[]).await.unwrap();
        assert!(vecs.is_empty());
    }

    #[tokio::test]
    async fn embed_maps_401_to_auth() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/embeddings"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let err = embed(server.uri())
            .embed(&["x"])
            .await
            .unwrap_err();
        assert_eq!(err.kind, ProviderErrorKind::Auth);
    }

    #[tokio::test]
    async fn embed_returns_dim_from_config() {
        let e = embed("http://unused.example".into());
        assert_eq!(e.dim(), 1536);
    }
}
