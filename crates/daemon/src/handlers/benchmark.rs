//! Development-only benchmark lifecycle RPC handlers.

use async_trait::async_trait;
use nomai_core::CoreError;
use nomai_protocol::method::benchmark::{ABORT, FINISH, NEXT_CASE, RECORD_ANSWER, START, STATUS};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;

use crate::benchmark::{BenchmarkRuntime, StatusResult};
use crate::daemon::Daemon;
use crate::rpc::RpcHandler;

fn runtime(daemon: &Daemon) -> Result<&BenchmarkRuntime, CoreError> {
    daemon
        .benchmark
        .as_deref()
        .ok_or_else(|| CoreError::Validation("benchmark mode is disabled".into()))
}

fn runtime_handle(daemon: &Daemon) -> Result<Arc<BenchmarkRuntime>, CoreError> {
    daemon
        .benchmark
        .clone()
        .ok_or_else(|| CoreError::Validation("benchmark mode is disabled".into()))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct StartParams {
    pub suite_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RunIdParams {
    pub run_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RecordAnswerParams {
    pub run_id: String,
    pub case_id: String,
    pub answer: String,
}

pub struct Start;

#[async_trait]
impl RpcHandler for Start {
    fn method(&self) -> &'static str {
        START
    }

    fn is_mutating(&self) -> bool {
        true
    }

    fn description(&self) -> &'static str {
        "Start one development benchmark run and load its temporary fixtures."
    }

    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(StartParams).to_value())
    }

    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let params: StartParams = serde_json::from_value(params)
            .map_err(|error| CoreError::Validation(format!("invalid params: {error}")))?;
        let runtime = runtime_handle(daemon)?;
        let suite_id = params.suite_id;
        let result = tokio::task::spawn_blocking(move || runtime.start(&suite_id))
            .await
            .map_err(|error| {
                CoreError::Config(format!("benchmark start task failed: {error}"))
            })??;
        daemon.search_cache.clear();
        daemon.search_cache.bump_generation();
        Ok(json!({
            "run_id": result.run_id,
            "suite_id": result.suite_id,
            "case_count": result.case_ids.len(),
            "provider": {
                "name": daemon.llm.name(),
                "embedding_model": daemon.embedding_model,
                "llm_model": daemon.llm_model,
            }
        }))
    }
}

pub struct NextCase;

#[async_trait]
impl RpcHandler for NextCase {
    fn method(&self) -> &'static str {
        NEXT_CASE
    }

    fn description(&self) -> &'static str {
        "Return the next benchmark question without exposing its reference answer or gold IDs."
    }

    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(RunIdParams).to_value())
    }

    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let params: RunIdParams = serde_json::from_value(params)
            .map_err(|error| CoreError::Validation(format!("invalid params: {error}")))?;
        serde_json::to_value(runtime(daemon)?.next_case(&params.run_id)?)
            .map_err(|error| CoreError::Config(format!("serialize benchmark case: {error}")))
    }
}

pub struct RecordAnswer;

#[async_trait]
impl RpcHandler for RecordAnswer {
    fn method(&self) -> &'static str {
        RECORD_ANSWER
    }

    fn description(&self) -> &'static str {
        "Record the model answer for the current benchmark case and return its metrics."
    }

    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(RecordAnswerParams).to_value())
    }

    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let params: RecordAnswerParams = serde_json::from_value(params)
            .map_err(|error| CoreError::Validation(format!("invalid params: {error}")))?;
        let report = runtime(daemon)?
            .record_answer(
                &params.run_id,
                &params.case_id,
                params.answer,
                daemon.llm.as_ref(),
            )
            .await?;
        Ok(json!({
            "case_id": report.case_id,
            "metrics": report.metrics,
        }))
    }
}

pub struct Finish;

#[async_trait]
impl RpcHandler for Finish {
    fn method(&self) -> &'static str {
        FINISH
    }

    fn is_mutating(&self) -> bool {
        true
    }

    fn description(&self) -> &'static str {
        "Finish the benchmark run, compare its read-only Git baseline, and remove fixtures."
    }

    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(RunIdParams).to_value())
    }

    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let params: RunIdParams = serde_json::from_value(params)
            .map_err(|error| CoreError::Validation(format!("invalid params: {error}")))?;
        let runtime = runtime_handle(daemon)?;
        let run_id = params.run_id.clone();
        let report = tokio::task::spawn_blocking(move || runtime.finish(&run_id))
            .await
            .map_err(|error| {
                CoreError::Config(format!("benchmark finish task failed: {error}"))
            })??;
        daemon.search_cache.clear();
        daemon.search_cache.bump_generation();
        let mut output = serde_json::to_value(report)
            .map_err(|error| CoreError::Config(format!("serialize benchmark report: {error}")))?;
        output["run_id"] = Value::String(params.run_id);
        Ok(output)
    }
}

pub struct Abort;

#[async_trait]
impl RpcHandler for Abort {
    fn method(&self) -> &'static str {
        ABORT
    }

    fn is_mutating(&self) -> bool {
        true
    }

    fn description(&self) -> &'static str {
        "Abort the benchmark run and remove all temporary fixtures."
    }

    fn input_schema(&self) -> Option<Value> {
        Some(schemars::schema_for!(RunIdParams).to_value())
    }

    async fn call(&self, daemon: &Daemon, params: Value) -> Result<Value, CoreError> {
        let params: RunIdParams = serde_json::from_value(params)
            .map_err(|error| CoreError::Validation(format!("invalid params: {error}")))?;
        let runtime = runtime_handle(daemon)?;
        let run_id = params.run_id.clone();
        let result = tokio::task::spawn_blocking(move || runtime.abort(&run_id))
            .await
            .map_err(|error| {
                CoreError::Config(format!("benchmark abort task failed: {error}"))
            })??;
        daemon.search_cache.clear();
        daemon.search_cache.bump_generation();
        Ok(json!({
            "run_id": result.run_id,
            "aborted": true,
            "deleted_entry_count": result.deleted_ids.len(),
        }))
    }
}

pub struct Status;

#[async_trait]
impl RpcHandler for Status {
    fn method(&self) -> &'static str {
        STATUS
    }

    fn description(&self) -> &'static str {
        "Return the current development benchmark lifecycle state."
    }

    fn input_schema(&self) -> Option<Value> {
        Some(json!({"type": "object", "properties": {}, "additionalProperties": false}))
    }

    async fn call(&self, daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
        let status: StatusResult = runtime(daemon)?.status();
        Ok(json!({
            "enabled": true,
            "run_id": status.run_id,
            "case_id": status.case_id,
            "state": if status.active { "running" } else { "idle" },
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accepts(schema: &Value, value: &Value) -> bool {
        jsonschema::validator_for(schema).unwrap().is_valid(value)
    }

    #[test]
    fn benchmark_schemas_require_only_the_documented_inputs() {
        let start = Start.input_schema().unwrap();
        assert!(!accepts(&start, &json!({})));
        assert!(accepts(&start, &json!({"suite_id": "suite"})));

        let next = NextCase.input_schema().unwrap();
        assert!(!accepts(&next, &json!({})));
        assert!(accepts(&next, &json!({"run_id": "run"})));

        let answer = RecordAnswer.input_schema().unwrap();
        assert!(!accepts(&answer, &json!({"run_id": "run"})));
        assert!(accepts(
            &answer,
            &json!({"run_id": "run", "case_id": "case", "answer": "answer"})
        ));
    }

    #[test]
    fn status_schema_rejects_unexpected_fields() {
        let schema = Status.input_schema().unwrap();
        assert!(accepts(&schema, &json!({})));
        assert!(!accepts(&schema, &json!({"run_id": "run"})));
    }
}
