//! entry.* handlers. Populated in Task 6.

use serde_json::Value;

use nomai_core::CoreError;

use crate::daemon::Daemon;

pub async fn create(_daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
    Err(CoreError::Config("not implemented".into()))
}
pub async fn get(_daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
    Err(CoreError::Config("not implemented".into()))
}
pub async fn update(_daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
    Err(CoreError::Config("not implemented".into()))
}
pub async fn delete(_daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
    Err(CoreError::Config("not implemented".into()))
}
pub async fn list(_daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
    Err(CoreError::Config("not implemented".into()))
}
