use crate::daemon::Daemon;
use nomai_core::CoreError;
use serde_json::Value;

pub async fn list(_daemon: &Daemon, _params: Value) -> Result<Value, CoreError> {
    Err(CoreError::Config("not implemented".into()))
}
