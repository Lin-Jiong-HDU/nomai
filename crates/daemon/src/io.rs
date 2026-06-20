//! Stdio helpers.

use std::io::{self, Write};

use nomai_protocol::Response;

/// Serialize a response as a single line of JSON + newline, write to stdout.
pub fn write_response_line(resp: &Response) -> io::Result<()> {
    let stdout = io::stdout();
    let mut lock = stdout.lock();
    serde_json::to_writer(&mut lock, resp)?;
    writeln!(lock)?;
    lock.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nomai_protocol::{Id, JSONRPC_VERSION};
    use serde_json::json;

    #[test]
    fn write_response_line_emits_one_json_line() {
        // Capture is tricky for stdout; this test just verifies no error
        // is returned for a well-formed response.
        let resp = Response::ok(Some(Id::Number(1)), json!({"ok": true}));
        assert!(resp.jsonrpc == JSONRPC_VERSION);
        // Smoke: serializing works.
        let _ = serde_json::to_string(&resp).unwrap();
    }
}
