//! JSON-RPC method-name constants, grouped by namespace.
//!
//! See spec §6 for the full method table. Reserved method names
//! (`search.hybrid`, `provider.set`) are present so the daemon can match
//! them and return `METHOD_NOT_FOUND` (`-32601`) cleanly.

pub mod entry {
    pub const CREATE: &str = "entry.create";
    pub const GET: &str = "entry.get";
    pub const UPDATE: &str = "entry.update";
    pub const DELETE: &str = "entry.delete";
    pub const LIST: &str = "entry.list";
}

pub mod search {
    pub const FULLTEXT: &str = "search.fulltext";
    pub const SEMANTIC: &str = "search.semantic";
    /// Reserved: returns `METHOD_NOT_FOUND` (-32601) in MVP.
    pub const HYBRID: &str = "search.hybrid";
}

pub mod qa {
    pub const ASK: &str = "qa.ask";
}

pub mod provider {
    pub const LIST: &str = "provider.list";
    /// Reserved: returns `METHOD_NOT_FOUND` (-32601) in MVP.
    pub const SET: &str = "provider.set";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_namespace_methods() {
        assert_eq!(entry::CREATE, "entry.create");
        assert_eq!(entry::GET, "entry.get");
        assert_eq!(entry::UPDATE, "entry.update");
        assert_eq!(entry::DELETE, "entry.delete");
        assert_eq!(entry::LIST, "entry.list");
    }

    #[test]
    fn search_namespace_methods() {
        assert_eq!(search::FULLTEXT, "search.fulltext");
        assert_eq!(search::SEMANTIC, "search.semantic");
        assert_eq!(search::HYBRID, "search.hybrid");
    }

    #[test]
    fn qa_and_provider_namespaces() {
        assert_eq!(qa::ASK, "qa.ask");
        assert_eq!(provider::LIST, "provider.list");
        assert_eq!(provider::SET, "provider.set");
    }
}
