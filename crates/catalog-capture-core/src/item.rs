use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionKey {
    pub family: String,
    pub type_name: String,
    pub identifier: Option<String>,
    pub namespace: Option<String>,
}

impl PartitionKey {
    #[must_use]
    pub fn market_data(type_name: impl Into<String>, identifier: impl Display) -> Self {
        Self {
            family: "market_data".to_string(),
            type_name: type_name.into(),
            identifier: Some(identifier.to_string()),
            namespace: None,
        }
    }

    #[must_use]
    pub fn custom_data(
        type_name: impl Into<String>,
        identifier: Option<String>,
        topic: impl Into<String>,
    ) -> Self {
        Self {
            family: "custom_data".to_string(),
            type_name: type_name.into(),
            identifier,
            namespace: Some(topic.into()),
        }
    }

    #[must_use]
    pub fn stable_key(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.family,
            self.type_name,
            self.identifier.as_deref().unwrap_or("_"),
            self.namespace.as_deref().unwrap_or("_"),
        )
    }
}

#[derive(Debug, Clone)]
pub struct CaptureItem<T> {
    pub partition_key: PartitionKey,
    pub event_ts_ns: u64,
    pub init_ts_ns: Option<u64>,
    pub estimated_bytes: usize,
    pub payload: T,
}
