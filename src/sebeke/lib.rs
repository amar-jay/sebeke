mod node;
mod worker;
use std::sync::LazyLock;
use node::*;
use worker::*;


#[derive(Debug, Serialize, Deserialize)]
pub struct CoreConfig {
    pub namespace: String,
}

// --- 1. Global Configuration ---
// Using LazyLock (Rust 1.80+) to avoid once_cell
pub static GLOBAL_CONFIG: LazyLock<CoreConfig> = LazyLock<CoreConfig>::new(|| {
    // In a real app, you'd load this from a JSON file
    CoreConfig {
        namespace: "robotics/sebeke".to_string(),
    }
});


struct Session {}
// --- 3. The Core Infrastructure Handle ---
// This wraps Zenoh to make it "Serverless-first"
pub struct SebekeNode {
    pub session: Session,
    pub namespace: String,
}

impl SebekeNode {
    /// Initialize a new node and join the Zenoh network
    pub async fn new() -> Result<Self, zenoh::Error> {
        let session = zenoh::open(zenoh::config::Config::default()).await?;
        Ok(Self {
            session,
            namespace: GLOBAL_CONFIG.namespace.clone(),
        })
    }

    /// Helper to publish CBOR data easily
    pub async fn publish_media(&self, sub_path: &str, payload: RobotPayload) -> Result<(), Box<dyn std::error::Error>> {
        let path = format!("{}/{}", self.namespace, sub_path);
        
        // Serialize to CBOR (ciborium)
        let mut buffer = Vec::new();
        ciborium::into_writer(&payload, &mut buffer)?;

        // Send via Zenoh
        self.session.put(&path, buffer).res().await.map_err(|e| e.into())
    }
}