use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use zenoh::Session;

use super::config;

#[async_trait]
pub trait Relay: Send + Sync {
    fn get_default_config() -> RelayConfig {
        RelayConfig {
            cache_max_cap: 10_000,
            cache_ttl: Duration::from_secs(5 * 60),
        }
    }
    fn new(session: Arc<Session>, cfg: RelayConfig) -> Self
    where
        Self: Sized;

    /// Example: register_proxy("robot/camera/**", "https://aa.your-cloudflare-worker.com/robotica/camera/**")
    fn register_proxy(&self, topic_pattern: &str, url_pattern: &str) -> Result<()>;

    /// Example: unregister_proxy("robot/camera/**")
    fn unregister_proxy(&self, topic_pattern: &str) -> Result<()>;

    fn get_proxy_registry(&self) -> HashMap<String, Vec<String>>;

    /// Example: bind_worker("https://aa.your-cloudflare-worker.com/robotica", {...prereqs...})
    async fn bind_worker(&self, base_url: &str, config: config::WorkerConfig) -> Result<()>;

    /// Example: unbind_worker("https://aa.your-cloudflare-worker.com/robotica")
    async fn unbind_worker(&self, base_url: &str) -> Result<()>;

    fn get_worker_list(&self) -> Vec<String>;

    /// The main execution loop.
    /// It forwards local Zenoh traffic to the configured worker and republishes
    /// remote traffic back to the local Zenoh bus.
    async fn listen(&self) -> Result<()>;
}

pub struct RelayConfig {
    pub cache_max_cap: u64,
    pub cache_ttl: Duration,
}

#[derive(Clone, Debug)]
pub enum WorkerConfig {
    Cloudflare(CloudflareConfig),
    AWS(AWSConfig),
    Vercel(VercelConfig),
}

#[derive(Clone, Debug)]
pub struct CloudflareConfig {
    pub api_token: String,
    pub machine_id: String, // zenoh zid. will use better identifier in the future
    pub request_timeout_ms: u64,
    pub pull_interval_ms: u64,
    pub bind_path: String,
    pub unbind_path: String,
    pub push_path: String,
    pub pull_path: String,
    pub local_address: String,
    // pub ingress_url: String, is this really needed? can't we just use the local_address for callbacks?
}

impl Default for CloudflareConfig {
    fn default() -> Self {
        Self {
            api_token: String::new(),
            machine_id: String::new(),
            request_timeout_ms: 5_000,
            pull_interval_ms: 300,
            bind_path: "/bind".to_string(),
            unbind_path: "/unbind".to_string(),
            push_path: "/push".to_string(),
            pull_path: "/pull".to_string(),
            local_address: "0.0.0.0:8787".to_string(),
            // ingress_url: "http://localhost:8787".to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AWSConfig {}

#[derive(Clone, Debug)]
pub struct VercelConfig {}
