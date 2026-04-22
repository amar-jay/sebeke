use std::{collections::HashMap, sync::Arc};

use zenoh::Session;

pub struct WorkerRelay {
    /// The active Zenoh session
    session: Arc<Session>,
    
    /// The HTTP client used to "dispatch" data to the all the available workers
    client: reqwest::Client,

		worker_list: Vec<&'static str>, // to prevent the worker url from ever changing.
		proxy_registry: HashMap<String, Vec<&'static str>>,
}

#[derive(Clone)]
pub struct WorkerConfig {
    pub api_token: String,
    pub timeout_ms: u64,
}

impl Relay for WorkerRelay {

	fn new()->WorkerRelay{
        
				WorkerRelay { session: (), client: () }
	}

	fn register_proxy(topic_pattern: &str, url_pattern: &str) -> Result<()> {
		Ok(())
	}
	/// Example: unregister_proxy("robot/camera/**")
	fn unregister_proxy(topic_pattern: &str) {
	}

	fn get_proxy_registry() -> Result<HashMap<String, Vec<String>>> {

	}
	/// Example: bind_worker("https://aa.your-cloudflare-worker.com/robotica", {...prereqs...})
	async fn bind_worker(&self, base_url: &str, config: WorkerConfig) -> Result<()> {

	}

	/// Example: unbind_worker("https://aa.your-cloudflare-worker.com/robotica")
	fn unbind_worker(base_url: &str) -> Result<()> {

	}

	fn get_worker_list() -> &'static [&'static str] {

	}

	/// The main execution loop
	fn listen(){

	}
}

enum WorkerConfig {
    Cloudflare(CloudflareConfig),
    AWS(AWSConfig),
    Vercel(VercelConfig),
}
struct CloudflareConfig {}
struct AWSConfig {}
struct VercelConfig {}

