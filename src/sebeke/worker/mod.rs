use std::collections::HashMap;

use anyhow::Result;

mod cloudflare; // cloudflare workers

// WHICH NAME IS BETTER? RELAY, PROXY, BRIDGE OR WORKER.

// all seen zenoh nodes -> bridge 
// bridge -> zenoh nodes 
trait Relay {
	fn new();
	/// Example: register_proxy("robot/camera/**", "https://aa.your-cloudflare-worker.com/robotica/camera/**")
	fn register_proxy(topic_pattern: &str, url_pattern: &str) -> Result<()>;
	/// Example: unregister_proxy("robot/camera/**")
	fn unregister_proxy(topic_pattern: &str);
	fn get_proxy_registry() -> Result<HashMap<String, Vec<String>>>;
	/// Example: bind_worker("https://aa.your-cloudflare-worker.com/robotica", {...prereqs...})
	async fn bind_worker(&self, base_url: &str, config: WorkerConfig) -> Result<()>;
	/// Example: unbind_worker("https://aa.your-cloudflare-worker.com/robotica")
	fn unbind_worker(base_url: &str) -> Result<()>;
	fn get_worker_list() -> &'static [&'static str];

	/// The main execution loop
	fn listen();
}

enum WorkerConfig {
    Cloudflare(CloudflareConfig),
    AWS(AWSConfig),
    Vercel(VercelConfig),
}
struct CloudflareConfig {}
struct AWSConfig {}
struct VercelConfig {}
