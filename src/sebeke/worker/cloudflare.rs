use std::{
	any, collections::HashMap, sync::{Arc, RwLock}, time::Duration
};

use anyhow::{Context, Result, anyhow, bail};
use dashmap::DashMap;
use serde::Serialize;
use zenoh::Session;

use super::config::{Relay, WorkerConfig};
use async_trait::async_trait;

#[derive(Debug, Clone, Serialize)]
struct BindPayload<'a> {
	machine_id: &'a str,
}

pub struct WorkerRelay {
	/// The active Zenoh session
	session: Arc<Session>,

	/// The HTTP client used to dispatch data to available workers
	client: reqwest::Client,

	proxy_registry: RwLock<DashMap<String, Vec<String>>>,
	workers: RwLock<DashMap<String, WorkerConfig>>,
}

#[async_trait]
impl Relay for WorkerRelay {
	fn new(session: Arc<Session>) -> WorkerRelay {
		Self {
			session,
			client: reqwest::Client::new(),
			proxy_registry: RwLock::new(DashMap::new()),
			workers: RwLock::new(DashMap::new()),
		}
	}

	fn register_proxy(&self, topic_pattern: &str, url_pattern: &str) -> Result<()> {
		let registry = self
			.proxy_registry
			.write()
			.map_err(|_| anyhow!("proxy registry is poisoned"))?;

		registry
			.entry(topic_pattern.to_string())
			.and_modify(|urls| urls.push(url_pattern.to_string()))
			.or_insert_with(|| vec![url_pattern.to_string()]);

		Ok(())
	}

	fn unregister_proxy(&self, topic_pattern: &str) -> Result<()> {
		let registry = self
			.proxy_registry
			.write()
			.map_err(|_| anyhow!("proxy registry is poisoned"))?;

		if registry.remove(topic_pattern).is_none() {
			bail!("no proxy found for the provided topic pattern")
		}

		Ok(())
	}

	fn get_proxy_registry(&self) -> HashMap<String, Vec<String>> {
		let registry = self
			.proxy_registry
			.read()
			.map_err(|_| anyhow!("proxy registry is poisoned"))
			.expect("proxy registry lock poisoned");

		registry
			.iter()
			.map(|e| (e.key().clone(), e.value().clone()))
			.collect()
	}

	async fn bind_worker(&self, base_url: &str, config: WorkerConfig) -> Result<()> {
		let cfg = match &config {
			WorkerConfig::Cloudflare(c) => c,
			_ => return Err(anyhow!("invalid config for cloudflare")),
		};

		if cfg.api_token.is_empty() {
			bail!("Cloudflare api_token cannot be empty")
		}

		if cfg.machine_id.is_empty() {
			bail!("Cloudflare machine_id cannot be empty")
		}

		self.client
			.post(format!("{}{}", base_url, cfg.bind_path))
			.bearer_auth(&cfg.api_token)
			.timeout(Duration::from_millis(cfg.request_timeout_ms))
			.json(&BindPayload {
				machine_id: &cfg.machine_id, // zenoh zid. will use better identifier in the future
			})
			.send()
			.await
			.context("failed to reach Cloudflare worker during bind")?
			.error_for_status()
			.context("Cloudflare worker rejected bind request")?;

		self.workers
			.write()
			.map_err(|_| anyhow!("worker list is poisoned"))?
			.insert(base_url.to_string(), config);

		Ok(())
	}

	async fn unbind_worker(&self, base_url: &str) -> Result<()> {
		let binding = self
			.workers
			.read()
			.map_err(|_| anyhow!("worker list is poisoned"))?
			.get(base_url)
			.ok_or_else(|| anyhow!("no active worker found for the provided base url"))?
			.clone();

		if let WorkerConfig::Cloudflare(binding) = binding {
			let unbind_url = format!("{}{}", base_url, binding.unbind_path);
			let timeout = Duration::from_millis(binding.request_timeout_ms.max(500));

			self.client
				.post(unbind_url)
				.bearer_auth(&binding.api_token)
				.timeout(timeout)
				.json(&BindPayload {
					machine_id: &binding.machine_id,
				})
				.send()
				.await
				.context("failed to reach Cloudflare worker during unbind")?
				.error_for_status()
				.context("Cloudflare worker rejected unbind request")?;

			self.workers
				.write()
				.map_err(|_| anyhow!("worker list is poisoned"))?
				.remove(base_url);
		}

		Ok(())
	}

	fn get_worker_list(&self) -> Vec<String>{
		self.workers
			.read()
			.map_err(|_| anyhow!("worker list is poisoned"))
			.expect("worker list lock poisoned")
			.iter()
			.map(|entry| entry.key().clone())
			.collect()
	}

	async fn listen(&self) -> Result<()> {
			// The main execution loop.
			// It forwards local Zenoh traffic to the configured worker and republishes
			// remote traffic back to the local Zenoh bus.

			// For simplicity, this is left as a placeholder. The actual implementation would involve:
			// 1. Subscribing to relevant Zenoh topics based on the proxy registry.
			// 2. For incoming messages, determining the appropriate worker(s) from the registry and forwarding the data.
			// 3. Handling responses from workers and republishing them to the Zenoh bus if necessary.
			Ok(())
	}
}