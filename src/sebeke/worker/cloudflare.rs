use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use dashmap::DashMap;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use zenoh::{Session, bytes::Encoding};

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

impl WorkerRelay {
    fn serialize<T: Serialize>(sample: &T, encoding: Encoding) -> Result<Vec<u8>> {
        // 1. Pre-allocate a buffer with actual size
        let mut bytes = Vec::with_capacity(128);

        match encoding {
            Encoding::APPLICATION_CBOR => {
                ciborium::into_writer(sample, &mut bytes)
                    .map_err(|_| anyhow::anyhow!("Failed to serialize sample into CBOR format"))?;
                Ok(bytes)
            }
            _ => Err(anyhow!("Unsupported serialization format")),
        }
    }

    fn deserialize<T: DeserializeOwned>(bytes: &[u8], encoding: Encoding) -> Result<T> {
        match encoding {
            Encoding::APPLICATION_CBOR => {
                // ciborium::from_reader takes any type that implements std::io::Read.
                // A slice of bytes &[u8] implements Read perfectly.
                ciborium::from_reader(bytes).map_err(|_| {
                    anyhow!(
                        "Failed to deserialize CBOR bytes into the {} type",
                        std::any::type_name::<T>()
                    )
                })
            }
            _ => Err(anyhow!("Unsupported serialization format")),
        }
    }
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

    fn get_worker_list(&self) -> Vec<String> {
        self.workers
            .read()
            .map_err(|_| anyhow!("worker list is poisoned"))
            .expect("worker list lock poisoned")
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    async fn listen(&self) -> Result<()> {
        // Spawn a background task to periodically pull from workers
        let workers = match self.workers.read() {
            Ok(w) => w.clone(),
            Err(_) => return Err(anyhow!("worker list is poisoned")),
        };
        let session = self.session.clone();
        let client = self.client.clone();

        tokio::spawn(async move {
            loop {
                let worker_list: Vec<(String, WorkerConfig)> = workers
                    .iter()
                    .map(|entry| (entry.key().clone(), entry.value().clone()))
                    .collect();

                for (base_url, config) in worker_list {
                    if let WorkerConfig::Cloudflare(cfg) = config {
                        let pull_url = format!("{}{}", base_url, cfg.pull_path);
                        let timeout = Duration::from_millis(cfg.request_timeout_ms.max(500));

                        let res = client
                            .get(&pull_url)
                            .bearer_auth(&cfg.api_token)
                            .timeout(timeout)
                            .send()
                            .await;

                        if let Ok(response) = res {
                            if let Ok(bytes) = response.bytes().await {
                                if let Ok(data) = WorkerRelay::deserialize::<Vec<PullPayload>>(
                                    bytes.as_ref(),
                                    Encoding::APPLICATION_CBOR,
                                ) {
                                    for payload in data {
                                        let _ = session.put(payload.topic, payload.data).await;
                                    }
                                }
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        });

        // Subscribe to Zenoh topics to push to workers
        let subscriber = self
            .session
            .declare_subscriber("**")
            .await
            .map_err(|e| anyhow!("failed to subscribe: {}", e))?;
        let proxy_registry = match self.proxy_registry.read() {
            Ok(r) => r.clone(),
            Err(_) => return Err(anyhow!("proxy registry is poisoned")),
        };
        let workers_push = match self.workers.read() {
            Ok(w) => w.clone(),
            Err(_) => return Err(anyhow!("worker list is poisoned")),
        };
        let client_push = self.client.clone();

        tokio::spawn(async move {
            while let Ok(sample) = subscriber.recv_async().await {
                let topic = sample.key_expr().as_str();
                let payload_bytes = sample.payload().to_bytes().into_owned();

                let registry: Vec<(String, Vec<String>)> = proxy_registry
                    .iter()
                    .map(|entry| (entry.key().clone(), entry.value().clone()))
                    .collect();

                for (local_pattern, url_patterns) in registry {
                    for url_pattern in url_patterns {
                        if let Ok(resolved_url) =
                            super::utils::resolve_zenoh_url(&local_pattern, &url_pattern, topic)
                        {
                            // Find which worker matches this url pattern
                            for worker_entry in workers_push.iter() {
                                let base_url = worker_entry.key();
                                if resolved_url.starts_with(base_url) {
                                    if let WorkerConfig::Cloudflare(cfg) = worker_entry.value() {
                                        let push_payload = PushPayload {
                                            machine_id: cfg.machine_id.clone(),
                                            topic: topic.to_string(),
                                            data: payload_bytes.clone(),
                                        };

                                        let push_url = format!("{}{}", base_url, cfg.push_path);

                                        if let Ok(body) = WorkerRelay::serialize(
                                            &push_payload,
                                            Encoding::APPLICATION_CBOR,
                                        ) {
                                            let _ = client_push
                                                .post(&push_url)
                                                .bearer_auth(&cfg.api_token)
                                                .header("Content-Type", "application/cbor")
                                                .body(body)
                                                .send()
                                                .await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
struct PushPayload {
    machine_id: String,
    topic: String,
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize)]
struct PullPayload {
    topic: String,
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}
