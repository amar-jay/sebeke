use std::{
    any,
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use dashmap::DashMap;
use moka::sync::Cache;
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use zenoh::{Session, bytes::Encoding};

use super::config::{Relay, WorkerConfig};
use async_trait::async_trait;
use axum::{
    extract::{State, Multipart},
    http::StatusCode,
    routing::post,
    Router,
};
use reqwest::multipart;

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
        let workers = match self.workers.read() {
            Ok(w) => w.clone(),
            Err(_) => return Err(anyhow!("worker list is poisoned")),
        };
        let session = self.session.clone();
        let client = self.client.clone();
        
        let session_for_axum = self.session.clone();

        tokio::spawn(async move {
            let app = Router::new()
                .route("/cloudflare/webhook", post(
                    |State(session): State<Arc<Session>>, mut multipart: Multipart| async move {
                        let mut success = false;
                        
                        while let Ok(Some(field)) = multipart.next_field().await {
                            let name = field.name().unwrap_or_default().to_string();
                            
                            if name == "payload" {
                                if let Ok(bytes) = field.bytes().await {
                                    if let Ok(data) = WorkerRelay::deserialize::<Vec<PullPayload>>(
                                        bytes.as_ref(),
                                        Encoding::APPLICATION_CBOR,
                                    ) {
                                        for payload in data {
                                            let _ = session.put(payload.topic, payload.data).await;
                                        }
                                        success = true;
                                    }
                                }
                            } else if name == "media" {
                                // Auxiliary support for multipart data
                                // Here, field.bytes().await could handle JPEG/Video buffers matching a mapped media topic 
                                if let Ok(bytes) = field.bytes().await {
                                    let _ = session.put("media/incoming", bytes).await;
                                }
                            }
                        }

                        if success {
                            StatusCode::OK
                        } else {
                            StatusCode::BAD_REQUEST
                        }
                    },
                ))
                .with_state(session_for_axum);

            if let Ok(listener) = tokio::net::TcpListener::bind("0.0.0.0:8080").await {
                let _ = axum::serve(listener, app).await;
            }
        });

        // Edge -> Cloudflare (Egress):
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
        
        let topic_cache: Arc<Cache<String, Vec<(String, super::config::CloudflareConfig)>>> = Arc::new(Cache::builder()
            .max_capacity(10_000)
            .time_to_idle(Duration::from_secs(5 * 60))
            .build());

        tokio::spawn(async move {
            while let Ok(sample) = subscriber.recv_async().await {
                let topic = sample.key_expr().as_str();
                let payload_bytes = sample.payload().to_bytes().into_owned();

                // Check cache for mapped Push URLs and Configs for this concrete topic
                let targets = if let Some(cached) = topic_cache.get(topic) {
                    cached.clone()
                } else {
                    let mut matched = Vec::new();
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
                                            let push_url = format!("{}{}", base_url, cfg.push_path);
                                            matched.push((push_url, cfg.clone()));
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // Memoize O(1) loop target
                    topic_cache.insert(topic.to_string(), matched.clone());
                    matched
                };

                for (push_url, cfg) in targets {
                    let push_payload = PushPayload {
                        machine_id: cfg.machine_id.clone(),
                        topic: topic.to_string(),
                        data: payload_bytes.clone(),
                    };

                    if let Ok(body) = WorkerRelay::serialize(&push_payload, Encoding::APPLICATION_CBOR) {
                        // Handle multipart push (CBOR info + optional media boundary support)
                        let part = multipart::Part::bytes(body)
                            .file_name("cbor_payload")
                            .mime_str("application/cbor")
                            .unwrap();
                            
                        let form = multipart::Form::new()
                            .text("machine_id", cfg.machine_id.clone())
                            .part("payload", part);

                        if let Err(e) = client_push
                            .post(&push_url)
                            .bearer_auth(&cfg.api_token)
                            .multipart(form)
                            .send()
                            .await 
                        {
                            println!("Error sending to Cloudflare: {}", e);
                        } else {
                            println!("Data sent to Cloudflare target: {}", push_url);
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
