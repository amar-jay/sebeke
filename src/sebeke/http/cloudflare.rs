use std::{collections::HashMap, sync::Arc, time::Duration};
use tracing::{error, info, warn};

use anyhow::{Context, Result, anyhow, bail};
use dashmap::DashMap;
use moka::sync::Cache;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use zenoh::{Session, bytes::Encoding};

use crate::http::config::{CloudflareConfig, RelayConfig};

use super::config::{Relay, WorkerConfig};
use async_trait::async_trait;
use axum::{
    Router,
    extract::{Multipart, State},
    http::StatusCode,
    routing::post,
};
use reqwest::multipart;

#[derive(Debug, Clone, Serialize)]
struct BindPayload<'a> {
    machine_id: &'a str,
    ingress_url: &'a str,
}

pub struct WorkerRelay {
    /// The active Zenoh session
    session: Arc<Session>,

    /// The HTTP client used to dispatch data to available workers
    client: reqwest::Client,

    proxy_registry: Arc<DashMap<String, Vec<String>>>,
    workers: Arc<DashMap<String, WorkerConfig>>,

    /// Memoizes egress target resolution per concrete topic.
    /// Invalidated whenever the worker or proxy registry changes.
    topic_cache: Arc<Cache<String, Vec<(String, CloudflareConfig)>>>,
}

impl WorkerRelay {
    fn serialize<T: Serialize>(value: &T, encoding: Encoding) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(128);
        match encoding {
            Encoding::APPLICATION_CBOR => {
                ciborium::into_writer(value, &mut buf)
                    .map_err(|e| anyhow!("CBOR serialization failed: {e}"))?;
                Ok(buf)
            }
            _ => Err(anyhow!("unsupported serialization encoding")),
        }
    }

    fn deserialize<T: DeserializeOwned>(bytes: &[u8], encoding: Encoding) -> Result<T> {
        match encoding {
            Encoding::APPLICATION_CBOR => ciborium::from_reader(bytes).map_err(|e| {
                anyhow!(
                    "CBOR deserialization into {} failed: {e}",
                    std::any::type_name::<T>()
                )
            }),
            _ => Err(anyhow!("unsupported serialization encoding")),
        }
    }
}

#[async_trait]
impl Relay for WorkerRelay {
    fn new(session: Arc<Session>, cfg: RelayConfig) -> WorkerRelay {
        let default = Self::get_default_config();

        // Determine values once
        let max_cap = if cfg.cache_max_cap == 0 {
            default.cache_max_cap
        } else {
            cfg.cache_max_cap
        };
        let ttl = if cfg.cache_ttl.is_zero() {
            default.cache_ttl
        } else {
            cfg.cache_ttl
        };
        Self {
            session,
            client: reqwest::Client::new(),
            proxy_registry: Arc::new(DashMap::new()),
            workers: Arc::new(DashMap::new()),
            topic_cache: Arc::new(
                Cache::builder()
                    .max_capacity(max_cap)
                    .time_to_idle(ttl)
                    .build(),
            ),
        }
    }

    fn register_proxy(&self, topic_pattern: &str, url_pattern: &str) -> Result<()> {
        self.proxy_registry
            .entry(topic_pattern.to_string())
            .and_modify(|urls| urls.push(url_pattern.to_string()))
            .or_insert_with(|| vec![url_pattern.to_string()]);

        // New proxy mapping may affect cached resolutions
        self.topic_cache.invalidate_all();
        Ok(())
    }

    fn unregister_proxy(&self, topic_pattern: &str) -> Result<()> {
        if self.proxy_registry.remove(topic_pattern).is_none() {
            bail!("no proxy found for topic pattern: {topic_pattern}");
        }
        self.topic_cache.invalidate_all();
        Ok(())
    }

    fn get_proxy_registry(&self) -> HashMap<String, Vec<String>> {
        self.proxy_registry
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    async fn bind_worker(&self, base_url: &str, config: WorkerConfig) -> Result<()> {
        let cfg = match &config {
            WorkerConfig::Cloudflare(c) => c,
            _ => bail!("bind_worker only supports Cloudflare configs"),
        };

        if cfg.api_token.is_empty() {
            bail!("Cloudflare api_token cannot be empty");
        }
        if cfg.machine_id.is_empty() {
            bail!("Cloudflare machine_id cannot be empty");
        }

        let mut ingress_url = cfg.ingress_url.clone();
        if ingress_url.is_empty() {
            ingress_url = format!("http://{}", cfg.local_address);
        }

        self.client
            .post(format!("{}{}", base_url, cfg.bind_path))
            .bearer_auth(&cfg.api_token)
            .timeout(Duration::from_millis(cfg.request_timeout_ms))
            .json(&BindPayload {
                machine_id: &cfg.machine_id,
                ingress_url: &ingress_url,
            })
            .send()
            .await
            .context("failed to reach Cloudflare worker during bind")?
            .error_for_status()
            .context("Cloudflare worker rejected bind request")?;

        self.workers.insert(base_url.to_string(), config);
        // A new worker may be a valid target for cached topics
        self.topic_cache.invalidate_all();

        info!(base_url, "worker bound");
        Ok(())
    }
    async fn unbind_worker(&self, base_url: &str) -> Result<()> {
        // Atomically remove so there is no window where the worker is absent
        // from the map but has not yet been told to unbind
        let (_, binding) = self
            .workers
            .remove(base_url)
            .ok_or_else(|| anyhow!("no active worker for: {base_url}"))?;

        self.topic_cache.invalidate_all();

        if let WorkerConfig::Cloudflare(cfg) = binding {
            let mut ingress_url = cfg.ingress_url.clone();
            if ingress_url.is_empty() {
                ingress_url = format!("http://{}", cfg.local_address);
            }

            self.client
                .post(format!("{}{}", base_url, cfg.unbind_path))
                .bearer_auth(&cfg.api_token)
                .timeout(Duration::from_millis(cfg.request_timeout_ms.max(500)))
                .json(&BindPayload {
                    machine_id: &cfg.machine_id,
                    ingress_url: &ingress_url,
                })
                .send()
                .await
                .context("failed to reach Cloudflare worker during unbind")?
                .error_for_status()
                .context("Cloudflare worker rejected unbind request")?;
        }

        info!(base_url, "worker unbound");
        Ok(())
    }

    fn get_worker_list(&self) -> Vec<String> {
        self.workers
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Starts ingress (Cloudflare → Zenoh) servers and the egress
    /// (Zenoh → Cloudflare) subscriber loop. Returns once both are running.
    async fn listen(&self) -> Result<()> {
        self.spawn_ingress_servers().await?;
        self.spawn_egress_loop().await?;
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

impl WorkerRelay {
    /// Collects all bound Cloudflare workers, groups them by `local_address`,
    /// and spawns one Axum server per unique address. Binding is done before
    /// spawning so bind errors are returned to the caller rather than logged
    /// inside a task.
    async fn spawn_ingress_servers(&self) -> Result<()> {
        // Group configs by the local address they should listen on.
        // Multiple workers can share an address — they get distinct routes
        // via their individual `bind_path` values.
        let mut by_address: HashMap<String, Vec<CloudflareConfig>> = HashMap::new();

        for entry in self.workers.iter() {
            if let WorkerConfig::Cloudflare(cfg) = entry.value() {
                by_address
                    .entry(cfg.local_address.clone())
                    .or_default()
                    .push(cfg.clone());
            }
        }

        if by_address.is_empty() {
            bail!("listen() called with no Cloudflare workers bound");
        }

        for (local_address, cfgs) in by_address {
            let router = Self::build_ingress_router(&cfgs, self.session.clone());

            // Bind eagerly — error here, not buried in a spawned task
            let listener = tokio::net::TcpListener::bind(&local_address)
                .await
                .with_context(|| format!("failed to bind ingress listener on {local_address}"))?;

            info!(
                address = %local_address,
                routes = cfgs.len(),
                "ingress server listening"
            );

            tokio::spawn(async move {
                if let Err(e) = axum::serve(listener, router).await {
                    error!(address = %local_address, error = %e, "ingress server exited");
                }
            });
        }

        Ok(())
    }

    /// Builds an Axum `Router` with one POST route per Cloudflare config's
    /// `bind_path`. All routes share the same Zenoh session via Axum state.
    fn build_ingress_router(cfgs: &[CloudflareConfig], session: Arc<Session>) -> Router {
        let mut router = Router::new();

        for cfg in cfgs {
            info!(bind_path = %cfg.bind_path, "registering ingress route");
            router = router.route(&cfg.bind_path, post(Self::handle_pull_request));
        }

        router.with_state(session)
    }

    /// Axum handler for inbound multipart payloads from Cloudflare.
    ///
    /// Expected fields:
    /// - `payload` — CBOR-encoded `Vec<PullPayload>`, forwarded into Zenoh
    /// - `media`   — raw bytes published to `media/incoming`
    async fn handle_pull_request(
        State(session): State<Arc<Session>>,
        mut multipart: Multipart,
    ) -> StatusCode {
        let mut any_payload_ok = false;

        while let Ok(Some(field)) = multipart.next_field().await {
            match field.name().unwrap_or_default() {
                "payload" => {
                    any_payload_ok = Self::ingest_payload_field(field, &session).await; // does this overwrite if so use |=
                }
                "media" => {
                    Self::ingest_media_field(field, &session).await;
                }
                unknown => {
                    warn!(field = %unknown, "ignoring unknown multipart field");
                }
            }
        }

        if any_payload_ok {
            StatusCode::OK
        } else {
            StatusCode::BAD_REQUEST
        }
    }

    /// Reads the `payload` multipart field, deserializes the CBOR batch, and
    /// publishes each entry into the Zenoh session. Returns `true` if at least
    /// one entry was published without error.
    async fn ingest_payload_field(
        field: axum::extract::multipart::Field<'_>,
        session: &Arc<Session>,
    ) -> bool {
        let bytes = match field.bytes().await {
            Ok(b) => b,
            Err(e) => {
                error!(error = %e, "failed to read payload field bytes");
                return false;
            }
        };

        let payloads =
            match Self::deserialize::<Vec<PullPayload>>(bytes.as_ref(), Encoding::APPLICATION_CBOR)
            {
                Ok(p) => p,
                Err(e) => {
                    error!(error = %e, "failed to deserialize pull payload");
                    return false;
                }
            };

        let mut all_ok = true;
        for p in payloads {
            if let Err(e) = session.put(p.topic.clone(), p.data).await {
                error!(topic = %p.topic, error = %e, "session.put failed");
                all_ok = false;
            }
        }
        all_ok
    }

    /// Reads the `media` multipart field and publishes raw bytes to
    /// `media/incoming` in the Zenoh session.
    async fn ingest_media_field(
        field: axum::extract::multipart::Field<'_>,
        session: &Arc<Session>,
    ) {
        match field.bytes().await {
            Ok(bytes) => {
                if let Err(e) = session.put("media/incoming", bytes).await {
                    error!(error = %e, "session.put(media/incoming) failed");
                }
            }
            Err(e) => error!(error = %e, "failed to read media field bytes"),
        }
    }

    /// Declares a Zenoh `**` subscriber and spawns a task that forwards
    /// every received sample to all matching Cloudflare workers.
    async fn spawn_egress_loop(&self) -> Result<()> {
        let subscriber = self
            .session
            .declare_subscriber("**")
            .await
            .map_err(|e| anyhow!("failed to declare Zenoh subscriber: {e}"))?;

        // Clone Arcs — the live maps are shared, not snapshotted, so workers
        // bound after listen() starts are automatically visible.
        let proxy_registry = self.proxy_registry.clone();
        let workers = self.workers.clone();
        let client = self.client.clone();
        let topic_cache = self.topic_cache.clone();

        tokio::spawn(async move {
            while let Ok(sample) = subscriber.recv_async().await {
                let topic = sample.key_expr().as_str().to_string();
                let data = sample.payload().to_bytes().into_owned();

                let targets =
                    Self::resolve_egress_targets(&topic, &proxy_registry, &workers, &topic_cache);

                if targets.is_empty() {
                    continue;
                }

                for (push_url, cfg) in targets {
                    match Self::push_to_cloudflare(&client, &push_url, &cfg, &topic, data.clone())
                        .await
                    {
                        Ok(()) => info!(url = %push_url, topic = %topic, "egress push ok"),
                        Err(e) => {
                            error!(url = %push_url, topic = %topic, error = %e, "egress push failed")
                        }
                    }
                }
            }

            warn!("egress subscriber loop terminated");
        });

        Ok(())
    }

    /// Returns the list of `(push_url, config)` pairs that a sample on
    /// `topic` should be forwarded to, using the cache to avoid repeating
    /// the O(proxies × workers) lookup on every message.
    fn resolve_egress_targets(
        topic: &str,
        proxy_registry: &Arc<DashMap<String, Vec<String>>>,
        workers: &Arc<DashMap<String, WorkerConfig>>,
        cache: &Arc<Cache<String, Vec<(String, CloudflareConfig)>>>,
    ) -> Vec<(String, CloudflareConfig)> {
        if let Some(cached) = cache.get(topic) {
            return cached;
        }

        let mut matched: Vec<(String, CloudflareConfig)> = Vec::new();

        for proxy_entry in proxy_registry.iter() {
            let local_pattern = proxy_entry.key();
            for url_pattern in proxy_entry.value().iter() {
                let resolved_url =
                    match super::utils::resolve_zenoh_url(local_pattern, url_pattern, topic) {
                        Ok(u) => u,
                        Err(e) => {
                            warn!(
                                local = %local_pattern,
                                url = %url_pattern,
                                topic = %topic,
                                error = %e,
                                "could not resolve proxy URL"
                            );
                            continue;
                        }
                    };

                for worker_entry in workers.iter() {
                    let base_url = worker_entry.key();
                    if resolved_url.starts_with(base_url.as_str()) {
                        if let WorkerConfig::Cloudflare(cfg) = worker_entry.value() {
                            let push_url = format!("{}{}", base_url, cfg.push_path);
                            matched.push((push_url, cfg.clone()));
                        }
                    }
                }
            }
        }

        cache.insert(topic.to_string(), matched.clone());
        matched
    }

    /// Serializes `topic` + `data` into a CBOR multipart body and POSTs it
    /// to a single Cloudflare worker, respecting the worker's configured
    /// request timeout.
    async fn push_to_cloudflare(
        client: &reqwest::Client,
        push_url: &str,
        cfg: &CloudflareConfig,
        topic: &str,
        data: Vec<u8>,
    ) -> Result<()> {
        let payload = PushPayload {
            machine_id: cfg.machine_id.clone(),
            topic: topic.to_string(),
            data,
        };

        let body = Self::serialize(&payload, Encoding::APPLICATION_CBOR)
            .context("failed to serialize push payload")?;

        let part = multipart::Part::bytes(body)
            .file_name("cbor_payload")
            .mime_str("application/cbor")
            .map_err(|e| anyhow!("invalid MIME type: {e}"))?;

        let form = multipart::Form::new()
            .text("machine_id", cfg.machine_id.clone())
            .part("payload", part);

        client
            .post(push_url)
            .bearer_auth(&cfg.api_token)
            .timeout(Duration::from_millis(cfg.request_timeout_ms))
            .multipart(form)
            .send()
            .await
            .with_context(|| format!("failed to reach worker at {push_url}"))?
            .error_for_status()
            .with_context(|| format!("worker at {push_url} rejected push"))?;

        Ok(())
    }
}
