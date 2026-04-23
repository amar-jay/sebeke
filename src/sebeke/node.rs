use std::{collections::HashSet, sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use serde::{Serialize, de::DeserializeOwned};
use zenoh::{bytes::Encoding, config::WhatAmI};

struct Node {
    session: zenoh::Session,
}

impl Node {
    pub async fn new() -> Self {
        let session = zenoh::open(zenoh::Config::default()).await.unwrap();
        let node = Self { session: session };
        node.log("Session established: {session:?}");
        node
    }

    fn log(&self, message: &str) {
        println!("[{}] {}", self.session.zid(), message);
    }

    async fn probe(timeout: Duration) -> bool {
        let receiver = zenoh::scout(WhatAmI::Peer | WhatAmI::Router, zenoh::Config::default())
            .await
            .unwrap();

        let mut unique_zids = HashSet::new();

        let scouting_result = tokio::time::timeout(timeout, async {
            while let Ok(hello) = receiver.recv_async().await {
                unique_zids.insert(hello.zid());

                //  only care about finding at least one,
                if !unique_zids.is_empty() {
                    return true;
                }
            }
            false
        })
        .await;

        match scouting_result {
            Ok(found) => found,
            Err(_) => !unique_zids.is_empty(),
        }
    }

    /// Discovers active topics (liveliness tokens) in the network.
    pub async fn get_topics(&self, prefix: Option<&str>) -> Result<HashSet<String>> {
        let mut topics = HashSet::new();
        let selector = prefix.unwrap_or("**");

        let replies = self
            .session
            .liveliness()
            .get(selector)
            .await
            .map_err(|e| anyhow!("Failed to query liveliness: {}", e))?;

        // The stream naturally ends when all known nodes have responded
        while let Ok(reply) = replies.recv_async().await {
            let Ok(sample) = reply.result() else { continue };
            let topic_name = sample.key_expr().to_string();
            topics.insert(topic_name);
        }

        Ok(topics)
    }

    fn serialize<T: Serialize>(&self, sample: &T, encoding: Encoding) -> Result<Vec<u8>> {
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

    pub async fn subscribe<F, Fut>(self: Arc<Self>, topic: &str, callback: F) -> Result<()>
    where
        F: Fn(Message) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let subscriber = self
            .session
            .declare_subscriber(topic)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to subscribe to {}: {}", topic, e))?;

        // Declare liveliness token so other nodes can discover this topic
        let liveliness = self.session.liveliness().declare_token(topic).await;

        self.log("Subscriber loop started for: {topic:?}");

        let self_clone = Arc::clone(&self);
        tokio::spawn(async move {
            // Keep liveliness alive as long as this loop runs
            let _keep_alive = liveliness;

            while let Ok(sample) = subscriber.recv_async().await {
                let bytes = sample.payload().to_bytes();

                // Assuming your deserialize function is available
                match Self::deserialize::<Message>(&bytes, sample.encoding().clone()) {
                    Ok(msg) => callback(msg).await,
                    Err(e) => {
                        self_clone.log(&format!(
                            "DROPPED: Malformed packet on topic '{}'. {}",
                            sample.key_expr(),
                            e
                        ));
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn publish<T: Serialize>(&self, topic: &str, message: &T) -> Result<()> {
        let bytes = self.serialize(message, Encoding::APPLICATION_CBOR)?;

        // .put() is the standard way to broadcast data in Zenoh
        self.session
            .put(topic, bytes)
            .await
            .map_err(|e| anyhow::anyhow!("Zenoh publish error on topic '{}': {}", topic, e))?;

        self.log(&format!("Published message to {}", topic));
        Ok(())
    }
}

#[derive(serde::Deserialize)]
struct Message {}
