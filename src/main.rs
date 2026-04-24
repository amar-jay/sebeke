pub mod node;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use zenoh::bytes::Encoding;

use sebeke::http::{
    cloudflare::WorkerRelay,
    config::{self, Relay},
};

// Worker URL provided by Cloudflare
const WORKER_URL: &str = "https://cloudflare.abdelmanan-abdelrahman03.workers.dev";
const TOPIC: &str = "sensors/imu/1";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Initialize Zenoh Session
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let session = Arc::new(session);

    let workerd = WorkerRelay::new(session.clone(), WorkerRelay::get_default_config());

    // 2. Bind the Cloudflare Worker to the Edge process
    println!(
        "🔌 Binding edge router to Cloudflare worker: {}",
        WORKER_URL
    );
    workerd
        .bind_worker(
            WORKER_URL,
            config::WorkerConfig::Cloudflare(config::CloudflareConfig {
                api_token: "local-dev-token".to_string(), // In production, pass an actual security token
                machine_id: "edge-test-machine-01".to_string(),
                push_path: "/".to_string(), // Egress POST path on worker
                ..Default::default()
            }),
        )
        .await
        .unwrap();

    // 3. Register a Proxy rule
    // Matches any topic under "sensors/" and forwards it to the root of the worker
    println!(
        "🔗 Registering proxy mapping: sensors/** -> {}/",
        WORKER_URL
    );
    workerd
        .register_proxy("sensors/**", &format!("{}/", WORKER_URL))
        .unwrap();

    // 4. Start the background listeners for Egress push and Ingress pull/webhooks
    println!("📡 Starting worker multiplexer listener...");
    workerd.listen().await.unwrap();

    // Give listeners a brief moment to boot
    sleep(Duration::from_millis(500)).await;

    // 5. Publish Telemetry via Zenoh
    let message = "Test IMU Vector Data X=0 Y=1 Z=0";
    println!(
        "🚀 Publishing simulated payload to Zenoh topic '{}' -> {}",
        TOPIC, message
    );

    // As per `WorkerRelay` logic, any `Vec<u8>` put on Zenoh is grabbed by `listen` and then mapped.
    session
        .put(TOPIC, message)
        .encoding(Encoding::TEXT_PLAIN) // Data bytes inner payload mapping
        .await
        .unwrap();

    // 6. Give the background tokio tasks a second to actually POST this multipart CBOR form
    sleep(Duration::from_secs(2)).await;

    // 7. Verify the cloudflare end received it successfully via the GET/Test API
    println!("🔍 Polling Cloudflare Worker to verify edge telemetry arrived...");
    let reqwest_client = reqwest::Client::new();
    let get_resp = reqwest_client
        .get(&format!("{}/data?topic={}", WORKER_URL, TOPIC))
        .send()
        .await?;

    if get_resp.status().is_success() {
        let result = get_resp.text().await?;
        println!("✅ Test Passed! Cloudflare responded: \n{}", result);
    } else {
        println!(
            "❌ Test Failed! Worker responded: \n{}",
            get_resp.text().await?
        );
    }

    Ok(())
}
