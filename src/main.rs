pub mod node;

// Worker URL provided by Cloudflare
const WORKER_URL: &str = "https://cloudflare.abdelmanan-abdelrahman03.workers.dev";
const TOPIC: &str = "sensors/imu/1";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!(
        "hello world! worker_url: {worker_url}\ttopic:{topic}",
        worker_url = WORKER_URL,
        topic = TOPIC
    );
    Ok(())
}
