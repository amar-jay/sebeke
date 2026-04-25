mod camera_cli;
mod image;
mod utils;
mod video;
use camera_cli::*;
use image::*;
use utils::*;
use video::*;

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail};
use axum::{
    Router,
    extract::State,
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use clap::Parser;
use sebeke::relay::{
    worker::WorkerRelay,
    config::{self, Relay},
};
use tokio::{net::TcpListener, sync::RwLock};

#[path = "../../src/node/mod.rs"]
pub mod node;
use node::Node;

const WORKER_URL: &str = "https://cloudflare.abdelmanan-abdelrahman03.workers.dev";

#[derive(Clone)]
struct WebState {
    latest: Arc<RwLock<Option<CameraFrame>>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let cli = Cli::parse();
    let node = Arc::new(Node::new().await);
    maybe_start_relay(node.clone(), &cli).await?;

    match cli.command {
        CameraCommand::Publish(args) => publish_frames(node, &cli.topic, args).await,
        CameraCommand::Receive {
            out_dir,
            print_meta,
            web_host,
            web_port,
        } => receive_frames(node, &cli.topic, out_dir, print_meta, web_host, web_port).await,
    }
}

async fn maybe_start_relay(node: Arc<Node>, cli: &Cli) -> Result<()> {
    let telemetry_mode = cli.topic.starts_with("telemetry");

    if telemetry_mode {
        println!(
            "Starting relay and binding worker on port {} using {}",
            cli.port, WORKER_URL
        );
        println!(
            "Warning: topic '{}' is classified as telemetry and will use HTTP push, not Durable Object websocket.",
            cli.topic
        );
    } else {
        println!(
            "Starting relay in websocket-only mode using {} (skipping bind/tunnel)",
            WORKER_URL
        );
        println!(
            "Topic '{}' is multimedia; relay will use Durable Object websocket path '/ws'.",
            cli.topic
        );
    }

    let machine_id = node.get_id().await?;
    let relay = Arc::new(WorkerRelay::new(
        node.session.clone(),
        WorkerRelay::get_default_config(),
    ));

    let worker_cfg = config::CloudflareConfig {
        api_token: "local-dev-token".to_owned(),
        machine_id,
        push_path: "/push".to_owned(),
        ws_path: "/ws".to_owned(),
        pull_path: "/pull".to_owned(),
        local_address: format!("0.0.0.0:{}", cli.port),
        ..Default::default()
    };

    if telemetry_mode {
        relay
            .bind_worker(
                WORKER_URL,
                config::WorkerConfig::Cloudflare(worker_cfg.clone()),
            )
            .await
            .context("failed to bind worker relay")?;
    } else {
        relay
            .attach_worker_ws_only(WORKER_URL, worker_cfg)
            .await
            .context("failed to attach websocket-only worker relay")?;
    }

    relay
        .register_proxy(&cli.topic, &format!("{}/", WORKER_URL))
        .context("failed to register exact topic worker proxy")?;

    tokio::spawn(async move {
        if let Err(err) = relay.listen().await {
            eprintln!("relay listener stopped: {err}");
        }
    });

    Ok(())
}

async fn publish_frames(node: Arc<Node>, topic: &str, args: PublishArgs) -> Result<()> {
    let mode_count = args.image.is_some() as u8 + args.video.is_some() as u8 + args.camera as u8;
    if mode_count > 1 {
        bail!("use only one source mode: --image OR --video OR --camera");
    }

    if let Some(image_path) = args.image.clone() {
        return publish_image_file(node, topic, image_path, &args).await;
    }

    if let Some(video_path) = args.video.clone() {
        return publish_video_file(node, topic, video_path, &args).await;
    }

    publish_live_camera(node, topic, &args).await
}

async fn receive_frames(
    node: Arc<Node>,
    topic: &str,
    out_dir: PathBuf,
    print_meta: bool,
    web_host: String,
    web_port: u16,
) -> Result<()> {
    tokio::fs::create_dir_all(&out_dir)
        .await
        .with_context(|| format!("failed to create output directory: {}", out_dir.display()))?;

    println!(
        "Listening for frames on topic '{}' and writing into {}",
        topic,
        out_dir.display()
    );
    let shared_latest = Arc::new(RwLock::new(None));
    let viewer_url = start_web_viewer(shared_latest.clone(), web_host, web_port).await?;
    println!("Web viewer available at {viewer_url}");

    let out_dir_for_cb = out_dir.clone();
    let latest_for_cb = shared_latest.clone();
    node.subscribe(topic, move |frame: CameraFrame| {
        let out_dir = out_dir_for_cb.clone();
        let latest = latest_for_cb.clone();
        async move {
            let ext = extension_from_mime(&frame.mime);
            let file_name = format!(
                "{}_{}_{}.{}",
                sanitize_for_path(&frame.source),
                frame.sequence,
                frame.captured_at_ms,
                ext
            );
            let full_path = out_dir.join(file_name);

            if let Err(err) = tokio::fs::write(&full_path, &frame.data).await {
                eprintln!("failed to write frame file {}: {err}", full_path.display());
                return;
            }

            *latest.write().await = Some(frame.clone());

            if print_meta {
                println!(
                    "Received frame source={} seq={} mime={} bytes={} saved={}",
                    frame.source,
                    frame.sequence,
                    frame.mime,
                    frame.data.len(),
                    full_path.display()
                );
            }
        }
    })
    .await
    .with_context(|| format!("failed to subscribe to topic {topic}"))?;

    tokio::signal::ctrl_c().await?;
    println!("Stopping camera receiver.");
    Ok(())
}

async fn start_web_viewer(
    latest: Arc<RwLock<Option<CameraFrame>>>,
    host: String,
    port: u16,
) -> Result<String> {
    let state = WebState { latest };
    let app = Router::new()
        .route("/", get(index_handler))
        .route("/latest", get(latest_handler))
        .with_state(state);

    let bind = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&bind)
        .await
        .with_context(|| format!("failed to bind web viewer on {bind}"))?;

    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            eprintln!("web viewer stopped: {err}");
        }
    });

    Ok(format!("http://127.0.0.1:{port}/"))
}

async fn index_handler() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Camera Viewer</title>
    <style>
      body { margin: 0; background: #101113; color: #f3f5f7; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
      .wrap { display: grid; min-height: 100vh; place-items: center; }
      img { width: min(96vw, 1200px); height: auto; border: 1px solid #30343a; border-radius: 10px; }
      p { opacity: 0.75; }
    </style>
  </head>
  <body>
    <div class="wrap">
      <div>
        <img id="feed" src="/latest" alt="camera feed" />
        <p>Live frame view (auto-refreshing every 120ms)</p>
      </div>
    </div>
    <script>
      const img = document.getElementById('feed');
      setInterval(() => {
        img.src = '/latest?t=' + Date.now();
      }, 120);
    </script>
  </body>
</html>
"#,
    )
}

async fn latest_handler(State(state): State<WebState>) -> Response {
    let guard = state.latest.read().await;
    let Some(frame) = guard.as_ref() else {
        return StatusCode::NO_CONTENT.into_response();
    };

    (
        [(header::CONTENT_TYPE, frame.mime.as_str())],
        frame.data.clone(),
    )
        .into_response()
}
