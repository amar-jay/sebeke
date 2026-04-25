use anyhow::{Context, Result};
use regex::Regex;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tracing::{info, warn};

pub struct Tunnel {
    _process: Child,
    pub public_url: String,
}

/// Downloading and installation of cloudflared.
impl Tunnel {
    pub async fn install_cloudflared() -> Result<()> {
        if which::which("cloudflared").is_err() {
            warn!("Not implemented yet! need to write the scripts first.");
        }
        Ok(())
    }

    fn ensure_cloudflared() -> Result<()> {
        if which::which("cloudflared").is_err() {
            warn!("`cloudflared` not found in PATH.");
            anyhow::bail!(
                "`cloudflared` is required. Install: https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/install-and-setup/installation"
            );
        }
        Ok(())
    }
}

impl Tunnel {
    pub async fn stop(&mut self) -> anyhow::Result<()> {
        // Try graceful shutdown first
        self._process.kill().await.ok(); // ignore error if already dead

        // Wait to fully reap the process
        let _ = self._process.wait().await;

        Ok(())
    }

    /// https / quic, default: https
    fn set_protocol(protocol: &str) -> &'static str {
        match protocol {
            "http2" => "http2",
            "quic" => "quic",
            _ => {
                warn!("Unsupported protocol '{}', defaulting to 'http2'", protocol);
                "http2"
            }
        }
    }

    pub async fn start(local_port: u16, protocol: &str) -> Result<Self> {
        Self::ensure_cloudflared()?;

        info!("Starting cloudflared tunnel on port {}...", local_port);

        let mut child = Command::new("cloudflared")
            .args([
                "tunnel",
                "--protocol",
                Self::set_protocol(protocol),
                "--url",
                &format!("http://127.0.0.1:{}", local_port),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("Failed to spawn `cloudflared`. Is it supported on this platform?")?;

        let stderr = child.stderr.take().context("Failed to capture stderr")?;

        let mut reader = BufReader::new(stderr).lines();
        let re = Regex::new(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com")?;

        let mut public_url = String::new();

        // Extract URL
        while let Some(line) = reader.next_line().await? {
            if let Some(caps) = re.captures(&line) {
                public_url = caps[0].to_string();
                info!("Cloudflare Tunnel established: {}", public_url);
                break;
            }
        }

        if public_url.is_empty() {
            anyhow::bail!("Failed to extract TryCloudflare URL.");
        }

        // Drain logs in background
        tokio::spawn(async move { while let Ok(Some(_)) = reader.next_line().await {} });

        Ok(Self {
            _process: child,
            public_url,
        })
    }
}
