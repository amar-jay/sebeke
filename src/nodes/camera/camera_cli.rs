use std::{path::PathBuf};

use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
#[command(name = "camera_node")]
#[command(about = "Publish and receive camera frames over Zenoh")]
pub struct Cli {
	#[arg(long, default_value = "camera/frames")]
	pub topic: String,

	#[arg(long, default_value_t = 8787)]
	pub port: u16,

	#[command(subcommand)]
	pub command: CameraCommand,
}

#[derive(Debug, Subcommand)]
pub enum CameraCommand {
	Publish(PublishArgs),
	Receive {
		#[arg(long, default_value = "./camera_frames")]
		out_dir: PathBuf,

		#[arg(long, default_value_t = false)]
		print_meta: bool,

		#[arg(long, default_value = "0.0.0.0")]
		web_host: String,

		#[arg(long, default_value_t = 8080)]
		web_port: u16,
	},
}

#[derive(Debug, Args)]
pub struct PublishArgs {
	#[arg(long)]
	pub	image: Option<PathBuf>,

	#[arg(long)]
	pub video: Option<PathBuf>,

	#[arg(long, default_value_t = false)]
	pub camera: bool,

	#[arg(long, default_value = "/dev/video0")]
	pub camera_device: String,

	#[arg(long, default_value_t = 10)]
	pub fps: u32,

	#[arg(long, default_value_t = 640)]
	pub width: u32,

	#[arg(long, default_value_t = 480)]
	pub height: u32,

	#[arg(long)]
	pub duration_secs: Option<u64>,

	#[arg(long, default_value = "camera-local")]
	pub source: String,

	#[arg(long, default_value = "image/jpeg")]
	pub mime: String,

	#[arg(long, default_value_t = 1)]
	pub repeat: u64,

	#[arg(long, default_value_t = 200)]
	pub interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CameraFrame {
	pub source: String,
	pub mime: String,
	pub sequence: u64,
	pub captured_at_ms: u128,
	#[serde(with = "serde_bytes")]
	pub data: Vec<u8>,
}


