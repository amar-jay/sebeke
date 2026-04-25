use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};

use super::camera_cli::PublishArgs;
use super::image;
use super::utils::now_ms;
use crate::node::Node ;

use tokio::process::Command as TokioCommand;
use std::process::Stdio;
use tokio::{
	io::AsyncReadExt,
};

async fn next_jpeg_frame<R: AsyncReadExt + Unpin>(
	reader: &mut R,
	buffer: &mut Vec<u8>,
) -> Result<Option<Vec<u8>>> {
	loop {
		if let Some(frame) = extract_jpeg_frame(buffer) {
			return Ok(Some(frame));
		}

		let mut chunk = [0_u8; 8192];
		let n = reader.read(&mut chunk).await?;
		if n == 0 {
			return Ok(extract_jpeg_frame(buffer));
		}

		buffer.extend_from_slice(&chunk[..n]);
		if buffer.len() > 16 * 1024 * 1024 {
			let keep_from = buffer.len().saturating_sub(512 * 1024);
			buffer.drain(0..keep_from);
		}
	}
}

fn extract_jpeg_frame(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
	let start = buffer
		.windows(2)
		.position(|w| w[0] == 0xFF && w[1] == 0xD8)?;

	if start > 0 {
		buffer.drain(0..start);
	}

	let end = buffer
		.windows(2)
		.position(|w| w[0] == 0xFF && w[1] == 0xD9)?;

	let frame_end = end + 2;
	let frame = buffer[..frame_end].to_vec();
	buffer.drain(0..frame_end);
	Some(frame)
}

pub async fn publish_live_camera(node: Arc<Node>, topic: &str, args: &PublishArgs) -> Result<()> {
	let mut command = TokioCommand::new("ffmpeg");
	command
		.args(["-hide_banner", "-loglevel", "error", "-f", "v4l2", "-framerate"])
		.arg(args.fps.to_string())
		.args(["-video_size"])
		.arg(format!("{}x{}", args.width, args.height))
		.args(["-i"])
		.arg(args.camera_device.as_str())
		.args(["-f", "image2pipe", "-vcodec", "mjpeg", "-"])
		.stdout(Stdio::piped())
		.stderr(Stdio::inherit());

	stream_mjpeg_to_topic(node, topic, command, args).await
}

async fn stream_mjpeg_to_topic(
	node: Arc<Node>,
	topic: &str,
	mut command: TokioCommand,
	args: &PublishArgs,
) -> Result<()> {
	let mut child = command
		.spawn()
		.with_context(|| "failed to start ffmpeg. install ffmpeg and verify camera/video source")?;
	let mut stdout = child
		.stdout
		.take()
		.context("ffmpeg stdout capture failed")?;

	let started = now_ms();
	let mut sequence: u64 = 0;
	let mut buffer = Vec::with_capacity(128 * 1024);

	loop {
		if let Some(limit_secs) = args.duration_secs {
			if now_ms().saturating_sub(started) >= (limit_secs as u128 * 1000) {
				break;
			}
		}

		tokio::select! {
			_ = tokio::signal::ctrl_c() => {
				break;
			}
			result = next_jpeg_frame(&mut stdout, &mut buffer) => {
				let Some(frame_bytes) = result? else {
					break;
				};
				image::publish_one_frame(node.clone(), topic, sequence, frame_bytes, &args.source, &args.mime).await?;
				sequence = sequence.saturating_add(1);
			}
		}
	}

	let _ = child.kill().await;
	let _ = child.wait().await;
	println!("Stopped streaming after publishing {sequence} frames");
	Ok(())
}

pub async fn publish_video_file(
	node: Arc<Node>,
	topic: &str,
	video_path: PathBuf,
	args: &PublishArgs,
) -> Result<()> {
	let mut command = TokioCommand::new("ffmpeg");
	command
		.args(["-hide_banner", "-loglevel", "error", "-re", "-i"])
		.arg(video_path.as_os_str())
		.args(["-f", "image2pipe", "-vcodec", "mjpeg", "-"])
		.stdout(Stdio::piped())
		.stderr(Stdio::inherit());

	stream_mjpeg_to_topic(node, topic, command, args).await
}