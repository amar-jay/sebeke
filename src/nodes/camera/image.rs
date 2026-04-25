use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;

use anyhow::anyhow;

use crate::{node::Node, utils::now_ms};

use super::camera_cli::{CameraFrame, PublishArgs};

pub async fn publish_image_file(
    node: Arc<Node>,
    topic: &str,
    file: PathBuf,
    args: &PublishArgs,
) -> Result<()> {
    let payload = tokio::fs::read(&file).await?;

    for sequence in 0..args.repeat {
        publish_one_frame(
            node.clone(),
            topic,
            sequence,
            payload.clone(),
            &args.source,
            &args.mime,
        )
        .await?;
        if sequence + 1 < args.repeat {
            tokio::time::sleep(Duration::from_millis(args.interval_ms)).await;
        }
    }

    Ok(())
}

pub async fn publish_one_frame(
    node: Arc<Node>,
    topic: &str,
    sequence: u64,
    data: Vec<u8>,
    source: &str,
    mime: &str,
) -> Result<()> {
    let frame = CameraFrame {
        source: source.to_owned(),
        mime: mime.to_owned(),
        sequence,
        captured_at_ms: now_ms(),
        data,
    };

    node.publish(topic, &frame)
        .await
        .map_err(|_| anyhow!("failed to publish frame #{sequence} to topic {topic}"))?;

    println!(
        "Published frame #{sequence} ({} bytes) on topic '{}'",
        frame.data.len(),
        topic
    );

    Ok(())
}
