//! AMQP-Connection + Publisher. Dünn — wir publishen nur, kein
//! Consumer nötig. Retry-Strategie: 10 Versuche × 3 s beim Start;
//! danach übernimmt lapin die Reconnect-Heartbeats.

use std::time::Duration;

use lapin::options::BasicPublishOptions;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};

use crate::config::AmqpConfig;
use crate::envelope::{AviseData, Envelope};
use crate::error::{AppError, AppResult};

const EVENT_NAME: &str = "warehouse.schenk.avise-received";
const EVENT_VERSION: &str = "0.1.0";

pub struct AmqpPublisher {
    channel: Channel,
    cfg: AmqpConfig,
}

impl AmqpPublisher {
    pub async fn connect_with_retry(cfg: AmqpConfig) -> AppResult<Self> {
        let props = ConnectionProperties::default()
            .with_executor(tokio_executor_trait::Tokio::current())
            .with_reactor(tokio_reactor_trait::Tokio);

        let mut last: Option<lapin::Error> = None;
        for attempt in 1..=10 {
            match Connection::connect(&cfg.url, props.clone()).await {
                Ok(conn) => {
                    let channel = conn.create_channel().await.map_err(wrap)?;
                    tracing::info!(attempt, "amqp verbunden");
                    return Ok(Self { channel, cfg });
                }
                Err(e) => {
                    tracing::warn!(attempt, error = %e, "amqp connect fehlgeschlagen, retry in 3s");
                    last = Some(e);
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        }
        Err(AppError::Other(anyhow::anyhow!(
            "amqp-connect nach 10 Versuchen fehlgeschlagen: {:?}",
            last
        )))
    }

    /// Publisht eine einzelne Avis-Zeile als `warehouse.schenk.avise-received`-
    /// Event. Persistent, content-type `application/json`, message-id =
    /// event_id.
    pub async fn publish_avise(&self, data: AviseData) -> AppResult<String> {
        let envelope = Envelope::wrap(
            EVENT_NAME,
            EVENT_VERSION,
            self.cfg.producer_name.clone(),
            self.cfg.customer_key.clone(),
            data,
        );
        let body = serde_json::to_vec(&envelope)
            .map_err(|e| AppError::Other(anyhow::anyhow!("serialize: {e}")))?;
        let props = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(2)
            .with_message_id(envelope.event_id.clone().into());
        self.channel
            .basic_publish(
                &self.cfg.outbound_exchange,
                &self.cfg.outbound_routing_key,
                BasicPublishOptions::default(),
                &body,
                props,
            )
            .await
            .map_err(wrap)?;
        Ok(envelope.event_id)
    }
}

fn wrap(e: lapin::Error) -> AppError {
    AppError::Other(anyhow::anyhow!("amqp: {e}"))
}
