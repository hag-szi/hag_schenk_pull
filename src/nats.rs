//! NATS JetStream Publisher. Wir publishen nur, kein Consumer.
//!
//! Subject ist fix `hag.events.schenk.lager.avise.received` (v2-
//! Taxonomy aus dem HAG-Connect-Contracts-Repo). Der env-Token wird
//! als zweites Subject-Segment eingefügt — `hag.<env>.events.…` —
//! das spiegelt die heutige Stream-Capture-Regel im
//! `hag-events`-Stream (`hag{ENV_SUFFIX}.events.>`).
//!
//! JetStream-Publish setzt den `Nats-Msg-Id`-Header auf die
//! event_id; der Stream entdeduppt innerhalb des
//! Stream-Dedup-Windows (300s) automatisch — Doppel-Publishes nach
//! Crash/Restart führen zu **einer** Nachricht im Stream.

use std::time::Duration;

use async_nats::jetstream::{self, context::Publish};
use async_nats::HeaderMap;
use bytes::Bytes;

use crate::config::NatsConfig;
use crate::envelope::{AviseData, Envelope};
use crate::error::{AppError, AppResult};

const EVENT_NAME: &str = "hag.events.schenk.lager.avise.received";
const EVENT_VERSION: &str = "1.0.1";

pub struct NatsPublisher {
    js: jetstream::Context,
    cfg: NatsConfig,
    /// Bereits env-skopierter Subject-String (siehe Modul-Doc).
    subject: String,
}

impl NatsPublisher {
    pub async fn connect_with_retry(cfg: NatsConfig) -> AppResult<Self> {
        let mut last: Option<async_nats::Error> = None;
        for attempt in 1..=10 {
            match Self::connect_once(&cfg).await {
                Ok(client) => {
                    let js = jetstream::new(client);
                    let subject = env_scope_subject(EVENT_NAME, &cfg.env);
                    tracing::info!(
                        attempt,
                        url = %cfg.url,
                        subject = %subject,
                        "nats verbunden"
                    );
                    return Ok(Self { js, cfg, subject });
                }
                Err(e) => {
                    tracing::warn!(attempt, error = %e, "nats connect fehlgeschlagen, retry in 3s");
                    last = Some(e);
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        }
        Err(AppError::Other(anyhow::anyhow!(
            "nats-connect nach 10 Versuchen fehlgeschlagen: {:?}",
            last
        )))
    }

    async fn connect_once(cfg: &NatsConfig) -> Result<async_nats::Client, async_nats::Error> {
        let mut opts = async_nats::ConnectOptions::new()
            .name("hag-schenk-pull")
            .connection_timeout(Duration::from_secs(5));
        if let (Some(user), Some(pass)) = (cfg.username.as_deref(), cfg.password.as_deref()) {
            opts = opts.user_and_password(user.into(), pass.into());
        }
        opts.connect(&cfg.url).await.map_err(|e| e.into())
    }

    /// Publisht eine einzelne Avis-Zeile als
    /// `hag.events.schenk.lager.avise.received`. Wartet auf den
    /// JetStream-Publish-Ack — bei Fehler gibt der Caller die Zeile
    /// nicht in `mark_pulled` weiter und versucht beim nächsten Pull
    /// erneut.
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

        let mut headers = HeaderMap::new();
        // Server-seitiger Dedup-Key — siehe Modul-Doc.
        headers.insert("Nats-Msg-Id", envelope.event_id.as_str());

        let ack = self
            .js
            .send_publish(
                self.subject.clone(),
                Publish::build().payload(Bytes::from(body)).headers(headers),
            )
            .await
            .map_err(|e| AppError::Other(anyhow::anyhow!("nats publish: {e}")))?
            .await
            .map_err(|e| AppError::Other(anyhow::anyhow!("nats publish-ack: {e}")))?;

        tracing::debug!(
            event_id = %envelope.event_id,
            stream = %ack.stream,
            seq = ack.sequence,
            duplicate = ack.duplicate,
            "avise publish-ack"
        );
        Ok(envelope.event_id)
    }
}

/// Fügt den Env-Token als zweites Subject-Segment ein:
/// `hag.events.schenk.lager.avise.received` + env=`dev`
/// → `hag.dev.events.schenk.lager.avise.received`.
/// Spiegel der `subject_suffix_for(env)`-Logik im Plattform-Repo.
fn env_scope_subject(base: &str, env: &str) -> String {
    let (head, rest) = base.split_once('.').unwrap_or((base, ""));
    if rest.is_empty() {
        return format!("{head}.{env}");
    }
    format!("{head}.{env}.{rest}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_scope_dev() {
        assert_eq!(
            env_scope_subject("hag.events.schenk.lager.avise.received", "dev"),
            "hag.dev.events.schenk.lager.avise.received"
        );
    }

    #[test]
    fn env_scope_prod() {
        assert_eq!(
            env_scope_subject("hag.events.schenk.lager.avise.received", "prod"),
            "hag.prod.events.schenk.lager.avise.received"
        );
    }
}
