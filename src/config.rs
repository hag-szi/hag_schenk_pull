use figment::providers::{Env, Format, Toml};
use figment::Figment;
use serde::Deserialize;
use std::path::PathBuf;

use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub logging: LoggingConfig,
    pub postgres: PostgresConfig,
    pub nats: NatsConfig,
    pub pull: PullConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    pub dir: PathBuf,
    pub level: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    pub url: String,
    pub lngroot_id: i32,
    pub lngprod_id: i32,
    /// Wenn `true`, werden SELECTs ausgeführt, aber **niemals**
    /// `UPDATE strlocked='N'`. Für Dev-Umgebungen — der Publisher
    /// publisht dann dieselben Avise endlos erneut.
    pub read_only: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NatsConfig {
    /// NATS-Server-URL, z.B. `nats://192.168.4.128:4222`. Credentials
    /// kommen besser per `HAG_SCHENK_PULL__NATS__USERNAME` /
    /// `HAG_SCHENK_PULL__NATS__PASSWORD` aus dem Env.
    pub url: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    /// Env-Token (`dev`/`test`/`prod`), der als zweites
    /// Subject-Segment eingefügt wird — siehe `nats::env_scope_subject`.
    /// Match zu `subject_suffix_for(env)` im Plattform-`declare_topology.py`.
    pub env: String,
    /// Wird als `producer` im Event-Envelope eingetragen.
    pub producer_name: String,
    /// Wird als `customer_key` im Event-Envelope eingetragen.
    pub customer_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PullConfig {
    /// Taktung der Inner-Loop. Zwischen zwei Pulls wird so viele
    /// Sekunden geschlafen.
    pub interval_seconds: u64,
}

impl Config {
    pub fn load(path: &str) -> AppResult<Self> {
        Figment::new()
            .merge(Toml::file(path))
            .merge(Env::prefixed("HAG_SCHENK_PULL__").split("__"))
            .extract()
            .map_err(|e| AppError::Config(e.to_string()))
    }
}
