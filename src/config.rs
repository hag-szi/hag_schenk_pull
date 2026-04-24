use figment::providers::{Env, Format, Toml};
use figment::Figment;
use serde::Deserialize;
use std::path::PathBuf;

use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub logging: LoggingConfig,
    pub postgres: PostgresConfig,
    pub amqp: AmqpConfig,
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
pub struct AmqpConfig {
    /// AMQP-URI inkl. Credentials + Vhost. Credentials besser per
    /// Env-Variable (`HAG_SCHENK_PULL__AMQP__URL`).
    pub url: String,
    pub outbound_exchange: String,
    pub outbound_routing_key: String,
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
