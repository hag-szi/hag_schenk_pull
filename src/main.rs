use std::time::Duration;

use clap::Parser;

mod amqp;
mod config;
mod db;
mod envelope;
mod error;
mod logging;
mod pull;

use config::Config;
use error::AppResult;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Pollt Schenk-Avise aus Postgres und publisht sie auf den HAG-Bus"
)]
struct Cli {
    #[arg(
        long,
        env = "HAG_SCHENK_PULL_CONFIG",
        default_value = "config/config.toml"
    )]
    config: String,

    /// Einmaliger Pull-Durchlauf, dann Ende. Ideal für Debug/Tests
    /// oder externen Cron.
    #[arg(long)]
    once: bool,
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let cli = Cli::parse();
    let cfg = Config::load(&cli.config)?;
    let _log_guard = logging::init(&logging::LoggingConfig {
        dir: cfg.logging.dir.clone(),
        level: cfg.logging.level.clone(),
    })?;

    if cfg.postgres.read_only {
        tracing::warn!(
            "postgres.read_only=true — `strlocked`-UPDATE wird übersprungen. \
             Nur Dev-Umgebung; Publisher wird dieselben Avise endlos erneut senden."
        );
    }

    let pg = db::connect(&cfg.postgres).await?;
    let amqp = amqp::AmqpPublisher::connect_with_retry(cfg.amqp.clone()).await?;

    if cli.once {
        let n = pull::run_once(&pg, &amqp, &cfg.postgres).await?;
        tracing::info!(published = n, "einmaliger Lauf fertig");
        return Ok(());
    }

    let interval = Duration::from_secs(cfg.pull.interval_seconds);
    tracing::info!(
        interval_secs = cfg.pull.interval_seconds,
        exchange = %cfg.amqp.outbound_exchange,
        routing_key = %cfg.amqp.outbound_routing_key,
        "hag-schenk-pull läuft"
    );

    // Inner-Loop: pull → sleep → pull → …
    // Ctrl-C (SIGINT) beendet sauber.
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutdown requested, bye");
                return Ok(());
            }
            _ = async {
                match pull::run_once(&pg, &amqp, &cfg.postgres).await {
                    Ok(n) if n > 0 => tracing::info!(published = n, "batch fertig"),
                    Ok(_) => {},
                    Err(e) => tracing::warn!(error = %e, "pull-iteration fehlgeschlagen, weiter beim nächsten intervall"),
                }
                tokio::time::sleep(interval).await;
            } => {}
        }
    }
}
