use std::path::PathBuf;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::error::AppResult;

pub struct LoggingConfig {
    pub dir: PathBuf,
    pub level: String,
}

pub fn init(cfg: &LoggingConfig) -> AppResult<WorkerGuard> {
    std::fs::create_dir_all(&cfg.dir)?;
    let file_appender = tracing_appender::rolling::daily(&cfg.dir, "hag-schenk-pull.log");
    let (nb_writer, guard) = tracing_appender::non_blocking(file_appender);

    let filter = EnvFilter::try_new(&cfg.level).unwrap_or_else(|_| EnvFilter::new("info"));
    let stdout_layer = fmt::layer().with_ansi(atty_stdout()).with_target(false);
    let file_layer = fmt::layer()
        .json()
        .with_writer(nb_writer)
        .with_target(true)
        .with_current_span(false)
        .with_span_list(false);

    tracing_subscriber::registry()
        .with(filter)
        .with(stdout_layer)
        .with(file_layer)
        .init();
    Ok(guard)
}

fn atty_stdout() -> bool {
    use std::io::IsTerminal;
    std::io::stdout().is_terminal()
}
