//! Postgres-Zugriff auf die Legacy `io_oxaion.i_weavise` + `m.artikelbasis`.
//!
//! Der Code ist 1:1 aus dem alten `schenk_we_export/src/db/avise.rs`
//! übernommen — dort war er in den Web-Service eingebaut, jetzt wohnt
//! er in diesem eigenständigen Pull-Publisher.

use std::sync::atomic::{AtomicBool, Ordering};

use sqlx::{PgPool, Row};

use crate::config::PostgresConfig;
use crate::error::AppResult;

/// Wird auf `true` gesetzt, wenn `m.artikelbasis` nicht existiert
/// (typisch in Dev-Umgebungen, in denen die Tabelle nicht kopiert
/// wurde). Danach wird der Lookup stumm übersprungen, statt die Logs
/// zu fluten.
static ARTIKELBASIS_UNAVAILABLE: AtomicBool = AtomicBool::new(false);

/// Pool-Factory (Loopback-Postgres via autossh-Tunnel).
pub async fn connect(cfg: &PostgresConfig) -> AppResult<PgPool> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&cfg.url)
        .await?;
    Ok(pool)
}

/// Gebinde-Typ-Vorbelegung aus `strmengeneinheit` (alte Lobster-Regel):
/// `FL` → `FLA`, `KT` → `KAR`, sonst erste 3 Zeichen unverändert.
pub fn prefill_gebinde_typ(strmengeneinheit: Option<&str>) -> Option<String> {
    strmengeneinheit
        .map(|s| match s.trim() {
            "FL" => "FLA".to_string(),
            "KT" => "KAR".to_string(),
            other => other.chars().take(3).collect(),
        })
        .filter(|s| !s.is_empty())
}

/// Artikel-Stammdaten, die wir pro Avis-Zeile anreichern.
#[derive(Debug, Clone, Default)]
pub struct ArtikelInfo {
    pub bezeichnung: Option<String>,
    /// `nummengeimlademittel × lngbuendelung` — Kapazität einer Vollpalette.
    pub pal_menge: Option<f64>,
    pub lngbuendelung: Option<i32>,
}

/// Liest `strartikelbezeichnung1`, `nummengeimlademittel`,
/// `lngbuendelung` aus `m.artikelbasis`. Fehlt die Tabelle, wird
/// einmalig gewarnt und danach ein leerer ArtikelInfo zurückgegeben.
pub async fn lookup_artikel_info(
    pg: &PgPool,
    strartikelnr: Option<&str>,
) -> AppResult<ArtikelInfo> {
    let Some(art) = strartikelnr else {
        return Ok(ArtikelInfo::default());
    };
    if ARTIKELBASIS_UNAVAILABLE.load(Ordering::Relaxed) {
        return Ok(ArtikelInfo::default());
    }
    type ArtikelRow = (Option<String>, Option<f64>, Option<i32>);
    let res: Result<Option<ArtikelRow>, _> = sqlx::query_as(
        "SELECT RTRIM(strartikelbezeichnung1), \
                nummengeimlademittel::double precision, \
                lngbuendelung \
         FROM m.artikelbasis WHERE RTRIM(strartikelnr) = $1",
    )
    .bind(art)
    .fetch_optional(pg)
    .await;
    match res {
        Ok(None) => Ok(ArtikelInfo::default()),
        Ok(Some((bez, menge, buendelung))) => {
            let pal_menge = match (menge, buendelung) {
                (Some(m), Some(b)) if m > 0.0 && b > 0 => Some(m * b as f64),
                _ => None,
            };
            Ok(ArtikelInfo {
                bezeichnung: bez.filter(|s| !s.is_empty()),
                pal_menge,
                lngbuendelung: buendelung.filter(|b| *b > 0),
            })
        }
        Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("42P01") => {
            tracing::warn!(
                "m.artikelbasis existiert nicht — Artikel-Bezeichnung/Paletten-Split deaktiviert"
            );
            ARTIKELBASIS_UNAVAILABLE.store(true, Ordering::Relaxed);
            Ok(ArtikelInfo::default())
        }
        Err(e) => Err(e.into()),
    }
}

/// Eine Zeile aus `i_weavise`, wie wir sie nach Postgres-RTRIM-Handling
/// weiterverarbeiten.
#[derive(Debug, Clone)]
pub struct AviseRow {
    pub strbestellnr: String,
    pub lngpositionsnr: i64,
    pub strartikelnr: Option<String>,
    pub nummenge_bestellt: Option<f64>,
    pub strmengeneinheit: Option<String>,
    pub strjahrgang: Option<String>,
    pub strlieferantennr: Option<String>,
    pub strlieferantenfilialnr: Option<String>,
    pub strtransportauftragsnr: Option<String>,
    pub strlagerkennzeichen: Option<String>,
    pub dtmbestelldatum: Option<chrono::NaiveDate>,
    pub dtmliefertermin: Option<chrono::NaiveDate>,
}

/// Liest alle aktuell offenen Avise (`strvalid='Y' AND strlocked='T'`).
///
/// Schreibt **nicht** auf Postgres — das macht `mark_pulled` erst nach
/// erfolgreichem Publish, damit im Crash-Fall die Zeilen beim nächsten
/// Lauf noch einmal publiziert werden. Der Consumer auf der anderen
/// Seite ist idempotent (`ON CONFLICT DO NOTHING`).
pub async fn fetch_new_avise(pg: &PgPool, cfg: &PostgresConfig) -> AppResult<Vec<AviseRow>> {
    // nummenge_bestellt ist in Prod numeric(13,3) — Cast auf double,
    // damit sqlx ohne bigdecimal-Feature auskommt. VARCHAR-Spalten
    // sind auf feste Länge mit Leerzeichen gepaddet — RTRIM beim Lesen.
    let rows = sqlx::query(
        r#"
        SELECT strbestellnr,
               lngpositionsnr,
               RTRIM(strartikelnr) AS strartikelnr,
               nummenge_bestellt::double precision AS nummenge_bestellt,
               RTRIM(strmengeneinheit) AS strmengeneinheit,
               RTRIM(strjahrgang) AS strjahrgang,
               RTRIM(strlieferantennr) AS strlieferantennr,
               RTRIM(strlieferantenfilialnr) AS strlieferantenfilialnr,
               RTRIM(strtransportauftragsnr) AS strtransportauftragsnr,
               RTRIM(strlagerkennzeichen) AS strlagerkennzeichen,
               dtmbestelldatum,
               dtmliefertermin
        FROM io_oxaion.i_weavise
        WHERE strvalid = 'Y'
          AND strlocked = 'T'
          AND lngroot_id = $1
          AND lngprod_id = $2
        "#,
    )
    .bind(cfg.lngroot_id)
    .bind(cfg.lngprod_id)
    .fetch_all(pg)
    .await?;
    Ok(rows.into_iter().map(map_row).collect())
}

/// Markiert die übergebenen Avise als verarbeitet
/// (`strlocked='N'`). Bei `cfg.read_only == true` passiert nichts.
pub async fn mark_pulled(pg: &PgPool, cfg: &PostgresConfig, avise: &[AviseRow]) -> AppResult<()> {
    if avise.is_empty() {
        return Ok(());
    }
    if cfg.read_only {
        tracing::warn!(
            count = avise.len(),
            "postgres read_only=true — UPDATE strlocked='N' übersprungen"
        );
        return Ok(());
    }

    sqlx::query(
        "UPDATE io_oxaion.i_weavise SET strlocked = 'N' \
         WHERE lngroot_id = $1 AND lngprod_id = $2 \
           AND strbestellnr = ANY($3) AND lngpositionsnr = ANY($4)",
    )
    .bind(cfg.lngroot_id)
    .bind(cfg.lngprod_id)
    .bind(
        avise
            .iter()
            .map(|a| a.strbestellnr.clone())
            .collect::<Vec<_>>(),
    )
    .bind(
        avise
            .iter()
            .map(|a| a.lngpositionsnr as i32)
            .collect::<Vec<i32>>(),
    )
    .execute(pg)
    .await?;
    Ok(())
}

fn map_row(r: sqlx::postgres::PgRow) -> AviseRow {
    AviseRow {
        strbestellnr: r.get("strbestellnr"),
        lngpositionsnr: r.get::<i32, _>("lngpositionsnr") as i64,
        strartikelnr: r.try_get("strartikelnr").ok().flatten(),
        nummenge_bestellt: r.try_get("nummenge_bestellt").ok().flatten(),
        strmengeneinheit: r.try_get("strmengeneinheit").ok().flatten(),
        strjahrgang: r.try_get("strjahrgang").ok().flatten(),
        strlieferantennr: r.try_get("strlieferantennr").ok().flatten(),
        strlieferantenfilialnr: r.try_get("strlieferantenfilialnr").ok().flatten(),
        strtransportauftragsnr: r.try_get("strtransportauftragsnr").ok().flatten(),
        strlagerkennzeichen: r.try_get("strlagerkennzeichen").ok().flatten(),
        dtmbestelldatum: r.try_get("dtmbestelldatum").ok().flatten(),
        dtmliefertermin: r.try_get("dtmliefertermin").ok().flatten(),
    }
}

#[cfg(test)]
mod tests {
    use super::prefill_gebinde_typ;

    #[test]
    fn abbildung_fl_kt_sonst() {
        assert_eq!(prefill_gebinde_typ(Some("FL")).as_deref(), Some("FLA"));
        assert_eq!(prefill_gebinde_typ(Some("KT")).as_deref(), Some("KAR"));
        assert_eq!(prefill_gebinde_typ(Some("ST")).as_deref(), Some("ST"));
        assert_eq!(prefill_gebinde_typ(Some("")), None);
        assert_eq!(prefill_gebinde_typ(None), None);
    }
}
