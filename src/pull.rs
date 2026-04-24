//! Die eigentliche Pull-Logik: fetch → enrich → publish → mark.
//!
//! Wichtig für die Reihenfolge: wir markieren die Avise erst als
//! verarbeitet, **nachdem** alle Publishes für den Batch durch sind.
//! Fällt der Prozess zwischendurch aus, gehen beim nächsten Lauf
//! dieselben Zeilen nochmal raus — der Consumer ist idempotent.

use sqlx::PgPool;

use crate::config::PostgresConfig;
use crate::db::{self, AviseRow};
use crate::envelope::AviseData;
use crate::error::AppResult;
use crate::nats::NatsPublisher;

pub async fn run_once(
    pg: &PgPool,
    nats: &NatsPublisher,
    pg_cfg: &PostgresConfig,
) -> AppResult<usize> {
    let rows = db::fetch_new_avise(pg, pg_cfg).await?;
    if rows.is_empty() {
        tracing::debug!("keine neuen avise");
        return Ok(0);
    }
    tracing::info!(count = rows.len(), "avise-batch gezogen");

    let mut published: Vec<AviseRow> = Vec::with_capacity(rows.len());
    for row in rows {
        // Enrichment per `m.artikelbasis`-Lookup — erst hier, damit
        // wir im Fehlerfall nur die bereits publizierten Zeilen
        // markieren.
        let info = db::lookup_artikel_info(pg, row.strartikelnr.as_deref()).await?;
        let gebinde_typ_prefill = db::prefill_gebinde_typ(row.strmengeneinheit.as_deref());
        let data = to_avise_data(&row, &info, gebinde_typ_prefill);

        match nats.publish_avise(data).await {
            Ok(event_id) => {
                tracing::info!(
                    event_id = %event_id,
                    bestellnr = %row.strbestellnr,
                    position_nr = row.lngpositionsnr,
                    "avise publiziert"
                );
                published.push(row);
            }
            Err(e) => {
                tracing::warn!(
                    bestellnr = %row.strbestellnr,
                    position_nr = row.lngpositionsnr,
                    error = %e,
                    "avise publish fehlgeschlagen — wird beim nächsten Lauf erneut versucht"
                );
            }
        }
    }

    // Mark erst jetzt — alle nicht-publishten Zeilen bleiben
    // unverändert und werden beim nächsten Lauf nochmal probiert.
    let count = published.len();
    db::mark_pulled(pg, pg_cfg, &published).await?;
    Ok(count)
}

fn to_avise_data(
    row: &AviseRow,
    info: &db::ArtikelInfo,
    gebinde_typ_prefill: Option<String>,
) -> AviseData {
    AviseData {
        bestellnr: row.strbestellnr.clone(),
        position_nr: row.lngpositionsnr,
        ta_nummer: row.strtransportauftragsnr.clone(),
        artikelnr: row.strartikelnr.clone().unwrap_or_default(),
        artikel_bezeichnung: info.bezeichnung.clone(),
        menge_bestellt: row.nummenge_bestellt.unwrap_or(0.0),
        pal_menge: info.pal_menge,
        lngbuendelung: info.lngbuendelung,
        mengeneinheit: row.strmengeneinheit.clone(),
        lieferantennr: row.strlieferantennr.clone(),
        lieferantenfilialnr: row.strlieferantenfilialnr.clone(),
        lagerkennzeichen: row.strlagerkennzeichen.clone(),
        bestelldatum: row.dtmbestelldatum,
        liefertermin: row.dtmliefertermin,
        jahrgang: row.strjahrgang.clone(),
        gebinde_typ_prefill,
    }
}
