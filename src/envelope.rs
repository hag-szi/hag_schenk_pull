//! Event-Envelope + Payload. Spiegel der Pydantic-Contracts aus
//! `HAG Connect Platform/contracts/schemas/events/schenk-lager-avise-received.yaml`
//! (v1.0.1, v2-Subject-Taxonomy).

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope<T> {
    pub event_id: String,
    pub event_name: String,
    pub event_version: String,
    pub occurred_at: DateTime<Utc>,
    pub producer: String,
    pub customer_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,
    pub data: T,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachments: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_payload: Option<serde_json::Value>,
}

impl<T> Envelope<T> {
    /// Baut einen Envelope mit frischer UUID und aktueller Zeit.
    pub fn wrap(
        event_name: impl Into<String>,
        event_version: impl Into<String>,
        producer: impl Into<String>,
        customer_key: impl Into<String>,
        data: T,
    ) -> Self {
        Self {
            event_id: uuid::Uuid::new_v4().to_string(),
            event_name: event_name.into(),
            event_version: event_version.into(),
            occurred_at: Utc::now(),
            producer: producer.into(),
            customer_key: customer_key.into(),
            correlation_id: None,
            causation_id: None,
            data,
            attachments: None,
            raw_payload: None,
        }
    }
}

/// Payload für `hag.events.schenk.lager.avise.received`. Feld-für-Feld
/// analog zur Pydantic-Definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AviseData {
    pub bestellnr: String,
    pub position_nr: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ta_nummer: Option<String>,
    pub artikelnr: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artikel_bezeichnung: Option<String>,
    pub menge_bestellt: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pal_menge: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lngbuendelung: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mengeneinheit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lieferantennr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lieferantenfilialnr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lagerkennzeichen: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bestelldatum: Option<NaiveDate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub liefertermin: Option<NaiveDate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jahrgang: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gebinde_typ_prefill: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_minimal() {
        let data = AviseData {
            bestellnr: "KA24/001723".into(),
            position_nr: 1,
            ta_nummer: None,
            artikelnr: "07418".into(),
            artikel_bezeichnung: None,
            menge_bestellt: 800.0,
            pal_menge: Some(100.0),
            lngbuendelung: Some(6),
            mengeneinheit: None,
            lieferantennr: None,
            lieferantenfilialnr: None,
            lagerkennzeichen: None,
            bestelldatum: None,
            liefertermin: None,
            jahrgang: Some("2022".into()),
            gebinde_typ_prefill: Some("FLA".into()),
        };
        let env = Envelope::wrap(
            "hag.events.schenk.lager.avise.received",
            "1.0.1",
            "hag-schenk-pull",
            "SCHENK",
            data,
        );
        let json = serde_json::to_string(&env).unwrap();
        assert!(json.contains("\"event_name\":\"hag.events.schenk.lager.avise.received\""));
        assert!(!json.contains("attachments"));
        assert!(!json.contains("causation_id"));
        let back: Envelope<AviseData> = serde_json::from_str(&json).unwrap();
        assert_eq!(back.data.bestellnr, "KA24/001723");
        assert_eq!(back.data.pal_menge, Some(100.0));
    }
}
