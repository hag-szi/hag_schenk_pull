# HAG Schenk Pull

Long-running Rust-Service, der die Legacy-Postgres (`io_oxaion.i_weavise`)
in einer konfigurierbaren Taktung auf neue Schenk-Avise pollt, die
Zeilen mit Stammdaten aus `m.artikelbasis` anreichert und sie als
`hag.events.schenk.lager.avise.received`-Events an den HAG-Connect-Bus
publisht.

Ersetzt den früheren, in `schenk_we_export` eingebauten Pull-Pfad —
jetzt läuft der Pull als eigenständiger Dienst, und der
Web-Service konsumiert das publizierte Event.

## Architektur

```
                                   ┌───────────────────────┐
                                   │  HAG Connect Bus      │
                                   │  NATS JetStream       │
                                   │  hag-events stream    │
                                   └──────────▲────────────┘
                                              │
                          hag.<env>.events.schenk.lager.avise.received
                                  (JetStream-Publish + Dedup-Header)
                                              │
                                   ┌───────────────────────┐
                                   │ hag-schenk-pull       │
                                   │                       │
                                   │  Inner-Loop (tokio)   │
                                   │   every N seconds:    │
                                   │     fetch + enrich    │
                                   │     → publish batch   │
                                   │     → mark strlocked  │
                                   └──────────▲────────────┘
                                              │
                          SELECT i_weavise / m.artikelbasis
                          UPDATE i_weavise SET strlocked='N'
                                              │
                                   ┌───────────────────────┐
                                   │ Legacy Postgres 8.1   │
                                   │ (io_oxaion / m)       │
                                   └───────────────────────┘
```

## Was der Service tut

Pro Iteration:

1. **Fetch**: `SELECT …` auf `io_oxaion.i_weavise` mit Filter
   `strvalid='Y' AND strlocked='T' AND lngroot_id=? AND lngprod_id=?`
   — die alte Lobster-Konvention für "noch nicht verarbeitet".
2. **Enrich**: pro Avis-Zeile `lookup_artikel_info` aus
   `m.artikelbasis` (Bezeichnung, `pal_menge` =
   `nummengeimlademittel × lngbuendelung`, `lngbuendelung`)
   und `prefill_gebinde_typ` aus der Mengeneinheit (FL → FLA,
   KT → KAR).
3. **Publish**: für jede Zeile ein
   `hag.events.schenk.lager.avise.received`-Event über JetStream
   (`hag-events`-Stream). `Nats-Msg-Id` = event_id sorgt für
   Server-seitiges Dedup im Stream-Window.
4. **Mark**: `UPDATE io_oxaion.i_weavise SET strlocked='N'` für alle
   erfolgreich publizierten Avise. Dadurch tauchen sie im nächsten
   Pull nicht wieder auf.

Bei `postgres.read_only = true` wird der UPDATE-Schritt
übersprungen — gut für Dev-Umgebungen, in denen man gegen die
Prod-DB lesen aber nichts ändern darf. In dem Fall sieht der
Publisher dieselben Avise beim nächsten Durchlauf wieder; der
Consumer auf der anderen Seite ist idempotent
(`ON CONFLICT DO NOTHING` auf `(bestellnr, positionsnr, sub_pos)`).

## Tech-Stack

- Rust 1.95.0 (pinned via `rust-toolchain.toml`)
- `tokio` (full) + `async-nats` 0.42 (JetStream) + `sqlx` 0.8 (Postgres)
- `figment` (TOML + Env), `clap`, `tracing` (JSON, daily rollend)

## Getting Started

### Voraussetzungen

- Zugang zur Legacy-Postgres (über SSH-Tunnel, wie bei den anderen
  Hartmann-Services).
- Zugang zum NATS-Server der HAG-Connect-Plattform
  (dev: `nats://192.168.4.128:4222`); Service-User
  `svc-hag-schenk-pull-<env>` + Passwort kommen aus
  [`declare_users.py`](https://gitlab.hartmannag.de/it-hartmann/hag-connect-platform).
- Stream `hag-events-<env>` muss **vorher** per
  `declare_topology.py --apply` provisioniert sein — der Publisher
  deklariert keine Streams.

### Konfiguration

```bash
cp config/config.sample.toml config/config.toml
$EDITOR config/config.toml
```

Die echte `config.toml` steht in `.gitignore`. Credentials besser
per Env-Variable — figment zieht `HAG_SCHENK_PULL__`-Präfix mit
`__` als Sektions-Separator, z.B.
`HAG_SCHENK_PULL__POSTGRES__URL=postgres://...`.

### Bauen + starten

```bash
cargo build --release
./target/release/hag-schenk-pull         # long-running, pollt zyklisch
./target/release/hag-schenk-pull --once  # einmaliger Lauf (Debug/Test)
```

### Tests + Lint

```bash
cargo fmt --check
cargo clippy --release --all-targets -- -D warnings
cargo test --release
```

## Deployment

- `systemd` Unit `hag-schenk-pull.service` startet die Release-Binary
  im Long-Running-Modus.
- Credentials über `/etc/hag-schenk-pull/env` (als `EnvironmentFile=`).
- Logs gehen per `tracing-appender` als JSON in `logging.dir`.
- Für den Postgres-Zugang läuft parallel der bekannte
  `autossh`-Tunnel, dessen Loopback-Port in der `postgres.url`
  steht.

## Bekannte Grenzen

- **Read-only-Flag nur lokal**. Wenn `read_only = true` in der Config
  steht, wird `strlocked` nicht umgesetzt — der Publisher publisht
  dieselben Avise endlos erneut. Nur für Dev-Umgebungen gedacht;
  in Prod muss `false` stehen.
- **Keine persistente Offset-State**. Das Tracking hängt an
  `strlocked` in der Legacy-DB. Wenn dort jemand manuell "Y"
  zurücksetzt, sieht der Publisher die Zeile wieder.
- **Publish-Reihenfolge**: wir ziehen einen Batch, publishen jede
  Zeile einzeln, und setzen dann `strlocked` für alle auf einmal.
  Bei Crash mitten im Batch können einzelne Zeilen publiziert, aber
  nicht als "verarbeitet" markiert werden — beim nächsten Lauf
  gehen sie nochmal raus. Consumer-Idempotenz deckt das ab.

## Related

- **[HAG Connect Platform](https://gitlab.hartmannag.de/it-hartmann/hag-connect-platform)** —
  Event-Contract `hag.events.schenk.lager.avise.received` unter
  `contracts/schemas/events/schenk-lager-avise-received.yaml` (v1.0.1).
- **[Schenk WE Export](https://gitlab.hartmannag.de/it-hartmann/schenk_we_export)** —
  konsumiert das Event; der alte eingebaute Pull-Pfad dort ist
  weggefallen, seit dieser Publisher übernommen hat.

## Lizenz

Intern — Hartmann AG.
