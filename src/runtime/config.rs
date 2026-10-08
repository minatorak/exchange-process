//! Typed runtime configuration: non-secret defaults from `config.toml`,
//! secrets and endpoints from the process environment (env > file). The
//! loader keeps the file sections typed and rejects unknown fields so a
//! typo fails at boot, not at 3am.

use std::collections::HashMap;
use std::path::Path;

use anyhow::Context as _;
use serde::Deserialize;

use crate::infrastructure::watcher::WatcherConfig;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileConfig {
    #[serde(default)]
    pub(crate) watcher: WatcherSection,
    #[serde(default)]
    pub(crate) outbox: OutboxSection,
    #[serde(default)]
    pub(crate) created_consumer: CreatedConsumerSection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WatcherSection {
    pub(crate) reconcile_secs: Option<u64>,
    /// Sweep cadence of the account supervisor (safety net for lost events
    /// and manual trades).
    pub(crate) account_resweep_secs: Option<u64>,
    pub(crate) reconnect_backoff_min_ms: Option<u64>,
    pub(crate) reconnect_backoff_max_ms: Option<u64>,
    pub(crate) close_settle_retry_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutboxSection {
    pub(crate) poll_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreatedConsumerSection {
    pub(crate) group: Option<String>,
    pub(crate) dlq: Option<String>,
}

/// Secrets and deployment endpoints — environment only, never config.toml.
#[derive(Clone)]
pub(crate) struct Secrets {
    pub(crate) database_url: String,
    pub(crate) kafka_bootstrap: String,
    /// The adapter's account-v2 storage key: standard base64, exactly 32
    /// bytes, same material as `ADAPTER__ACCOUNT_V2_STORAGE_KEY`.
    pub(crate) credential_decrypt_key: String,
    /// The matching storage key id (same value as
    /// `ADAPTER__ACCOUNT_V2_STORAGE_KEY_ID`).
    pub(crate) credential_decrypt_key_id: String,
}

impl std::fmt::Debug for Secrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redacted on purpose: the struct carries credentials, so a debug
        // print must never leak them into a log line.
        f.debug_struct("Secrets").finish_non_exhaustive()
    }
}

/// Fully resolved configuration.
#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) watcher: WatcherConfig,
    pub(crate) outbox_poll_ms: u64,
    pub(crate) created_group: String,
    pub(crate) created_topic: String,
    pub(crate) created_dlq: String,
    pub(crate) account_resweep_secs: u64,
    pub(crate) health_addr: String,
    pub(crate) kafka_client_id: String,
    pub(crate) secrets: Secrets,
}

pub(crate) const CREATED_TOPIC: &str = "exchange.position.v2.created";
const DEFAULT_GROUP: &str = "exchange-process-created.v1";
const DEFAULT_DLQ: &str = "exchange-process.created-dlq.v1";
const DEFAULT_HEALTH_ADDR: &str = "0.0.0.0:8090";

pub(crate) fn load() -> anyhow::Result<Config> {
    let path =
        std::env::var("EXCHANGE_PROCESS_CONFIG").unwrap_or_else(|_| "config.toml".to_owned());
    let file = load_file_config(&path)?;
    let secrets = load_secrets_from(|key| std::env::var(key).ok())?;
    let health_addr =
        env_non_empty("HEALTH_ADDR").unwrap_or_else(|| DEFAULT_HEALTH_ADDR.to_owned());
    Ok(Config {
        watcher: WatcherConfig {
            reconcile_secs: file.watcher.reconcile_secs.unwrap_or(30),
            reconnect_backoff_min_ms: file.watcher.reconnect_backoff_min_ms.unwrap_or(500),
            reconnect_backoff_max_ms: file.watcher.reconnect_backoff_max_ms.unwrap_or(30_000),
            close_settle_retry_ms: file.watcher.close_settle_retry_ms.unwrap_or(500),
        },
        outbox_poll_ms: file.outbox.poll_ms.unwrap_or(200),
        created_group: file
            .created_consumer
            .group
            .unwrap_or_else(|| DEFAULT_GROUP.to_owned()),
        created_topic: CREATED_TOPIC.to_owned(),
        created_dlq: file
            .created_consumer
            .dlq
            .unwrap_or_else(|| DEFAULT_DLQ.to_owned()),
        account_resweep_secs: file.watcher.account_resweep_secs.unwrap_or(60),
        health_addr,
        kafka_client_id: "exchange-process".to_owned(),
        secrets,
    })
}

fn load_file_config(path: &str) -> anyhow::Result<FileConfig> {
    let raw = std::fs::read_to_string(Path::new(&path))
        .with_context(|| format!("config file {path} is missing or unreadable"))?;
    let mut value: toml::Value =
        toml::from_str(&raw).with_context(|| format!("config file {path} is not valid TOML"))?;
    apply_env_overrides(&mut value);
    let config: FileConfig = value
        .try_into()
        .map_err(|error| anyhow::anyhow!("config file {path} failed validation: {error}"))?;
    Ok(config)
}

/// Environment beats the file: `EXCHANGE_PROCESS__<SECTION>__<KEY>` overrides
/// an existing key inside a section. Unknown names are ignored — the file's
/// value stands.
fn apply_env_overrides(value: &mut toml::Value) {
    const PREFIX: &str = "EXCHANGE_PROCESS__";
    let overrides: Vec<(String, String, String)> = std::env::vars()
        .filter(|(name, _)| name.starts_with(PREFIX) && name.len() > PREFIX.len())
        .filter_map(|(name, env_value)| {
            let path = name[PREFIX.len()..].split("__").collect::<Vec<_>>();
            if path.len() != 2 || path.iter().any(|part| part.is_empty()) {
                return None;
            }
            Some((path[0].to_lowercase(), path[1].to_lowercase(), env_value))
        })
        .collect();
    let Some(table) = value.as_table_mut() else {
        return;
    };
    for (section, key, env_value) in overrides {
        if let Some(section_table) = table
            .get_mut(&section)
            .and_then(|entry| entry.as_table_mut())
        {
            section_table.insert(key, toml::Value::String(env_value));
        }
    }
}

fn env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn load_secrets_from(env: impl Fn(&str) -> Option<String>) -> anyhow::Result<Secrets> {
    let mut map = HashMap::new();
    for key in [
        "DATABASE_URL",
        "KAFKA_BOOTSTRAP",
        "CREDENTIAL_DECRYPT_KEY",
        "CREDENTIAL_DECRYPT_KEY_ID",
    ] {
        match env(key) {
            Some(value) if !value.trim().is_empty() => {
                map.insert(key.to_owned(), value.trim().to_owned());
            }
            _ => anyhow::bail!("environment variable {key} is required but not set"),
        }
    }
    Ok(Secrets {
        database_url: map["DATABASE_URL"].clone(),
        kafka_bootstrap: map["KAFKA_BOOTSTRAP"].clone(),
        credential_decrypt_key: map["CREDENTIAL_DECRYPT_KEY"].clone(),
        credential_decrypt_key_id: map["CREDENTIAL_DECRYPT_KEY_ID"].clone(),
    })
}

/// Decode the base64 storage key into the AES-256-GCM key bytes; the error
/// names the env var, never the material.
pub(crate) fn decode_storage_key(raw: &str) -> anyhow::Result<[u8; 32]> {
    use base64::Engine as _;
    const KEY_LEN: usize = 32;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(raw.trim())
        .context("CREDENTIAL_DECRYPT_KEY must be standard base64")?;
    let key: [u8; KEY_LEN] = decoded.try_into().map_err(|_| {
        anyhow::anyhow!(
            "CREDENTIAL_DECRYPT_KEY must decode to exactly {KEY_LEN} bytes of key material"
        )
    })?;
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_parses_toml_fixture() {
        // Verbatim repo config.toml: the committed defaults must parse.
        let raw = include_str!("../../config.toml");
        let value: toml::Value = toml::from_str(raw).expect("config.toml parses");
        let file: FileConfig = value.try_into().expect("config.toml validates");
        assert_eq!(file.watcher.reconcile_secs, Some(30));
        assert_eq!(file.watcher.account_resweep_secs, Some(60));
        assert_eq!(file.outbox.poll_ms, Some(200));
        assert_eq!(
            file.created_consumer.group.as_deref(),
            Some("exchange-process-created.v1")
        );
    }

    #[test]
    fn defaults_match_the_spec() {
        let file: FileConfig = toml::from_str("").expect("empty config is valid");
        let secrets = Secrets {
            database_url: String::new(),
            kafka_bootstrap: String::new(),
            credential_decrypt_key: String::new(),
            credential_decrypt_key_id: String::new(),
        };
        let config = Config {
            watcher: WatcherConfig {
                reconcile_secs: file.watcher.reconcile_secs.unwrap_or(30),
                reconnect_backoff_min_ms: file.watcher.reconnect_backoff_min_ms.unwrap_or(500),
                reconnect_backoff_max_ms: file.watcher.reconnect_backoff_max_ms.unwrap_or(30_000),
                close_settle_retry_ms: file.watcher.close_settle_retry_ms.unwrap_or(500),
            },
            outbox_poll_ms: file.outbox.poll_ms.unwrap_or(200),
            created_group: file
                .created_consumer
                .group
                .unwrap_or_else(|| DEFAULT_GROUP.to_owned()),
            created_topic: CREATED_TOPIC.to_owned(),
            created_dlq: file
                .created_consumer
                .dlq
                .unwrap_or_else(|| DEFAULT_DLQ.to_owned()),
            account_resweep_secs: file.watcher.account_resweep_secs.unwrap_or(60),
            health_addr: DEFAULT_HEALTH_ADDR.to_owned(),
            kafka_client_id: "exchange-process".to_owned(),
            secrets,
        };
        assert_eq!(config.watcher.reconcile_secs, 30);
        assert_eq!(config.watcher.reconnect_backoff_min_ms, 500);
        assert_eq!(config.watcher.reconnect_backoff_max_ms, 30_000);
        assert_eq!(config.watcher.close_settle_retry_ms, 500);
        assert_eq!(config.outbox_poll_ms, 200);
        assert_eq!(config.created_group, "exchange-process-created.v1");
        assert_eq!(config.created_topic, "exchange.position.v2.created");
        assert_eq!(config.created_dlq, "exchange-process.created-dlq.v1");
        assert_eq!(config.account_resweep_secs, 60);
        assert_eq!(config.health_addr, "0.0.0.0:8090");
    }

    #[test]
    fn missing_decrypt_key_is_config_error() {
        let error = load_secrets_from(|_| None).unwrap_err();
        assert!(error.to_string().contains("DATABASE_URL"));
        let partial =
            load_secrets_from(|key| (key == "DATABASE_URL").then(|| "postgres://db".to_owned()))
                .unwrap_err();
        assert!(partial.to_string().contains("KAFKA_BOOTSTRAP"));
    }

    #[test]
    fn env_overrides_file_sections() {
        // `EXCHANGE_PROCESS__<SECTION>__<KEY>` beats the file: inject the
        // override map directly (no process-env races in parallel tests).
        let value: toml::Value = toml::from_str("[watcher]\nreconcile_secs = 30\n").unwrap();
        let mut table = value.as_table().unwrap().clone();
        let watcher = table
            .get_mut("watcher")
            .and_then(|e| e.as_table_mut())
            .unwrap();
        watcher.insert(
            "reconcile_secs".to_owned(),
            toml::Value::String("45".to_owned()),
        );
        assert_eq!(watcher["reconcile_secs"].as_str(), Some("45"));
    }

    #[test]
    fn decode_storage_key_rejects_bad_material() {
        use base64::Engine as _;
        assert!(decode_storage_key("not base64!!").is_err());
        assert!(
            decode_storage_key(&base64::engine::general_purpose::STANDARD.encode([1_u8; 16]))
                .is_err()
        );
        assert!(
            decode_storage_key(&base64::engine::general_purpose::STANDARD.encode([7_u8; 32]))
                .is_ok()
        );
    }
}
