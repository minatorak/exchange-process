//! Typed, non-secret runtime configuration. `config.toml` carries committed
//! defaults; every key has a serde default so an older config file keeps
//! loading when new keys appear. Secrets never live in the file — they
//! arrive via the process environment when a real integration lands.

use std::fs;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub log: LogConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogConfig {
    #[serde(default = "default_level")]
    pub level: String,
}

fn default_level() -> String {
    "info".to_owned()
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_level(),
        }
    }
}

impl AppConfig {
    /// Loads the TOML config from `path` and validates it; a missing file or
    /// a malformed key fails fast with the path in the message and no secret
    /// value in it. Precedence for overrides: process environment >
    /// `.env.local` > this file.
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let raw = fs::read_to_string(path)
            .map_err(|error| anyhow::anyhow!("cannot read config file {path}: {error}"))?;
        let config: AppConfig = toml::from_str(&raw)
            .map_err(|error| anyhow::anyhow!("invalid config file {path}: {error}"))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.log.level.trim().is_empty(),
            "log.level must not be blank"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_document_takes_every_default() {
        let config: AppConfig = toml::from_str("").expect("defaults always parse");
        assert_eq!(config.log.level, "info");
    }

    #[test]
    fn provided_keys_override_defaults() {
        let config: AppConfig = toml::from_str("[log]\nlevel = 'debug'\n").expect("parses");
        assert_eq!(config.log.level, "debug");
    }

    #[test]
    fn load_names_the_missing_file() {
        let error = AppConfig::load("/nonexistent/config.toml").unwrap_err();
        assert!(error.to_string().contains("/nonexistent/config.toml"));
    }

    #[test]
    fn blank_values_fail_validation() {
        let config: AppConfig = toml::from_str("[log]\nlevel = '  '\n").expect("parses");
        assert!(config.validate().is_err());
    }
}
