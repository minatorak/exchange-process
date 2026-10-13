//! Read-only access to exchange-adapter's account tables — the access
//! contract: the ONLY Adapter-owned tables this service may touch are
//! `exchange.exchange_accounts_v2` and `exchange.exchange_account_credentials_v2`, read-only.
//! Credentials decrypt per call inside the watcher's memory.

use sqlx::PgPool;
use uuid::Uuid;

use crate::infrastructure::crypto::{
    AccountCredentials, CredentialCrypto, SealedCredentials, StorageContext,
};

const ACCOUNTS_SQL: &str = "SELECT exchange_account, user_id, environment \
    FROM exchange.exchange_accounts_v2 \
    WHERE status = 'active' AND provider = 'bybit'";
const CREDENTIALS_SQL: &str = "SELECT a.user_id, a.provider, a.channel, a.environment, \
    c.credential_ciphertext, c.credential_nonce, c.encryption_key_id, \
    c.encryption_format_version \
    FROM exchange.exchange_accounts_v2 a \
    JOIN exchange.exchange_account_credentials_v2 c USING (exchange_account) \
    WHERE a.exchange_account = $1 AND a.status = 'active' AND a.provider = 'bybit'";

#[derive(Debug, thiserror::Error)]
pub(crate) enum AccountError {
    #[error("database unavailable")]
    Unavailable,
    #[error("account {0} has no usable credentials")]
    CredentialsMissing(Uuid),
    #[error("account credentials do not decrypt")]
    DecryptFailed(Uuid),
}

/// One active Bybit account of the mirror.
#[derive(Debug, Clone)]
pub(crate) struct ActiveAccount {
    pub(crate) exchange_account: Uuid,
    pub(crate) user_id: String,
    pub(crate) testnet: bool,
}

/// Who the supervisor watches and with which credentials.
#[async_trait::async_trait]
pub(crate) trait AccountSource: Send + Sync {
    async fn active_accounts(&self) -> Result<Vec<ActiveAccount>, AccountError>;

    /// Decrypt per call, memory only. A decrypt failure isolates the single
    /// account — the caller logs it and moves on to the others.
    async fn credentials(&self, account: Uuid) -> Result<AccountCredentials, AccountError>;
}

pub(crate) struct AccountSourcePg {
    pool: PgPool,
    crypto: CredentialCrypto,
}

impl AccountSourcePg {
    pub(crate) fn new(pool: PgPool, crypto: CredentialCrypto) -> Self {
        Self { pool, crypto }
    }
}

#[async_trait::async_trait]
impl AccountSource for AccountSourcePg {
    async fn active_accounts(&self) -> Result<Vec<ActiveAccount>, AccountError> {
        let rows = sqlx::query_as::<_, AccountRow>(ACCOUNTS_SQL)
            .fetch_all(&self.pool)
            .await
            .map_err(|_| AccountError::Unavailable)?;
        Ok(rows
            .into_iter()
            .map(|row| ActiveAccount {
                exchange_account: row.exchange_account,
                user_id: row.user_id,
                testnet: row.environment == "testnet",
            })
            .collect())
    }

    async fn credentials(&self, account: Uuid) -> Result<AccountCredentials, AccountError> {
        let row = sqlx::query_as::<_, CredentialRow>(CREDENTIALS_SQL)
            .bind(account)
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| AccountError::Unavailable)?
            .ok_or(AccountError::CredentialsMissing(account))?;

        let context = StorageContext {
            exchange_account: account,
            user_id: row.user_id,
            provider: row.provider,
            channel: row.channel,
            environment: row.environment,
        };
        let sealed = SealedCredentials {
            ciphertext: row.credential_ciphertext,
            nonce: row.credential_nonce,
            encryption_key_id: row.encryption_key_id,
            encryption_format_version: row.encryption_format_version,
        };
        self.crypto
            .open(&sealed, &context)
            .map_err(|_| AccountError::DecryptFailed(account))
    }
}

#[derive(sqlx::FromRow)]
struct AccountRow {
    exchange_account: Uuid,
    user_id: String,
    environment: String,
}

#[derive(sqlx::FromRow)]
struct CredentialRow {
    user_id: String,
    provider: String,
    channel: String,
    environment: String,
    credential_ciphertext: Vec<u8>,
    credential_nonce: Vec<u8>,
    encryption_key_id: String,
    encryption_format_version: i16,
}

#[cfg(test)]
mod tests {
    use super::{ACCOUNTS_SQL, CREDENTIALS_SQL};

    /// Test the actual runtime queries, not duplicate test-only SQL.
    #[test]
    fn queries_are_read_only_and_pinned() {
        for query in [ACCOUNTS_SQL, CREDENTIALS_SQL] {
            assert!(query.contains("exchange.exchange_accounts_v2"));
            assert!(!query.contains("public."));
            assert!(!query.contains("INSERT"));
            assert!(!query.contains("UPDATE "));
            assert!(!query.contains("DELETE"));
            assert!(!query.contains("credential_operations_v2"));
        }
        assert!(CREDENTIALS_SQL.contains("exchange.exchange_account_credentials_v2"));
        assert!(!CREDENTIALS_SQL.contains("exchange_accounts_v1"));
    }
}
