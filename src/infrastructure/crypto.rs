//! The AES-256-GCM credential codec shared with exchange-adapter's
//! account-v2 storage: a sealed row only opens under its own storage
//! context. Blob layout and AAD are byte-compatible with the adapter's
//! `CredentialCryptoV2::open_storage`:
//!
//! - plaintext = JSON `{"api_key": "...", "api_secret": "..."}` (validated:
//!   key ≤ 128 chars, secret ≤ 256 chars, non-empty)
//! - AAD = u32-big-endian length-prefixed fields `[
//!   "exchange-adapter-credential-storage-v2", account UUID string,
//!   user_id, provider, channel, environment, "2" ]`
//! - nonce = the row's 12 `credential_nonce` bytes, ciphertext+tag =
//!   `credential_ciphertext` bytes (PostgreSQL BYTEA — raw, not encoded)
//!
//! The key material is the adapter's `ADAPTER__ACCOUNT_V2_STORAGE_KEY`
//! (standard base64, exactly 32 bytes), delivered through this service's
//! own environment; the key *id* rides alongside and must match the row's
//! `encryption_key_id`. Plaintext credentials never enter a log line or an
//! error message, and live only inside the watcher's memory.

use aes_gcm::Aes256Gcm;
use aes_gcm::Nonce;
use aes_gcm::aead::{Aead, KeyInit, Payload};
use uuid::Uuid;

const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
const MAX_CIPHERTEXT_LEN: usize = 5464;
const STORAGE_FORMAT_VERSION: i16 = 2;
const STORAGE_INFO: &str = "exchange-adapter-credential-storage-v2";

/// Sealed credential bytes exactly as the adapter's tables store them.
#[derive(Debug, Clone)]
pub(crate) struct SealedCredentials {
    pub(crate) ciphertext: Vec<u8>,
    pub(crate) nonce: Vec<u8>,
    pub(crate) encryption_key_id: String,
    pub(crate) encryption_format_version: i16,
}

/// Decrypted, validated credentials — memory-only, never logged.
#[derive(Debug, Clone)]
pub(crate) struct AccountCredentials {
    pub(crate) api_key: String,
    pub(crate) api_secret: String,
}

/// The storage coordinates an envelope is bound to; a row only ever opens
/// under its own.
#[derive(Debug, Clone)]
pub(crate) struct StorageContext {
    pub(crate) exchange_account: Uuid,
    pub(crate) user_id: String,
    pub(crate) provider: String,
    pub(crate) channel: String,
    pub(crate) environment: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CryptoError {
    #[error("credential row does not match the configured decrypt key")]
    KeyMismatch,
    #[error("credential envelope is invalid")]
    InvalidEnvelope,
    #[error("credential decryption failed")]
    Failure,
}

/// One loaded storage key: the deployment-configured id + 32 key bytes.
#[derive(Clone)]
pub(crate) struct StorageKey {
    pub(crate) key_id: String,
    key: [u8; 32],
}

impl StorageKey {
    pub(crate) fn new(key_id: String, key: [u8; 32]) -> Self {
        Self { key_id, key }
    }
}

pub(crate) struct CredentialCrypto {
    storage: StorageKey,
}

impl CredentialCrypto {
    pub(crate) fn new(storage: StorageKey) -> Self {
        Self { storage }
    }

    /// Open one sealed credential row. Every failure is a closed box: no
    /// panic, no partial plaintext, no credential material in errors.
    pub(crate) fn open(
        &self,
        sealed: &SealedCredentials,
        context: &StorageContext,
    ) -> Result<AccountCredentials, CryptoError> {
        if sealed.encryption_format_version != STORAGE_FORMAT_VERSION {
            return Err(CryptoError::InvalidEnvelope);
        }
        if sealed.encryption_key_id != self.storage.key_id {
            return Err(CryptoError::KeyMismatch);
        }
        if sealed.nonce.len() != NONCE_LEN
            || sealed.ciphertext.len() < TAG_LEN
            || sealed.ciphertext.len() > MAX_CIPHERTEXT_LEN
        {
            return Err(CryptoError::InvalidEnvelope);
        }
        let aad = storage_aad(context);
        let cipher =
            Aes256Gcm::new_from_slice(&self.storage.key).map_err(|_| CryptoError::Failure)?;
        let plaintext = cipher
            .decrypt(
                Nonce::from_slice(&sealed.nonce),
                Payload {
                    msg: &sealed.ciphertext,
                    aad: &aad,
                },
            )
            .map_err(|_| CryptoError::InvalidEnvelope)?;
        let payload: StoredCredentialPayload =
            serde_json::from_slice(&plaintext).map_err(|_| CryptoError::InvalidEnvelope)?;
        if payload.api_key.trim().is_empty()
            || payload.api_key.len() > 128
            || payload.api_secret.trim().is_empty()
            || payload.api_secret.len() > 256
        {
            return Err(CryptoError::InvalidEnvelope);
        }
        Ok(AccountCredentials {
            api_key: payload.api_key,
            api_secret: payload.api_secret,
        })
    }
}

#[derive(serde::Deserialize)]
struct StoredCredentialPayload {
    api_key: String,
    api_secret: String,
}

/// Byte-compatible with the adapter's `storage_aad` +
/// `encode_length_prefixed`: every field is u32-BE-length-prefixed.
fn storage_aad(context: &StorageContext) -> Vec<u8> {
    let fields = [
        STORAGE_INFO,
        &context.exchange_account.to_string(),
        &context.user_id,
        &context.provider,
        &context.channel,
        &context.environment,
        "2",
    ];
    let mut aad = Vec::new();
    for field in fields {
        aad.extend_from_slice(&(field.len() as u32).to_be_bytes());
        aad.extend_from_slice(field.as_bytes());
    }
    aad
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> StorageKey {
        StorageKey::new("k-test".to_owned(), [7_u8; 32])
    }

    fn context() -> StorageContext {
        StorageContext {
            exchange_account: Uuid::parse_str("b3c1d2a4-0000-4000-8000-000000000001").unwrap(),
            user_id: "user-9f2b3c".to_owned(),
            provider: "bybit".to_owned(),
            channel: "bybit-linear".to_owned(),
            environment: "testnet".to_owned(),
        }
    }

    /// Seal with the same mechanics the adapter uses (encrypt + AAD) so the
    /// round trip pins the exact blob contract.
    fn seal(
        key: &StorageKey,
        api_key: &str,
        api_secret: &str,
        context: &StorageContext,
    ) -> SealedCredentials {
        use aes_gcm::aead::Aead;
        let cipher = Aes256Gcm::new_from_slice(&key.key).unwrap();
        let nonce_bytes: [u8; 12] = rand_like();
        let plaintext = serde_json::json!({
            "api_key": api_key,
            "api_secret": api_secret,
        })
        .to_string();
        let ciphertext = cipher
            .encrypt(
                Nonce::from_slice(&nonce_bytes),
                Payload {
                    msg: plaintext.as_bytes(),
                    aad: &storage_aad(context),
                },
            )
            .unwrap();
        SealedCredentials {
            ciphertext,
            nonce: nonce_bytes.to_vec(),
            encryption_key_id: key.key_id.clone(),
            encryption_format_version: STORAGE_FORMAT_VERSION,
        }
    }

    fn rand_like() -> [u8; 12] {
        let mut nonce = [0_u8; 12];
        // Deterministic test nonce is fine — the key is test-only.
        nonce.copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        nonce
    }

    #[test]
    fn sealed_credentials_round_trip() {
        let key = test_key();
        let context = context();
        let sealed = seal(&key, "api-key-1", "api-secret-1", &context);
        let crypto = CredentialCrypto::new(key);

        let opened = crypto.open(&sealed, &context).expect("opens");

        assert_eq!(opened.api_key, "api-key-1");
        assert_eq!(opened.api_secret, "api-secret-1");
    }

    #[test]
    fn decrypt_failure_is_a_closed_box() {
        let key = test_key();
        let context = context();
        let mut sealed = seal(&key, "k", "s", &context);
        sealed.ciphertext[0] ^= 0xFF;
        let crypto = CredentialCrypto::new(key);

        let error = crypto.open(&sealed, &context).unwrap_err();

        assert!(!error.to_string().contains("api-key-1"));
        assert!(matches!(
            error,
            CryptoError::InvalidEnvelope | CryptoError::Failure
        ));
    }

    #[test]
    fn wrong_context_fails_closed() {
        let key = test_key();
        let context = context();
        let sealed = seal(&key, "k", "s", &context);
        let mut wrong = context.clone();
        wrong.user_id = "someone-else".to_owned();
        let crypto = CredentialCrypto::new(key);

        assert!(crypto.open(&sealed, &wrong).is_err());
    }

    #[test]
    fn key_id_mismatch_is_key_mismatch() {
        let key = test_key();
        let context = context();
        let mut sealed = seal(&key, "k", "s", &context);
        sealed.encryption_key_id = "other-key".to_owned();
        let crypto = CredentialCrypto::new(key);

        assert!(matches!(
            crypto.open(&sealed, &context),
            Err(CryptoError::KeyMismatch)
        ));
    }

    #[test]
    fn aad_is_length_prefixed_like_the_adapter() {
        let context = context();
        let aad = storage_aad(&context);
        // First field: u32-BE length of the storage info string, then bytes.
        let info_len = u32::from_be_bytes([aad[0], aad[1], aad[2], aad[3]]) as usize;
        assert_eq!(info_len, STORAGE_INFO.len());
        assert_eq!(&aad[4..4 + info_len], STORAGE_INFO.as_bytes());
    }
}
