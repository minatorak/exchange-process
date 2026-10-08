//! Per-account watcher lifecycle: a sweep keeps every active Bybit account
//! watched. The created event is the fast path (the created-consumer asks
//! the supervisor to ensure a watcher), the resweep is the safety net — an
//! account whose event was lost still gets watched on the next tick, which
//! is what makes the event a fast-path rather than a guarantee. One
//! account's failure (decrypt, connect) never touches the others.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bybit_rs::bybit::private_ws::{PrivateConnection, TESTNET_PRIVATE_WS};
use bybit_rs::bybit::rest::{BybitRest, HttpBybitClient};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use uuid::Uuid;

use crate::domain::repo::PositionRepository;
use crate::infrastructure::crypto::AccountCredentials;
use crate::infrastructure::postgres::accounts::{AccountSource, ActiveAccount};
use crate::infrastructure::watcher::{
    RestSource, WatchError, Watcher, WatcherConfig, WsConnector, WsSession,
};

/// bybit-linear tracks the USDT-margined linear perps of the account.
const SETTLE_COIN: &str = "USDT";
/// Cursor-walk cap of the closed-pnl backfill per close (real pages are
/// 50–200 rows; the cap only bounds a misbehaving upstream).
const MAX_CLOSED_PNL_PAGES: usize = 20;
const MAINNET_PRIVATE_WS: &str = "wss://stream.bybit.com/v5/private";
const UPSTREAM_BUDGET: Duration = Duration::from_secs(4);

/// Spawns one account's watcher task. Implementations own the real wiring
/// (decrypt → connect → run); the supervisor only tracks cancellation.
#[async_trait]
pub(crate) trait WatcherSpawner: Send + Sync {
    async fn spawn(&self, account: ActiveAccount) -> Result<CancellationToken, WatchError>;
}

/// The sweep loop. `ensure_watcher` is the fast-path hook for the created
/// consumer; `run` resweeps every `account_resweep_secs` as the safety net.
pub(crate) struct AccountSupervisor {
    source: Arc<dyn AccountSource>,
    spawner: Arc<dyn WatcherSpawner>,
    watched: tokio::sync::Mutex<HashMap<Uuid, CancellationToken>>,
    resweep_secs: u64,
}

impl AccountSupervisor {
    pub(crate) fn new(
        source: Arc<dyn AccountSource>,
        spawner: Arc<dyn WatcherSpawner>,
        resweep_secs: u64,
    ) -> Self {
        Self {
            source,
            spawner,
            watched: tokio::sync::Mutex::new(HashMap::new()),
            resweep_secs,
        }
    }

    /// Fast path: make sure this account is watched right now.
    pub(crate) async fn ensure_watcher(&self, account: Uuid) {
        if self.watched.lock().await.contains_key(&account) {
            return;
        }
        self.spawn_if_known(account).await;
    }

    async fn spawn_if_known(&self, account: Uuid) {
        let accounts = match self.source.active_accounts().await {
            Ok(accounts) => accounts,
            Err(error) => {
                error!(%error, "account sweep failed");
                return;
            }
        };
        let Some(active) = accounts
            .into_iter()
            .find(|active| active.exchange_account == account)
        else {
            return;
        };
        self.spawn_one(active).await;
    }

    async fn spawn_one(&self, account: ActiveAccount) {
        let mut watched = self.watched.lock().await;
        if watched.contains_key(&account.exchange_account) {
            return;
        }
        match self.spawner.spawn(account.clone()).await {
            Ok(token) => {
                info!(account = %account.exchange_account, "watcher spawned");
                watched.insert(account.exchange_account, token);
            }
            Err(error) => {
                // Isolated to this account; the next sweep retries.
                error!(account = %account.exchange_account, %error, "watcher spawn failed; will retry next sweep");
            }
        }
    }

    /// The safety-net sweep + cooperative shutdown of every watcher.
    pub(crate) async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let mut ticker = tokio::time::interval(Duration::from_secs(self.resweep_secs.max(1)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    for (_, token) in self.watched.lock().await.drain() {
                        token.cancel();
                    }
                    return;
                }
                _ = ticker.tick() => self.sweep().await,
            }
        }
    }

    /// One sweep: spawn watchers for every active account that has none.
    async fn sweep(&self) {
        match self.source.active_accounts().await {
            Ok(accounts) => {
                for account in accounts {
                    self.spawn_one(account).await;
                }
            }
            Err(error) => error!(%error, "account resweep failed"),
        }
    }
}

/// The production wiring: decrypt once per spawn, build the private WS and
/// REST sources, and hand the watcher its own task. A decrypt failure is a
/// spawn failure — this account waits for the next sweep, untouched others
/// keep running (Review Focus 5).
pub(crate) struct BybitWatcherSpawner<Repo: PositionRepository + 'static> {
    pub(crate) source: Arc<dyn AccountSource>,
    pub(crate) repo: Arc<Repo>,
    pub(crate) config: WatcherConfig,
}

#[async_trait]
impl<Repo: PositionRepository + 'static> WatcherSpawner for BybitWatcherSpawner<Repo> {
    async fn spawn(&self, account: ActiveAccount) -> Result<CancellationToken, WatchError> {
        let credentials = self
            .source
            .credentials(account.exchange_account)
            .await
            .map_err(|error| WatchError::Ws(error.to_string()))?;
        let connector = BybitConnector {
            testnet: account.testnet,
            credentials: credentials.clone(),
        };
        let rest = Arc::new(BybitRestSource {
            testnet: account.testnet,
            credentials,
        });
        let watcher: Watcher<BybitSession, BybitRestSource, Repo> = Watcher::new(
            Arc::new(connector),
            rest,
            self.repo.clone(),
            account.clone(),
            "bybit-linear",
            self.config.clone(),
        );
        let token = CancellationToken::new();
        let task_token = token.clone();
        tokio::spawn(watcher.run(task_token));
        Ok(token)
    }
}

/// The real private-WS session.
pub(crate) struct BybitSession {
    connection: PrivateConnection,
}

#[async_trait]
impl WsSession for BybitSession {
    async fn next_message(
        &mut self,
    ) -> Result<Option<bybit_rs::bybit::private_ws::PrivateMessage>, WatchError> {
        self.connection
            .next_message()
            .await
            .map_err(|error| WatchError::Ws(error.to_string()))
    }
}

pub(crate) struct BybitConnector {
    testnet: bool,
    credentials: AccountCredentials,
}

#[async_trait]
impl WsConnector for BybitConnector {
    type Session = BybitSession;

    async fn connect(&self) -> Result<BybitSession, WatchError> {
        let url = if self.testnet {
            TESTNET_PRIVATE_WS
        } else {
            MAINNET_PRIVATE_WS
        };
        let credentials = bybit_rs::config::ApiCredentials {
            key: self.credentials.api_key.clone(),
            secret: self.credentials.api_secret.clone(),
        };
        let connection = PrivateConnection::connect(url, &credentials)
            .await
            .map_err(|error| WatchError::Ws(error.to_string()))?;
        Ok(BybitSession { connection })
    }
}

/// The real REST reads: fresh per call, one upstream budget per call.
pub(crate) struct BybitRestSource {
    testnet: bool,
    credentials: AccountCredentials,
}

impl BybitRestSource {
    fn client(&self) -> Result<HttpBybitClient, WatchError> {
        let environment = if self.testnet {
            bybit_rs::domain::TradingEnvironment::Testnet
        } else {
            bybit_rs::domain::TradingEnvironment::Mainnet
        };
        let credentials = bybit_rs::config::ApiCredentials {
            key: self.credentials.api_key.clone(),
            secret: self.credentials.api_secret.clone(),
        };
        HttpBybitClient::with_base_url(
            environment.rest_base_url(),
            UPSTREAM_BUDGET,
            Some(credentials),
        )
        .map_err(|error| WatchError::Rest(error.to_string()))
    }
}

#[async_trait]
impl RestSource for BybitRestSource {
    async fn position_list(&self) -> Result<Vec<bybit_rs::bybit::dto::PositionDto>, WatchError> {
        let list = self
            .client()?
            .position_list(SETTLE_COIN)
            .await
            .map_err(|error| WatchError::Rest(error.to_string()))?;
        Ok(list)
    }

    async fn closed_pnl(
        &self,
        symbol: &str,
        from_ms: i64,
    ) -> Result<Vec<bybit_rs::bybit::dto::ClosedPnlDto>, WatchError> {
        let client = self.client()?;
        let parsed_symbol = bybit_rs::domain::Symbol::new(symbol).map_err(WatchError::Rest)?;
        // Walk the cursor until it ends or the page predates the position.
        let mut records = Vec::new();
        let mut cursor = String::new();
        for _ in 0..MAX_CLOSED_PNL_PAGES {
            let (page, next_cursor) = client
                .closed_pnl_records(&parsed_symbol, &cursor)
                .await
                .map_err(|error| WatchError::Rest(error.to_string()))?;
            let oldest = page
                .iter()
                .filter_map(|record| record.updated_time.parse::<i64>().ok())
                .min();
            let reached_history = oldest.is_some_and(|oldest| oldest < from_ms);
            records.extend(
                page.into_iter()
                    .filter(|record| record.updated_time.parse::<i64>().unwrap_or(0) >= from_ms),
            );
            if next_cursor.is_empty() || reached_history {
                break;
            }
            cursor = next_cursor;
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::crypto::CryptoError;
    use std::sync::Mutex;

    struct FakeSource {
        accounts: Mutex<Vec<ActiveAccount>>,
        fail_credentials: Mutex<Vec<Uuid>>,
    }

    fn active(id: &str) -> ActiveAccount {
        ActiveAccount {
            exchange_account: Uuid::parse_str(id).unwrap(),
            user_id: format!("user-{id}"),
            testnet: true,
        }
    }

    #[async_trait]
    impl AccountSource for FakeSource {
        async fn active_accounts(
            &self,
        ) -> Result<Vec<ActiveAccount>, crate::infrastructure::postgres::accounts::AccountError>
        {
            Ok(self.accounts.lock().unwrap().clone())
        }

        async fn credentials(
            &self,
            account: Uuid,
        ) -> Result<AccountCredentials, crate::infrastructure::postgres::accounts::AccountError>
        {
            if self.fail_credentials.lock().unwrap().contains(&account) {
                return Err(
                    crate::infrastructure::postgres::accounts::AccountError::DecryptFailed(account),
                );
            }
            Ok(AccountCredentials {
                api_key: "k".to_owned(),
                api_secret: "s".to_owned(),
            })
        }
    }

    struct FakeSpawner {
        spawned: Mutex<Vec<Uuid>>,
        fail_for: Mutex<Vec<Uuid>>,
    }

    #[async_trait]
    impl WatcherSpawner for FakeSpawner {
        async fn spawn(&self, account: ActiveAccount) -> Result<CancellationToken, WatchError> {
            if self
                .fail_for
                .lock()
                .unwrap()
                .contains(&account.exchange_account)
            {
                return Err(WatchError::Ws(CryptoError::Failure.to_string()));
            }
            self.spawned.lock().unwrap().push(account.exchange_account);
            Ok(CancellationToken::new())
        }
    }

    fn supervisor(source: Arc<FakeSource>, spawner: Arc<FakeSpawner>) -> Arc<AccountSupervisor> {
        Arc::new(AccountSupervisor::new(source, spawner, 60))
    }

    #[tokio::test]
    async fn supervisor_spawns_missing_accounts_and_survives_individual_failure() {
        let account_a = "b3c1d2a4-0000-4000-8000-00000000000a";
        let account_b = "b3c1d2a4-0000-4000-8000-00000000000b";
        let source = Arc::new(FakeSource {
            accounts: Mutex::new(vec![active(account_a), active(account_b)]),
            fail_credentials: Mutex::new(vec![Uuid::parse_str(account_b).unwrap()]),
        });
        let spawner = Arc::new(FakeSpawner {
            spawned: Mutex::new(Vec::new()),
            fail_for: Mutex::new(vec![Uuid::parse_str(account_b).unwrap()]),
        });
        let supervisor = supervisor(source.clone(), spawner.clone());

        // Sweep one: A spawns, B fails — and the sweep does not stop there.
        supervisor.sweep().await;
        assert_eq!(
            *spawner.spawned.lock().unwrap(),
            vec![source.accounts.lock().unwrap()[0].exchange_account]
        );

        // Next tick: B's decrypt succeeds and it gets its watcher.
        source.fail_credentials.lock().unwrap().clear();
        spawner.fail_for.lock().unwrap().clear();
        supervisor.sweep().await;
        let spawned = spawner.spawned.lock().unwrap();
        assert!(spawned.contains(&Uuid::parse_str(account_a).unwrap()));
        assert!(spawned.contains(&Uuid::parse_str(account_b).unwrap()));
        assert_eq!(spawned.len(), 2, "no duplicate spawns for watched accounts");
    }
}
