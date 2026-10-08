//! The per-account watcher: private WS position pushes + periodic REST
//! reconcile into one mirror pipeline, and the close flow that fetches the
//! exchange's settled close records (`/v5/position/closed-pnl`) before it
//! flattens the mirror. Everything below the mapping boundary treats
//! exchange payloads as untrusted and the repository as the only state.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::application::ingest::{IngestOutcome, MirrorIdentity, MirrorIngestor};
use crate::domain::position::{
    ClosedPnlRecord, ClosedTotals, PositionSnapshot, Side, aggregate_closed,
};
use crate::domain::repo::{ChangeSource, CloseWrite, PositionRepository};

/// What can go wrong inside a watcher. Every variant is account-local:
/// nothing here may take down sibling accounts.
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum WatchError {
    #[error("websocket session failed: {0}")]
    Ws(String),
    #[error("rest read failed: {0}")]
    Rest(String),
}

#[derive(Debug, Clone)]
pub(crate) struct WatcherConfig {
    pub(crate) reconcile_secs: u64,
    pub(crate) reconnect_backoff_min_ms: u64,
    pub(crate) reconnect_backoff_max_ms: u64,
    pub(crate) close_settle_retry_ms: u64,
}

/// One connected private-WS session.
#[async_trait]
pub(crate) trait WsSession: Send {
    /// Next private message, `None` when the session ended (closed socket).
    async fn next_message(
        &mut self,
    ) -> Result<Option<bybit_rs::bybit::private_ws::PrivateMessage>, WatchError>;
}

/// Builds sessions — the reconnect path calls it again after every failure.
#[async_trait]
pub(crate) trait WsConnector: Send + Sync {
    type Session: WsSession;
    async fn connect(&self) -> Result<Self::Session, WatchError>;
}

/// The REST reads the watcher needs: the reconcile snapshot and the settled
/// close records of one symbol since `from_ms` (cursor walked internally).
#[async_trait]
pub(crate) trait RestSource: Send + Sync {
    async fn position_list(&self) -> Result<Vec<bybit_rs::bybit::dto::PositionDto>, WatchError>;
    async fn closed_pnl(
        &self,
        symbol: &str,
        from_ms: i64,
    ) -> Result<Vec<bybit_rs::bybit::dto::ClosedPnlDto>, WatchError>;
}

/// Settle budget: after seeing `size = 0`, the closed-pnl ledger may lag the
/// matching engine; retry this many times before falling back.
const CLOSE_SETTLE_ATTEMPTS: u32 = 3;

pub(crate) struct Watcher<W: WsSession, R: RestSource, Repo: PositionRepository + 'static> {
    connector: Arc<dyn WsConnector<Session = W> + Send + Sync>,
    rest: Arc<R>,
    ingestor: MirrorIngestor<Repo>,
    repo: Arc<Repo>,
    account: crate::infrastructure::postgres::accounts::ActiveAccount,
    identity: MirrorIdentity,
    config: WatcherConfig,
}

impl<W: WsSession, R: RestSource, Repo: PositionRepository + 'static> Watcher<W, R, Repo> {
    pub(crate) fn new(
        connector: Arc<dyn WsConnector<Session = W> + Send + Sync>,
        rest: Arc<R>,
        repo: Arc<Repo>,
        account: crate::infrastructure::postgres::accounts::ActiveAccount,
        channel: &str,
        config: WatcherConfig,
    ) -> Self {
        Self {
            connector,
            rest,
            ingestor: MirrorIngestor::new(repo.clone()),
            repo,
            identity: MirrorIdentity {
                user_id: account.user_id.clone(),
                channel: channel.to_owned(),
            },
            account,
            config,
        }
    }

    /// Runs until `cancel`: WS loop with bounded-backoff reconnects, the
    /// reconcile tick, and close detection through both paths.
    pub(crate) async fn run(self, cancel: CancellationToken) {
        let mut backoff_ms = self.config.reconnect_backoff_min_ms;
        loop {
            if cancel.is_cancelled() {
                return;
            }
            match self.connector.connect().await {
                Ok(mut session) => {
                    backoff_ms = self.config.reconnect_backoff_min_ms;
                    info!(account = %self.account.exchange_account, "private ws session connected");
                    let outcome = self.session_loop(&mut session, &cancel).await;
                    if cancel.is_cancelled() {
                        return;
                    }
                    match outcome {
                        Ok(()) => {
                            debug!(account = %self.account.exchange_account, "private ws session ended; reconnecting")
                        }
                        Err(error) => {
                            error!(account = %self.account.exchange_account, %error, "private ws session failed; reconnecting")
                        }
                    }
                }
                Err(error) => {
                    // A failing connect is this account's problem only.
                    error!(account = %self.account.exchange_account, %error, "private ws connect failed");
                }
            }
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
            }
            backoff_ms = (backoff_ms * 2).min(self.config.reconnect_backoff_max_ms);
        }
    }

    /// One connected period: messages and the reconcile tick share the loop.
    async fn session_loop(
        &self,
        session: &mut W,
        cancel: &CancellationToken,
    ) -> Result<(), WatchError> {
        let mut ticker =
            tokio::time::interval(Duration::from_secs(self.config.reconcile_secs.max(1)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Last observation per symbol — the seq dedupe memory. The mirror
        // stays authoritative in the database; this cache only filters
        // re-pushes.
        let mut last_seen: HashMap<String, PositionSnapshot> = HashMap::new();
        loop {
            tokio::select! {
                () = cancel.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    if let Err(error) = self.reconcile_once(&mut last_seen).await {
                        error!(account = %self.account.exchange_account, %error, "reconcile failed");
                    }
                }
                message = session.next_message() => {
                    match message? {
                        None => return Ok(()),
                        Some(bybit_rs::bybit::private_ws::PrivateMessage::Position(push)) => {
                            for dto in &push.data {
                                self.ingest_observation(dto, ChangeSource::Ws, &mut last_seen).await;
                            }
                        }
                        Some(bybit_rs::bybit::private_ws::PrivateMessage::Execution(_)) => {
                            // Trigger-only stream: closes are confirmed
                            // through the closed-pnl REST, opens through the
                            // position stream's own snapshots.
                            debug!(account = %self.account.exchange_account, "execution push observed");
                        }
                    }
                }
            }
        }
    }

    /// Map → dedupe → ingest one observation; on a close detection, run the
    /// close flow. Errors are swallowed into logs by design: one bad message
    /// must not end the session, and the reconcile heals the mirror.
    fn ingest_observation<'a>(
        &'a self,
        dto: &'a bybit_rs::bybit::dto::PositionDto,
        source: ChangeSource,
        last_seen: &'a mut HashMap<String, PositionSnapshot>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let snapshot = match crate::infrastructure::bybit_ws::map_position(
                dto,
                self.account.exchange_account,
            ) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    warn!(account = %self.account.exchange_account, symbol = %dto.symbol, %error, "position payload rejected");
                    return;
                }
            };
            if let Some(previous) = last_seen.get(&snapshot.symbol)
                && crate::domain::position::is_duplicate(previous, &snapshot)
            {
                debug!(symbol = %snapshot.symbol, seq = ?snapshot.seq, "duplicate position push skipped");
                return;
            }
            match self
                .ingestor
                .ingest(&snapshot, &self.identity, source)
                .await
            {
                Ok(IngestOutcome::ClosedDetected {
                    symbol,
                    instance_id,
                    order_link_id,
                    opened_at_ms,
                }) => {
                    self.close_once(&symbol, instance_id, order_link_id, opened_at_ms, source)
                        .await;
                }
                Ok(outcome @ (IngestOutcome::Changed { .. } | IngestOutcome::NoChange)) => {
                    debug!(symbol = %snapshot.symbol, ?outcome, "position observation ingested");
                }
                Err(error) => {
                    // Database backpressure: leave the session running; the
                    // offset/beat logic upstream handles retries, and the
                    // reconcile heals this symbol.
                    error!(account = %self.account.exchange_account, symbol = %snapshot.symbol, %error, "mirror ingest failed");
                }
            }
            last_seen.insert(snapshot.symbol.clone(), snapshot);
        })
    }

    /// The periodic safety net: a fresh `/v5/position/list` feeds the same
    /// ingest pipeline, and every open mirror symbol the list does NOT show
    /// is closed from the settled ledger — a close missed while the WS was
    /// down cannot escape.
    async fn reconcile_once(
        &self,
        last_seen: &mut HashMap<String, PositionSnapshot>,
    ) -> Result<(), WatchError> {
        let open = self
            .repo
            .open_positions(self.account.exchange_account)
            .await
            .map_err(|error| WatchError::Rest(error.to_string()))?;
        let listed = self.rest.position_list().await?;
        let mut listed_symbols = std::collections::HashSet::new();
        for dto in &listed {
            listed_symbols.insert(dto.symbol.clone());
            self.ingest_observation(dto, ChangeSource::Reconcile, last_seen)
                .await;
        }
        for mirror in open {
            if listed_symbols.contains(&mirror.symbol) {
                continue;
            }
            // Absent from the exchange = flat. Synthesize the flat
            // observation through the same pipeline so the close flow runs.
            let flat = PositionSnapshot {
                exchange_account: self.account.exchange_account,
                symbol: mirror.symbol.clone(),
                side: None,
                size: Some(rust_decimal::Decimal::ZERO),
                avg_price: None,
                stop_loss: None,
                take_profit: None,
                leverage: None,
                position_status: None,
                unrealised_pnl: None,
                position_value: None,
                occurred_at_ms: unix_now_ms(),
                seq: None,
            };
            match self
                .ingestor
                .ingest(&flat, &self.identity, ChangeSource::Reconcile)
                .await
            {
                Ok(IngestOutcome::ClosedDetected {
                    symbol,
                    instance_id,
                    order_link_id,
                    opened_at_ms,
                }) => {
                    self.close_once(
                        &symbol,
                        instance_id,
                        order_link_id,
                        opened_at_ms,
                        ChangeSource::Reconcile,
                    )
                    .await;
                }
                Ok(_) => {}
                Err(error) => {
                    error!(account = %self.account.exchange_account, symbol = %mirror.symbol, %error, "reconcile close ingest failed");
                }
            }
        }
        Ok(())
    }

    /// Close one position instance: settled values from the closed-pnl
    /// ledger (bounded settle retries), then `mark_closed` in one
    /// transaction. When the ledger answers nothing within the budget, the
    /// close still happens — with mirror-derived totals flagged `fallback`.
    async fn close_once(
        &self,
        symbol: &str,
        instance_id: uuid::Uuid,
        order_link_id: Option<String>,
        opened_at_ms: Option<i64>,
        source: ChangeSource,
    ) {
        let from_ms = opened_at_ms.unwrap_or_default();
        let side = self
            .repo
            .current_mirror(self.account.exchange_account, symbol)
            .await
            .ok()
            .flatten()
            .and_then(|mirror| mirror.side)
            .unwrap_or(Side::Buy);
        let mut attempts = 0;
        loop {
            attempts += 1;
            match self.rest.closed_pnl(symbol, from_ms).await {
                Ok(records) if !records.is_empty() => {
                    let parsed: Vec<ClosedPnlRecord> = records
                        .iter()
                        .filter_map(|record| map_closed_record(record, symbol))
                        .collect();
                    if parsed.is_empty() {
                        warn!(account = %self.account.exchange_account, symbol, "closed-pnl records unparsable; falling back to mirror totals");
                        break;
                    }
                    let totals = aggregate_closed(&parsed, side);
                    let marked = self.repo.mark_closed(CloseWrite {
                        account: self.account.exchange_account,
                        symbol,
                        instance_id,
                        order_link_id: order_link_id.as_deref(),
                        identity: &self.identity,
                        totals: &totals,
                        fallback: false,
                        source,
                        closed_at_ms: last_record_time(&records),
                    });
                    match marked.await {
                        Ok(event_id) => {
                            info!(account = %self.account.exchange_account, symbol, %event_id, "position closed with settled ledger values")
                        }
                        Err(error) => {
                            error!(account = %self.account.exchange_account, symbol, %error, "mark_closed failed; reconcile will retry the close")
                        }
                    }
                    return;
                }
                Ok(_) => {
                    if attempts >= CLOSE_SETTLE_ATTEMPTS {
                        info!(account = %self.account.exchange_account, symbol, attempts, "closed-pnl ledger has no records yet; using mirror totals");
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(self.config.close_settle_retry_ms))
                        .await;
                }
                Err(error) => {
                    error!(account = %self.account.exchange_account, symbol, %error, "closed-pnl fetch failed; using mirror totals");
                    break;
                }
            }
        }
        // Fallback: totals from the mirror itself, flagged in the event row.
        let mirror = self
            .repo
            .current_mirror(self.account.exchange_account, symbol)
            .await
            .ok()
            .flatten();
        let (quantity, avg_entry, last_side) = mirror
            .as_ref()
            .map(|mirror| {
                (
                    mirror.size.unwrap_or(rust_decimal::Decimal::ZERO),
                    mirror.avg_price,
                    mirror.side.unwrap_or(side),
                )
            })
            .unwrap_or((rust_decimal::Decimal::ZERO, None, side));
        let zero = rust_decimal::Decimal::ZERO;
        let totals = ClosedTotals {
            quantity,
            closed_pnl_usd: zero,
            closed_fee_usd: zero,
            avg_entry_price: avg_entry.unwrap_or(zero),
            avg_close_price: avg_entry.unwrap_or(zero),
            side: match last_side {
                Side::Buy => "Buy",
                Side::Sell => "Sell",
            },
        };
        let marked = self.repo.mark_closed(CloseWrite {
            account: self.account.exchange_account,
            symbol,
            instance_id,
            order_link_id: order_link_id.as_deref(),
            identity: &self.identity,
            totals: &totals,
            fallback: true,
            source,
            closed_at_ms: unix_now_ms(),
        });
        match marked.await {
            Ok(event_id) => {
                warn!(account = %self.account.exchange_account, symbol, %event_id, "position closed with fallback totals (ledger unavailable)")
            }
            Err(error) => {
                error!(account = %self.account.exchange_account, symbol, %error, "fallback mark_closed failed; reconcile will retry")
            }
        }
    }
}

fn last_record_time(records: &[bybit_rs::bybit::dto::ClosedPnlDto]) -> i64 {
    records
        .iter()
        .filter_map(|record| record.updated_time.parse::<i64>().ok())
        .max()
        .unwrap_or_else(unix_now_ms)
}

fn map_closed_record(
    record: &bybit_rs::bybit::dto::ClosedPnlDto,
    symbol: &str,
) -> Option<ClosedPnlRecord> {
    Some(ClosedPnlRecord {
        order_link_id: record.order_link_id.clone().unwrap_or_default(),
        closed_size: exact(&record.qty, symbol)?,
        closed_pnl: exact(&record.closed_pnl, symbol)?,
        open_fee: exact(&record.open_fee, symbol)?,
        close_fee: exact(&record.close_fee, symbol)?,
        cum_entry_value: exact(&record.cum_entry_value, symbol)?,
        cum_exit_value: exact(&record.cum_exit_value, symbol)?,
        updated_at_ms: record.updated_time.parse().ok()?,
    })
}

fn exact(value: &str, symbol: &str) -> Option<rust_decimal::Decimal> {
    // Negative values are valid (a losing close, a fee charged in the
    // position's currency) — only exact parsing gates the record.
    match rust_decimal::Decimal::from_str_exact(value) {
        Ok(value) => Some(value),
        _ => {
            warn!(symbol, value, "closed-pnl decimal unparsable");
            None
        }
    }
}

fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::fake_repo::FakePositionRepository;
    use crate::infrastructure::postgres::accounts::ActiveAccount;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uuid::Uuid;

    fn account() -> ActiveAccount {
        ActiveAccount {
            exchange_account: Uuid::parse_str("b3c1d2a4-0000-4000-8000-000000000001").unwrap(),
            user_id: "user-9f2b3c".to_owned(),
            testnet: true,
        }
    }

    fn config() -> WatcherConfig {
        WatcherConfig {
            reconcile_secs: 30,
            reconnect_backoff_min_ms: 500,
            reconnect_backoff_max_ms: 30_000,
            close_settle_retry_ms: 1,
        }
    }

    fn position_dto(
        symbol: &str,
        side: &str,
        size: &str,
        seq: i64,
    ) -> bybit_rs::bybit::dto::PositionDto {
        bybit_rs::bybit::dto::PositionDto {
            symbol: symbol.to_owned(),
            side: side.to_owned(),
            size: size.to_owned(),
            avg_price: String::new(),
            stop_loss: String::new(),
            take_profit: String::new(),
            leverage: String::new(),
            unrealised_pnl: String::new(),
            position_value: String::new(),
            position_status: None,
            seq,
            updated_time: "1788948000400".to_owned(),
            open_time: None,
        }
    }

    fn closed_dto(qty: &str, pnl: &str, at: &str) -> bybit_rs::bybit::dto::ClosedPnlDto {
        bybit_rs::bybit::dto::ClosedPnlDto {
            order_link_id: Some("op-1".to_owned()),
            symbol: "BTCUSDT".to_owned(),
            qty: qty.to_owned(),
            closed_pnl: pnl.to_owned(),
            avg_entry_price: "100".to_owned(),
            avg_exit_price: "110".to_owned(),
            open_fee: "0.01".to_owned(),
            close_fee: "0.01".to_owned(),
            cum_entry_value: "10".to_owned(),
            cum_exit_value: "11".to_owned(),
            fill_count: Some(1),
            updated_time: at.to_owned(),
        }
    }

    /// Scripted WS session(s): the connector hands out a fresh scripted
    /// session per connect.
    struct FakeConnector {
        sessions: Mutex<Vec<FakeSession>>,
    }

    #[async_trait]
    impl WsConnector for FakeConnector {
        type Session = FakeSession;
        async fn connect(&self) -> Result<FakeSession, WatchError> {
            self.sessions
                .lock()
                .unwrap()
                .pop()
                .ok_or_else(|| WatchError::Ws("no scripted session".to_owned()))
        }
    }

    #[derive(Default)]
    struct FakeSession {
        messages:
            Mutex<Vec<Result<Option<bybit_rs::bybit::private_ws::PrivateMessage>, WatchError>>>,
    }

    #[async_trait]
    impl WsSession for FakeSession {
        async fn next_message(
            &mut self,
        ) -> Result<Option<bybit_rs::bybit::private_ws::PrivateMessage>, WatchError> {
            // Queue semantics: scripted messages replay in push order.
            let mut messages = self.messages.lock().unwrap();
            if messages.is_empty() {
                Ok(None)
            } else {
                messages.remove(0)
            }
        }
    }

    struct FakeRest {
        positions: Mutex<Result<Vec<bybit_rs::bybit::dto::PositionDto>, WatchError>>,
        closed_pnl: Mutex<Vec<Result<Vec<bybit_rs::bybit::dto::ClosedPnlDto>, WatchError>>>,
        closed_pnl_calls: AtomicUsize,
    }

    impl Default for FakeRest {
        fn default() -> Self {
            Self {
                positions: Mutex::new(Ok(Vec::new())),
                closed_pnl: Mutex::new(Vec::new()),
                closed_pnl_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl RestSource for FakeRest {
        async fn position_list(
            &self,
        ) -> Result<Vec<bybit_rs::bybit::dto::PositionDto>, WatchError> {
            let guard = self.positions.lock().unwrap();
            guard.clone()
        }
        async fn closed_pnl(
            &self,
            _symbol: &str,
            _from_ms: i64,
        ) -> Result<Vec<bybit_rs::bybit::dto::ClosedPnlDto>, WatchError> {
            self.closed_pnl_calls.fetch_add(1, Ordering::Relaxed);
            let mut guard = self.closed_pnl.lock().unwrap();
            if guard.is_empty() {
                return Ok(Vec::new());
            }
            // Scripted answers replay from the back (Vec used as a queue).
            let last = guard.len() - 1;
            guard.remove(last)
        }
    }

    fn push(
        data: Vec<bybit_rs::bybit::dto::PositionDto>,
    ) -> bybit_rs::bybit::private_ws::PrivateMessage {
        bybit_rs::bybit::private_ws::PrivateMessage::Position(
            bybit_rs::bybit::dto::PrivatePositionMessage {
                topic: "position".to_owned(),
                id: Some("test".to_owned()),
                creation_time: None,
                data,
            },
        )
    }

    async fn run_until_quiescent(watcher: Watcher<FakeSession, FakeRest, FakePositionRepository>) {
        // The session loop returns as soon as the scripted messages run out;
        // no ticker fires within the drain window.
        let cancel = CancellationToken::new();
        tokio::select! {
            _ = watcher.session_loop_quiet(&cancel) => {}
            _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("watcher did not quiesce"),
        }
    }

    impl<W: WsSession, R: RestSource, Repo: PositionRepository + 'static> Watcher<W, R, Repo> {
        /// Test-only drain: run one session loop without the reconcile tick
        /// firing (reconcile tests call `reconcile_once` directly).
        async fn session_loop_quiet(&self, cancel: &CancellationToken) {
            let mut session = self.connector.connect().await.expect("connect");
            let mut last_seen = HashMap::new();
            loop {
                tokio::select! {
                    () = cancel.cancelled() => return,
                    message = session.next_message() => {
                        match message.expect("session error") {
                            None => return,
                            Some(bybit_rs::bybit::private_ws::PrivateMessage::Position(push)) => {
                                for dto in &push.data {
                                    self.ingest_observation(dto, ChangeSource::Ws, &mut last_seen).await;
                                }
                            }
                            Some(_) => {}
                        }
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn duplicate_seq_push_does_not_reach_ingest() {
        let repo = Arc::new(FakePositionRepository::default());
        let connector = Arc::new(FakeConnector {
            sessions: Mutex::new(vec![FakeSession {
                messages: Mutex::new(vec![
                    Ok(Some(push(vec![position_dto("BTCUSDT", "Buy", "0.015", 7)]))),
                    Ok(Some(push(vec![position_dto("BTCUSDT", "Buy", "0.015", 7)]))),
                ]),
            }]),
        });
        let rest = Arc::new(FakeRest::default());
        let watcher = Watcher::new(
            connector,
            rest,
            repo.clone(),
            account(),
            "bybit-linear",
            config(),
        );

        run_until_quiescent(watcher).await;

        // One push opened the mirror; the duplicate was filtered before the
        // ingest (Review Focus 1) — the event log holds exactly one opened
        // audit row and no updated rows.
        let events = repo.recorded_events();
        assert_eq!(events.len(), 1);
        assert_eq!(format!("{:?}", events[0].kind), "Opened");
        // An exchange-observed open is announced on the updated topic (the
        // adapter's created event never existed here) — pending for the
        // outbox, unlike a created-triggered open's audit row.
        assert!(events[0].pending);
    }

    #[tokio::test]
    async fn bad_payload_is_skipped_without_ingest() {
        let repo = Arc::new(FakePositionRepository::default());
        let connector = Arc::new(FakeConnector {
            sessions: Mutex::new(vec![FakeSession {
                messages: Mutex::new(vec![Ok(Some(push(vec![position_dto(
                    "BTCUSDT",
                    "Buy",
                    "not-a-number",
                    7,
                )])))]),
            }]),
        });
        let rest = Arc::new(FakeRest::default());
        let watcher = Watcher::new(
            connector,
            rest,
            repo.clone(),
            account(),
            "bybit-linear",
            config(),
        );

        run_until_quiescent(watcher).await;

        assert!(repo.recorded_events().is_empty());
        assert!(repo.row(account().exchange_account, "BTCUSDT").is_none());
    }

    #[tokio::test]
    async fn ws_close_triggers_closed_pnl_fetch_and_mark_closed() {
        let repo = Arc::new(FakePositionRepository::default());
        // Open first, then a flat push on the same symbol.
        let connector = Arc::new(FakeConnector {
            sessions: Mutex::new(vec![FakeSession {
                messages: Mutex::new(vec![
                    Ok(Some(push(vec![position_dto("BTCUSDT", "Buy", "0.015", 7)]))),
                    Ok(Some(push(vec![position_dto("BTCUSDT", "", "0", 8)]))),
                ]),
            }]),
        });
        let rest = FakeRest::default();
        *rest.closed_pnl.lock().unwrap() =
            vec![Ok(vec![closed_dto("0.015", "12.3456", "1788948100900")])];
        let rest = Arc::new(rest);
        let watcher = Watcher::new(
            connector,
            rest.clone(),
            repo.clone(),
            account(),
            "bybit-linear",
            config(),
        );

        run_until_quiescent(watcher).await;

        assert_eq!(rest.closed_pnl_calls.load(Ordering::Relaxed), 1);
        let row = repo
            .row(account().exchange_account, "BTCUSDT")
            .expect("mirror row");
        assert!(row.closed_at_ms.is_some());
        let closed = repo
            .recorded_events()
            .into_iter()
            .find(|event| format!("{:?}", event.kind) == "Closed")
            .expect("closed event row");
        assert_eq!(closed.topic, "exchange.position.v2.closed");
        assert_eq!(closed.source, ChangeSource::Ws);
        assert!(closed.pending, "the closed event goes through the outbox");
        assert_eq!(closed.payload_side(), "Buy");
    }

    #[tokio::test]
    async fn reconcile_closes_missed_position_from_closed_pnl_rest() {
        let repo = Arc::new(FakePositionRepository::default());
        let connector = Arc::new(FakeConnector {
            sessions: Mutex::new(vec![FakeSession::default()]),
        });
        // Open the mirror (created-event style), then reconcile with an
        // empty exchange list: the position closed while the WS was down.
        let mut created_snapshot = crate::domain::position::PositionSnapshot {
            exchange_account: account().exchange_account,
            symbol: "BTCUSDT".to_owned(),
            side: Some(Side::Buy),
            size: Some(rust_decimal::Decimal::from_str_exact("0.015").unwrap()),
            avg_price: None,
            stop_loss: None,
            take_profit: None,
            leverage: None,
            position_status: None,
            unrealised_pnl: None,
            position_value: None,
            occurred_at_ms: 1_788_948_000_400,
            seq: None,
        };
        created_snapshot.size = Some(rust_decimal::Decimal::from_str_exact("0.015").unwrap());
        repo.open_mirror(crate::domain::repo::MirrorOpen {
            snapshot: &created_snapshot,
            instance_id: Uuid::new_v4(),
            order_link_id: Some("op-1"),
            identity: &crate::domain::repo::MirrorIdentity {
                user_id: "user-9f2b3c".to_owned(),
                channel: "bybit-linear".to_owned(),
            },
            source: ChangeSource::CreatedEvent,
        })
        .await
        .unwrap();

        let rest = FakeRest::default();
        *rest.positions.lock().unwrap() = Ok(Vec::new());
        *rest.closed_pnl.lock().unwrap() = vec![
            Ok(vec![closed_dto("0.010", "5.0", "1788948090000")]),
            Ok(vec![closed_dto("0.005", "3.0", "1788948100900")]),
        ];
        let rest = Arc::new(rest);
        let watcher = Watcher::new(
            connector,
            rest.clone(),
            repo.clone(),
            account(),
            "bybit-linear",
            config(),
        );

        watcher
            .reconcile_once(&mut HashMap::new())
            .await
            .expect("reconcile");

        assert!(rest.closed_pnl_calls.load(Ordering::Relaxed) >= 1);
        let row = repo
            .row(account().exchange_account, "BTCUSDT")
            .expect("mirror row");
        assert!(row.closed_at_ms.is_some());
        let closed = repo
            .recorded_events()
            .into_iter()
            .find(|event| format!("{:?}", event.kind) == "Closed")
            .expect("closed event row");
        assert!(!closed.fallback, "settled values, not fallback");
    }

    #[tokio::test]
    async fn closed_pnl_empty_then_filled_settles_within_retries() {
        let repo = Arc::new(FakePositionRepository::default());
        let connector = Arc::new(FakeConnector {
            sessions: Mutex::new(vec![FakeSession {
                messages: Mutex::new(vec![
                    Ok(Some(push(vec![position_dto("BTCUSDT", "Buy", "0.015", 7)]))),
                    Ok(Some(push(vec![position_dto("BTCUSDT", "", "0", 8)]))),
                ]),
            }]),
        });
        let rest = FakeRest::default();
        // First fetch answers empty (ledger lag), the second has the record.
        *rest.closed_pnl.lock().unwrap() = vec![
            Ok(vec![closed_dto("0.015", "12.3456", "1788948100900")]),
            Ok(Vec::new()),
        ];
        *rest.closed_pnl.lock().unwrap() = vec![
            Ok(vec![closed_dto("0.015", "12.3456", "1788948100900")]),
            Ok(Vec::new()),
        ];
        let rest = Arc::new(rest);
        let watcher = Watcher::new(
            connector,
            rest.clone(),
            repo.clone(),
            account(),
            "bybit-linear",
            config(),
        );

        run_until_quiescent(watcher).await;

        assert_eq!(rest.closed_pnl_calls.load(Ordering::Relaxed), 2);
        let row = repo
            .row(account().exchange_account, "BTCUSDT")
            .expect("mirror row");
        assert!(row.closed_at_ms.is_some());
    }

    #[tokio::test]
    async fn rest_error_falls_back_and_watcher_survives() {
        let repo = Arc::new(FakePositionRepository::default());
        let connector = Arc::new(FakeConnector {
            sessions: Mutex::new(vec![FakeSession {
                messages: Mutex::new(vec![
                    Ok(Some(push(vec![position_dto("BTCUSDT", "Buy", "0.015", 7)]))),
                    Ok(Some(push(vec![position_dto("BTCUSDT", "", "0", 8)]))),
                ]),
            }]),
        });
        let rest = FakeRest::default();
        // Mutex mutates through a shared borrow: no `mut` binding needed.
        *rest.closed_pnl.lock().unwrap() =
            vec![Err(WatchError::Rest("network unreachable".to_owned()))];
        let rest = Arc::new(rest);
        let watcher = Watcher::new(
            connector,
            rest.clone(),
            repo.clone(),
            account(),
            "bybit-linear",
            config(),
        );

        run_until_quiescent(watcher).await;

        // Fallback close still flattens the mirror and emits the flagged row.
        let row = repo
            .row(account().exchange_account, "BTCUSDT")
            .expect("mirror row");
        assert!(row.closed_at_ms.is_some());
        let closed = repo
            .recorded_events()
            .into_iter()
            .find(|event| format!("{:?}", event.kind) == "Closed")
            .expect("closed event row");
        assert!(closed.fallback);
    }

    #[tokio::test]
    async fn manual_exchange_observed_position_gets_a_mirror_row() {
        let repo = Arc::new(FakePositionRepository::default());
        let connector = Arc::new(FakeConnector {
            sessions: Mutex::new(vec![FakeSession {
                messages: Mutex::new(vec![Ok(Some(push(vec![position_dto(
                    "ETHUSDT", "Sell", "2.5", 11,
                )])))]),
            }]),
        });
        let rest = Arc::new(FakeRest::default());
        let watcher = Watcher::new(
            connector,
            rest,
            repo.clone(),
            account(),
            "bybit-linear",
            config(),
        );

        run_until_quiescent(watcher).await;

        // No created event ever arrived: the WS snapshot opened the row as
        // exchange-observed (no link id), and tracking is complete anyway.
        let row = repo
            .row(account().exchange_account, "ETHUSDT")
            .expect("mirror row");
        assert_eq!(row.order_link_id, None);
        assert_eq!(row.side, Some(Side::Sell));
        assert!(row.is_open());
    }

    #[tokio::test]
    async fn protection_change_fires_only_when_tp_sl_move() {
        let repo = Arc::new(FakePositionRepository::default());
        // Created-event open carries the ordered SL/TP.
        let created_snapshot = crate::domain::position::PositionSnapshot {
            exchange_account: account().exchange_account,
            symbol: "BTCUSDT".to_owned(),
            side: Some(Side::Buy),
            size: Some(rust_decimal::Decimal::from_str_exact("0.015").unwrap()),
            avg_price: None,
            stop_loss: Some(rust_decimal::Decimal::from_str_exact("62800.0").unwrap()),
            take_profit: Some(rust_decimal::Decimal::from_str_exact("65000.0").unwrap()),
            leverage: None,
            position_status: None,
            unrealised_pnl: None,
            position_value: None,
            occurred_at_ms: 1_788_948_000_400,
            seq: None,
        };
        repo.open_mirror(crate::domain::repo::MirrorOpen {
            snapshot: &created_snapshot,
            instance_id: Uuid::new_v4(),
            order_link_id: Some("op-1"),
            identity: &crate::domain::repo::MirrorIdentity {
                user_id: "user-9f2b3c".to_owned(),
                channel: "bybit-linear".to_owned(),
            },
            source: ChangeSource::CreatedEvent,
        })
        .await
        .unwrap();

        // First WS push carries the SAME protection plus the fill's average
        // price; the second moves the stop. A push that leaves SL/TP alone
        // must not fire protection_changed — nor wipe the mirror's values.
        let mut first = position_dto("BTCUSDT", "Buy", "0.015", 7);
        first.avg_price = "63250.5".to_owned();
        first.stop_loss = "62800.0".to_owned();
        first.take_profit = "65000.0".to_owned();
        first.leverage = "5".to_owned();
        let mut second = position_dto("BTCUSDT", "Buy", "0.015", 8);
        second.avg_price = "63250.5".to_owned();
        second.stop_loss = "62900.0".to_owned();
        second.take_profit = "65000.0".to_owned();
        second.leverage = "5".to_owned();
        let connector = Arc::new(FakeConnector {
            sessions: Mutex::new(vec![FakeSession {
                messages: Mutex::new(vec![
                    Ok(Some(push(vec![first]))),
                    Ok(Some(push(vec![second]))),
                ]),
            }]),
        });
        let rest = Arc::new(FakeRest::default());
        let watcher = Watcher::new(
            connector,
            rest,
            repo.clone(),
            account(),
            "bybit-linear",
            config(),
        );

        run_until_quiescent(watcher).await;

        let events = repo.recorded_events();
        // The opened audit row (never pending) plus exactly one event per
        // real change — no protection_changed from the unchanged first push.
        let kinds: Vec<String> = events
            .iter()
            .map(|event| format!("{:?}", event.kind))
            .collect();
        assert_eq!(kinds, vec!["Opened", "SizeChanged", "ProtectionChanged"]);
        assert!(!events[0].pending, "the created-event open is audit-only");
        assert_eq!(
            events[2].payload["changed"]["stop_loss"]["to"],
            serde_json::json!("62900.0")
        );
        // The mirror kept the created event's protection after the first
        // push and tracked the moved stop after the second.
        let row = repo
            .row(account().exchange_account, "BTCUSDT")
            .expect("mirror row");
        assert_eq!(
            row.stop_loss,
            Some(rust_decimal::Decimal::from_str_exact("62900.0").unwrap())
        );
        assert_eq!(
            row.take_profit,
            Some(rust_decimal::Decimal::from_str_exact("65000.0").unwrap())
        );
    }
}
