// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Redis-backed cache database for the system.
//!
//! # Architecture
//!
//! Uses two Redis connections with distinct roles:
//! - **READ** (`self.con`): synchronous queries (`keys`, `read`, `load_all`),
//!   owned by the main struct.
//! - **WRITE**: owned by a background task on `get_runtime()`, receives
//!   commands via an unbounded `tokio::sync::mpsc` channel.
//!
//! All write operations (`insert`, `update`, `delete`, `flush`) are routed
//! through the command channel so they execute on the WRITE connection. This
//! avoids cross-runtime I/O issues since the WRITE connection is always
//! created on the Nautilus runtime.
//!
//! Synchronous callers (`close`, `flushdb_sync`) use `std::sync::mpsc` reply
//! channels to block until the background task confirms completion. When
//! called from the Nautilus runtime itself, `block_in_place` is used
//! automatically to avoid stalling the worker thread.

use std::{
    collections::VecDeque,
    fmt::{Debug, Write as _},
    ops::ControlFlow,
    pin::Pin,
    sync::mpsc::{self, SyncSender},
    time::Duration,
};

use ahash::AHashMap;
use anyhow::Context;
use bytes::Bytes;
use nautilus_common::{
    cache::{
        CacheConfig,
        database::{CacheDatabaseAdapter, CacheDatabaseFactory, CacheMap},
    },
    enums::SerializationEncoding,
    live::get_runtime,
    logging::{log_task_awaiting, log_task_started, log_task_stopped},
    signal::Signal,
};
use nautilus_core::{UUID4, UnixNanos, correctness::check_slice_not_empty};
use nautilus_cryptography::providers::install_cryptographic_provider;
use nautilus_model::{
    accounts::AccountAny,
    data::{
        Bar, CustomData, DataType, FundingRateUpdate, HasTsInit, InstrumentClose, QuoteTick,
        TradeTick,
    },
    events::{
        AccountState, OrderEventAny, OrderFilled, OrderSnapshot,
        position::snapshot::PositionSnapshot,
    },
    identifiers::{
        AccountId, ActorId, ClientId, ClientOrderId, InstrumentId, PositionId, StrategyId,
        TraderId, VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny, SyntheticInstrument},
    orderbook::OrderBook,
    orders::{Order, OrderAny},
    position::Position,
    types::{Currency, Money},
};
use redis::{AsyncCommands, Pipeline, aio::ConnectionManager};
use serde::{Deserialize, Serialize};
use ustr::Ustr;

use super::{REDIS_DELIMITER, REDIS_FLUSHDB, get_index_key};
use crate::redis::{RedisConnectionConfig, create_redis_connection, queries::DatabaseQueries};

// Task and connection names
const CACHE_READ: &str = "cache-read";
const CACHE_WRITE: &str = "cache-write";
const CACHE_PROCESS: &str = "cache-process";

// Error constants
const FAILED_TX_CHANNEL: &str = "Failed to send to channel";

// Collection keys
const INDEX: &str = "index";
const GENERAL: &str = "general";
const CURRENCIES: &str = "currencies";
const INSTRUMENTS: &str = "instruments";
const INSTRUMENT_CLOSES: &str = "instrument_closes";
const SYNTHETICS: &str = "synthetics";
const ACCOUNTS: &str = "accounts";
const ORDERS: &str = "orders";
const POSITIONS: &str = "positions";
const ACTORS: &str = "actors";
const STRATEGIES: &str = "strategies";
const SNAPSHOTS: &str = "snapshots";
const HEALTH: &str = "health";
const CUSTOM: &str = "custom";

// Index keys
const INDEX_ORDER_IDS: &str = "index:order_ids";
const INDEX_ORDER_POSITION: &str = "index:order_position";
const INDEX_ORDER_CLIENT: &str = "index:order_client";
const INDEX_ORDERS: &str = "index:orders";
const INDEX_ORDERS_OPEN: &str = "index:orders_open";
const INDEX_ORDERS_CLOSED: &str = "index:orders_closed";
const INDEX_ORDERS_EMULATED: &str = "index:orders_emulated";
const INDEX_ORDERS_INFLIGHT: &str = "index:orders_inflight";
const INDEX_POSITIONS: &str = "index:positions";
const INDEX_POSITIONS_OPEN: &str = "index:positions_open";
const INDEX_POSITIONS_CLOSED: &str = "index:positions_closed";

/// Appends an order event and updates its index sets only when the order event list exists.
///
/// `KEYS[1]` is the order event list and `KEYS[2..]` are order index sets. `ARGV[1]` is the
/// serialized event and `ARGV[2]` is the client order ID. `ARGV[i + 1]` is `1` to add the client
/// order ID to `KEYS[i]` or `0` to remove it. Returns 1 when appended, or 0 when the list is missing.
const APPEND_ORDER_EVENT_SCRIPT: &str = "
if redis.call('EXISTS', KEYS[1]) == 0 then
    return 0
end
redis.call('RPUSH', KEYS[1], ARGV[1])
for i = 2, #KEYS do
    if ARGV[i + 1] == '1' then
        redis.call('SADD', KEYS[i], ARGV[2])
    else
        redis.call('SREM', KEYS[i], ARGV[2])
    end
end
return 1
";

/// Configuration for a Redis-backed cache database.
///
/// Redis 6.2 or higher is required for correct operation.
#[cfg_attr(
    feature = "python",
    expect(
        clippy::unsafe_derive_deserialize,
        reason = "config deserializes plain fields; unsafe methods come from generated PyO3 integration"
    )
)]
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.infrastructure", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.infrastructure")
)]
pub struct RedisCacheConfig {
    /// The Redis host address. If `None`, `127.0.0.1` is used.
    pub host: Option<String>,
    /// The Redis port. If `None`, `6379` is used.
    pub port: Option<u16>,
    /// The Redis account username.
    pub username: Option<String>,
    /// The Redis account password.
    pub password: Option<String>,
    /// If Redis should use an SSL-enabled connection.
    pub ssl: bool,
    /// The timeout (in seconds) to wait for a new connection.
    pub connection_timeout: u16,
    /// The timeout (in seconds) to wait for a response.
    pub response_timeout: u16,
    /// The number of retry attempts with exponential backoff for connection attempts.
    pub number_of_retries: usize,
    /// The base value for exponential backoff calculation.
    pub exponent_base: u64,
    /// The maximum delay between retry attempts (in seconds).
    pub max_delay: u64,
    /// The multiplication factor for retry delay calculation.
    pub factor: u64,
}

impl Debug for RedisCacheConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted = self.password.as_ref().map(|_| "***");
        f.debug_struct(stringify!(RedisCacheConfig))
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &redacted)
            .field("ssl", &self.ssl)
            .field("connection_timeout", &self.connection_timeout)
            .field("response_timeout", &self.response_timeout)
            .field("number_of_retries", &self.number_of_retries)
            .field("exponent_base", &self.exponent_base)
            .field("max_delay", &self.max_delay)
            .field("factor", &self.factor)
            .finish()
    }
}

impl Default for RedisCacheConfig {
    fn default() -> Self {
        Self {
            host: None,
            port: None,
            username: None,
            password: None,
            ssl: false,
            connection_timeout: 20,
            response_timeout: 20,
            number_of_retries: 100,
            exponent_base: 2,
            max_delay: 1000,
            factor: 2,
        }
    }
}

impl RedisConnectionConfig for RedisCacheConfig {
    fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    fn port(&self) -> Option<u16> {
        self.port
    }

    fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    fn ssl(&self) -> bool {
        self.ssl
    }

    fn connection_timeout(&self) -> u16 {
        self.connection_timeout
    }

    fn response_timeout(&self) -> u16 {
        self.response_timeout
    }

    fn number_of_retries(&self) -> usize {
        self.number_of_retries
    }

    fn exponent_base(&self) -> u64 {
        self.exponent_base
    }

    fn max_delay(&self) -> u64 {
        self.max_delay
    }

    fn factor(&self) -> u64 {
        self.factor
    }
}

/// A type of database operation.
#[derive(Clone, Debug)]
pub enum DatabaseOperation {
    Insert,
    Update,
    UpdateOrder,
    /// Appends an order event with index membership taken from the post-event order.
    AppendOrderEvent(OrderIndexUpdate),
    ReplaceList,
    Delete,
    Flush(SyncSender<()>),
    Close,
}

/// Order index membership derived from an order state.
///
/// Carries the order facts that determine the order index sets, so the writer can update the
/// indexes in the same atomic pipeline as the event append without replaying the stored history.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderIndexUpdate {
    client_order_id: ClientOrderId,
    has_venue_order_id: bool,
    is_inflight: bool,
    lifecycle: OrderLifecycle,
    is_emulated: bool,
}

/// Whether an order belongs in the open or closed order index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OrderLifecycle {
    Open,
    Closed,
    Neither,
}

impl OrderIndexUpdate {
    fn from_order(order: &OrderAny) -> Self {
        let is_closed = order.is_closed();
        let lifecycle = if order.is_open() {
            OrderLifecycle::Open
        } else if is_closed {
            OrderLifecycle::Closed
        } else {
            OrderLifecycle::Neither
        };
        Self {
            client_order_id: order.client_order_id(),
            has_venue_order_id: order.venue_order_id().is_some(),
            is_inflight: order.is_inflight(),
            lifecycle,
            is_emulated: order.emulation_trigger().is_some() && !is_closed,
        }
    }

    /// Returns the index set changes in application order as `(index, is_member)` pairs.
    fn set_changes(self) -> impl Iterator<Item = (&'static str, bool)> {
        let lifecycle: &[(&'static str, bool)] = match self.lifecycle {
            OrderLifecycle::Open => &[(INDEX_ORDERS_CLOSED, false), (INDEX_ORDERS_OPEN, true)],
            OrderLifecycle::Closed => &[(INDEX_ORDERS_OPEN, false), (INDEX_ORDERS_CLOSED, true)],
            OrderLifecycle::Neither => &[],
        };

        std::iter::once((INDEX_ORDERS, true))
            .chain(self.has_venue_order_id.then_some((INDEX_ORDER_IDS, true)))
            .chain(std::iter::once((INDEX_ORDERS_INFLIGHT, self.is_inflight)))
            .chain(lifecycle.iter().copied())
            .chain(std::iter::once((INDEX_ORDERS_EMULATED, self.is_emulated)))
    }
}

/// Represents a database command to be performed which may be executed in a task.
#[derive(Clone, Debug)]
pub struct DatabaseCommand {
    /// The database operation type.
    pub op_type: DatabaseOperation,
    /// The primary key for the operation.
    pub key: Option<String>,
    /// The data payload for the operation.
    pub payload: Option<Vec<Bytes>>,
}

impl DatabaseCommand {
    /// Creates a new [`DatabaseCommand`] instance.
    #[must_use]
    pub const fn new(op_type: DatabaseOperation, key: String, payload: Option<Vec<Bytes>>) -> Self {
        Self {
            op_type,
            key: Some(key),
            payload,
        }
    }

    /// Initialize a `Close` database command, this is meant to close the database cache channel.
    #[must_use]
    pub const fn close() -> Self {
        Self {
            op_type: DatabaseOperation::Close,
            key: None,
            payload: None,
        }
    }
}

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.infrastructure")
)]
pub struct RedisCacheDatabase {
    pub con: ConnectionManager,
    pub trader_id: TraderId,
    pub trader_key: String,
    pub encoding: SerializationEncoding,
    pub bulk_read_batch_size: Option<usize>,
    tx: tokio::sync::mpsc::UnboundedSender<DatabaseCommand>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Debug for RedisCacheDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RedisCacheDatabase))
            .field("trader_id", &self.trader_id)
            .field("encoding", &self.encoding)
            .finish_non_exhaustive()
    }
}

impl RedisCacheDatabase {
    /// Creates a new [`RedisCacheDatabase`] instance for the given `trader_id`, `instance_id`, and `config`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The database configuration is missing in `config`.
    /// - Establishing the Redis connection fails.
    /// - The command processing task cannot be spawned.
    pub async fn new(
        trader_id: TraderId,
        instance_id: UUID4,
        config: CacheConfig,
        database: RedisCacheConfig,
    ) -> anyhow::Result<Self> {
        install_cryptographic_provider();

        let con = create_redis_connection(CACHE_READ, &database).await?;

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<DatabaseCommand>();
        let trader_key = get_trader_key(trader_id, instance_id, &config);
        let trader_key_clone = trader_key.clone();
        let encoding = config.encoding;
        let bulk_read_batch_size = config.bulk_read_batch_size;

        let handle = get_runtime().spawn(async move {
            if let Err(e) =
                process_commands(rx, trader_key_clone, config.clone(), database.clone()).await
            {
                log::error!("Error in task '{CACHE_PROCESS}': {e}");
            }
        });

        Ok(Self {
            con,
            trader_id,
            trader_key,
            encoding,
            bulk_read_batch_size,
            tx,
            handle: Some(handle),
        })
    }

    #[must_use]
    pub const fn get_encoding(&self) -> SerializationEncoding {
        self.encoding
    }

    #[must_use]
    pub fn get_trader_key(&self) -> &str {
        &self.trader_key
    }

    pub fn close(&mut self) {
        log::debug!("Closing");

        let Some(handle) = self.handle.take() else {
            log::debug!("Already closed");
            return;
        };

        if let Err(e) = self.tx.send(DatabaseCommand::close()) {
            log::debug!("Error sending close command: {e:?}");
        }

        log_task_awaiting(CACHE_PROCESS);

        let (tx, rx) = mpsc::sync_channel(1);

        get_runtime().spawn(async move {
            if let Err(e) = handle.await {
                log::error!("Error awaiting task '{CACHE_PROCESS}': {e:?}");
            }
            let _ = tx.send(());
        });
        let _ = blocking_recv(&rx);

        log::debug!("Closed");
    }

    pub async fn flushdb(&mut self) {
        if let Err(e) = redis::cmd(REDIS_FLUSHDB)
            .query_async::<()>(&mut self.con)
            .await
        {
            log::error!("Failed to flush database: {e:?}");
        }
    }

    /// Sends a flush command through the background task channel and blocks
    /// until it completes. Safe to call from any runtime context.
    ///
    /// # Errors
    ///
    /// Returns an error if the command channel is closed or the reply is lost.
    pub fn flushdb_sync(&self) -> anyhow::Result<()> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        let cmd = DatabaseCommand {
            op_type: DatabaseOperation::Flush(reply_tx),
            key: None,
            payload: None,
        };
        self.tx
            .send(cmd)
            .map_err(|e| anyhow::anyhow!("{FAILED_TX_CHANNEL}: {e}"))?;
        blocking_recv(&reply_rx).map_err(|e| anyhow::anyhow!("Failed to flush database: {e}"))?;
        Ok(())
    }

    /// Retrieves all keys matching the given `pattern` from Redis for this trader.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying Redis scan operation fails.
    pub async fn keys(&mut self, pattern: &str) -> anyhow::Result<Vec<String>> {
        let pattern = format!("{}{REDIS_DELIMITER}{pattern}", self.trader_key);
        DatabaseQueries::scan_keys(&mut self.con, pattern).await
    }

    /// Reads the value(s) associated with `key` for this trader from Redis.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying Redis read operation fails.
    pub async fn read(&mut self, key: &str) -> anyhow::Result<Vec<Bytes>> {
        DatabaseQueries::read(&self.con, &self.trader_key, key).await
    }

    /// Reads multiple values using bulk operations for efficiency.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying Redis read operation fails.
    pub async fn read_bulk(&mut self, keys: &[String]) -> anyhow::Result<Vec<Option<Bytes>>> {
        match self.bulk_read_batch_size {
            Some(batch_size) => {
                DatabaseQueries::read_bulk_batched(&self.con, keys, batch_size).await
            }
            None => DatabaseQueries::read_bulk(&self.con, keys).await,
        }
    }

    /// Loads custom data from Redis matching the given `data_type` (blocking).
    ///
    /// Spawns the async query on the global Nautilus runtime and blocks until
    /// the result arrives via a channel. Safe from any thread context (Python,
    /// test runtimes, plain threads).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or the reply channel is closed.
    pub fn load_custom_data(&self, data_type: &DataType) -> anyhow::Result<Vec<CustomData>> {
        let con = self.con.clone();
        let trader_key = self.trader_key.clone();
        let data_type = data_type.clone();
        let (tx, rx) = mpsc::channel();

        get_runtime().spawn(async move {
            let result = DatabaseQueries::load_custom_data(&con, &trader_key, &data_type).await;
            if let Err(e) = tx.send(result) {
                log::error!("Failed to send custom data result for '{data_type}': {e:?}");
            }
        });

        blocking_recv(&rx).map_err(|e| anyhow::anyhow!("load_custom_data channel closed: {e}"))?
    }

    /// Sends an insert command for `key` with optional `payload` to Redis via the background task.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn insert(&self, key: String, payload: Option<Vec<Bytes>>) -> anyhow::Result<()> {
        let op = DatabaseCommand::new(DatabaseOperation::Insert, key, payload);
        match self.tx.send(op) {
            Ok(()) => Ok(()),
            Err(e) => anyhow::bail!("{FAILED_TX_CHANNEL}: {e}"),
        }
    }

    /// Stores custom data in Redis (key format: `custom:<ts_init_020>:<uuid>`, value: full JSON).
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the insert command cannot be sent.
    pub fn add_custom_data(&self, data: &CustomData) -> anyhow::Result<()> {
        let json_bytes = serde_json::to_vec(data)
            .map_err(|e| anyhow::anyhow!("CustomData serialization failed: {e}"))?;
        let ts_init = data.ts_init().as_u64();
        let key = format!(
            "{CUSTOM}{REDIS_DELIMITER}{:020}{REDIS_DELIMITER}{}",
            ts_init,
            UUID4::new()
        );
        self.insert(key, Some(vec![Bytes::from(json_bytes)]))
    }

    /// Sends an update command for `key` with optional `payload` to Redis via the background task.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn update(&mut self, key: String, payload: Option<Vec<Bytes>>) -> anyhow::Result<()> {
        let op = DatabaseCommand::new(DatabaseOperation::Update, key, payload);
        match self.tx.send(op) {
            Ok(()) => Ok(()),
            Err(e) => anyhow::bail!("{FAILED_TX_CHANNEL}: {e}"),
        }
    }

    /// Sends a delete command for `key` with optional `payload` to Redis via the background task.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn delete(&mut self, key: String, payload: Option<Vec<Bytes>>) -> anyhow::Result<()> {
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key, payload);
        match self.tx.send(op) {
            Ok(()) => Ok(()),
            Err(e) => anyhow::bail!("{FAILED_TX_CHANNEL}: {e}"),
        }
    }

    /// Delete the given order from the database with full index cleanup.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn delete_order(&self, client_order_id: &ClientOrderId) -> anyhow::Result<()> {
        let order_id_bytes = Bytes::from(client_order_id.to_string());

        // Delete the order itself
        let key = format!("{ORDERS}{REDIS_DELIMITER}{client_order_id}");
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key, None);
        self.tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send delete order command: {e}"))?;

        // Delete from all order indexes
        let index_keys = [
            INDEX_ORDER_IDS,
            INDEX_ORDERS,
            INDEX_ORDERS_OPEN,
            INDEX_ORDERS_CLOSED,
            INDEX_ORDERS_EMULATED,
            INDEX_ORDERS_INFLIGHT,
        ];

        for index_key in &index_keys {
            let key = (*index_key).to_string();
            let payload = vec![order_id_bytes.clone()];
            let op = DatabaseCommand::new(DatabaseOperation::Delete, key, Some(payload));
            self.tx
                .send(op)
                .map_err(|e| anyhow::anyhow!("Failed to send delete order index command: {e}"))?;
        }

        // Delete from hash indexes
        let hash_indexes = [INDEX_ORDER_POSITION, INDEX_ORDER_CLIENT];
        for index_key in &hash_indexes {
            let key = (*index_key).to_string();
            let payload = vec![order_id_bytes.clone()];
            let op = DatabaseCommand::new(DatabaseOperation::Delete, key, Some(payload));
            self.tx.send(op).map_err(|e| {
                anyhow::anyhow!("Failed to send delete order hash index command: {e}")
            })?;
        }

        Ok(())
    }

    /// Delete the given position from the database with full index cleanup.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn delete_position(&self, position_id: &PositionId) -> anyhow::Result<()> {
        let position_id_bytes = Bytes::from(position_id.to_string());

        // Delete the position itself
        let key = format!("{POSITIONS}{REDIS_DELIMITER}{position_id}");
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key, None);
        self.tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send delete position command: {e}"))?;

        // Delete from all position indexes
        let index_keys = [
            INDEX_POSITIONS,
            INDEX_POSITIONS_OPEN,
            INDEX_POSITIONS_CLOSED,
        ];

        for index_key in &index_keys {
            let key = (*index_key).to_string();
            let payload = vec![position_id_bytes.clone()];
            let op = DatabaseCommand::new(DatabaseOperation::Delete, key, Some(payload));
            self.tx.send(op).map_err(|e| {
                anyhow::anyhow!("Failed to send delete position index command: {e}")
            })?;
        }

        Ok(())
    }

    /// Delete the given account event from the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn delete_account_event(
        &self,
        account_id: &AccountId,
        event_id: &str,
    ) -> anyhow::Result<()> {
        log::warn!(
            "Deleting account events currently a no-op (pending redesign), {account_id}: {event_id}"
        );
        Ok(())
    }
}

/// Receives a reply, handing off the worker first when called from the Nautilus runtime.
///
/// The check is whether a runtime handle is current, not whether this thread is a runtime worker.
/// Both branches block the caller, so a caller that must not block, such as a live node driven by
/// a host event loop, cannot use these paths at all and is rejected before it reaches them.
fn blocking_recv<T>(rx: &mpsc::Receiver<T>) -> Result<T, mpsc::RecvError> {
    let on_nautilus_runtime =
        tokio::runtime::Handle::try_current().is_ok_and(|h| h.id() == get_runtime().handle().id());

    if on_nautilus_runtime {
        tokio::task::block_in_place(|| rx.recv())
    } else {
        rx.recv()
    }
}

async fn process_commands(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<DatabaseCommand>,
    trader_key: String,
    config: CacheConfig,
    database: RedisCacheConfig,
) -> anyhow::Result<()> {
    log_task_started(CACHE_PROCESS);

    let mut con = create_redis_connection(CACHE_WRITE, &database).await?;

    // Buffering
    let mut buffer: VecDeque<DatabaseCommand> = VecDeque::new();
    let buffer_interval = Duration::from_millis(config.buffer_interval_ms.unwrap_or(0) as u64);

    // A sleep used to trigger periodic flushing of the buffer.
    // When `buffer_interval` is zero we skip using the timer and flush immediately
    // after every message.
    let flush_timer = tokio::time::sleep(buffer_interval);
    tokio::pin!(flush_timer);

    // Continue to receive and handle messages until channel is hung up
    loop {
        tokio::select! {
            maybe_cmd = rx.recv() => {
                let result = handle_command(
                    maybe_cmd,
                    &mut buffer,
                    buffer_interval,
                    &mut con,
                    &trader_key,
                    config.encoding,
                ).await;

                if result.is_break() {
                    break;
                }
            }
            () = &mut flush_timer, if !buffer_interval.is_zero() => {
                flush_buffer(
                    &mut buffer,
                    &mut con,
                    &trader_key,
                    config.encoding,
                    &mut flush_timer,
                    buffer_interval,
                ).await;
            }
        }
    }

    // Drain any remaining messages
    if !buffer.is_empty() {
        drain_buffer(&mut con, &trader_key, config.encoding, &mut buffer).await;
    }

    log_task_stopped(CACHE_PROCESS);
    Ok(())
}

async fn handle_command(
    maybe_cmd: Option<DatabaseCommand>,
    buffer: &mut VecDeque<DatabaseCommand>,
    buffer_interval: Duration,
    con: &mut ConnectionManager,
    trader_key: &str,
    encoding: SerializationEncoding,
) -> ControlFlow<()> {
    let Some(cmd) = maybe_cmd else {
        log::debug!("Command channel closed");
        return ControlFlow::Break(());
    };

    log::trace!("Received {cmd:?}");

    match cmd.op_type {
        DatabaseOperation::Close => {
            if !buffer.is_empty() {
                drain_buffer(con, trader_key, encoding, buffer).await;
            }
            return ControlFlow::Break(());
        }
        DatabaseOperation::Flush(reply_tx) => {
            if !buffer.is_empty() {
                drain_buffer(con, trader_key, encoding, buffer).await;
            }

            if let Err(e) = redis::cmd(REDIS_FLUSHDB).query_async::<()>(con).await {
                log::error!("Failed to flush database: {e:?}");
            }
            let _ = reply_tx.send(());
            return ControlFlow::Continue(());
        }
        _ => {}
    }

    buffer.push_back(cmd);

    if buffer_interval.is_zero() {
        drain_buffer(con, trader_key, encoding, buffer).await;
    }

    ControlFlow::Continue(())
}

async fn flush_buffer(
    buffer: &mut VecDeque<DatabaseCommand>,
    con: &mut ConnectionManager,
    trader_key: &str,
    encoding: SerializationEncoding,
    flush_timer: &mut Pin<&mut tokio::time::Sleep>,
    buffer_interval: Duration,
) {
    if !buffer.is_empty() {
        drain_buffer(con, trader_key, encoding, buffer).await;
    }
    flush_timer
        .as_mut()
        .reset(tokio::time::Instant::now() + buffer_interval);
}

async fn drain_buffer(
    conn: &mut ConnectionManager,
    trader_key: &str,
    encoding: SerializationEncoding,
    buffer: &mut VecDeque<DatabaseCommand>,
) {
    let mut pipe = redis::pipe();
    pipe.atomic();
    let mut has_pending_ops = false;
    let mut order_appends = Vec::new();

    for msg in buffer.drain(..) {
        let Some(key) = msg.key else {
            log::error!("Null key found for message: {msg:?}");
            continue;
        };
        let collection = match get_collection_key(&key) {
            Ok(collection) => collection,
            Err(e) => {
                log::error!("{e}");
                continue; // Continue to next message
            }
        };

        let key = format!("{trader_key}{REDIS_DELIMITER}{key}");

        match msg.op_type {
            DatabaseOperation::Insert => {
                if let Some(payload) = msg.payload {
                    log::debug!("Processing INSERT for collection: {collection}, key: {key}");
                    if let Err(e) = insert(&mut pipe, collection, &key, &payload) {
                        log::error!("{e}");
                    } else {
                        has_pending_ops = true;
                    }
                } else {
                    log::error!("Null `payload` for `insert`");
                }
            }
            DatabaseOperation::Update => {
                if let Some(payload) = msg.payload {
                    log::debug!("Processing UPDATE for collection: {collection}, key: {key}");
                    if let Err(e) = update(&mut pipe, collection, &key, &payload) {
                        log::error!("{e}");
                    } else {
                        has_pending_ops = true;
                    }
                } else {
                    log::error!("Null `payload` for `update`");
                }
            }
            DatabaseOperation::UpdateOrder => {
                flush_pending_pipeline(conn, &mut pipe, &mut has_pending_ops, &mut order_appends)
                    .await;

                if let Some(payload) = msg.payload {
                    log::debug!("Processing UPDATE_ORDER for key: {key}");
                    if let Err(e) =
                        update_order_event_log(conn, trader_key, encoding, &key, &payload).await
                    {
                        log::error!("{e}");
                    }
                } else {
                    log::error!("Null `payload` for `update_order`");
                }
            }
            DatabaseOperation::AppendOrderEvent(update) => {
                let payload = msg.payload.as_deref();
                has_pending_ops |= queue_order_event(
                    &mut pipe,
                    trader_key,
                    key,
                    payload,
                    update,
                    &mut order_appends,
                );
            }
            DatabaseOperation::ReplaceList => {
                if let Some(payload) = msg.payload {
                    log::debug!("Processing REPLACE_LIST for key: {key}");
                    if let Err(e) = replace_list_operation(&mut pipe, collection, &key, &payload) {
                        log::error!("{e}");
                    } else {
                        has_pending_ops = true;
                    }
                } else {
                    log::error!("Null `payload` for `replace_list`");
                }
            }
            DatabaseOperation::Delete => {
                log::debug!(
                    "Processing DELETE for collection: {}, key: {}, payload: {:?}",
                    collection,
                    key,
                    msg.payload.as_ref().map(std::vec::Vec::len)
                );
                // `payload` can be `None` for a delete operation
                if let Err(e) = delete(&mut pipe, collection, &key, msg.payload) {
                    log::error!("{e}");
                } else {
                    has_pending_ops = true;
                }
            }
            DatabaseOperation::Close => panic!("Close command should not be drained"),
            DatabaseOperation::Flush(_) => panic!("Flush command should not be drained"),
        }
    }

    flush_pending_pipeline(conn, &mut pipe, &mut has_pending_ops, &mut order_appends).await;
}

/// Executes the pending atomic pipeline.
///
/// `order_appends` holds the reply index and key of each order event append in the pipeline, so a
/// skipped append for a missing order event list is logged after the transaction completes.
async fn flush_pending_pipeline(
    conn: &mut ConnectionManager,
    pipe: &mut Pipeline,
    has_pending_ops: &mut bool,
    order_appends: &mut Vec<(usize, String)>,
) {
    if !*has_pending_ops {
        return;
    }

    if order_appends.is_empty() {
        if let Err(e) = pipe.query_async::<()>(conn).await {
            log::error!("{e}");
        }
    } else {
        match pipe.query_async::<Vec<redis::Value>>(conn).await {
            Ok(replies) => log_order_append_replies(&replies, order_appends),
            Err(e) => log::error!("{e}"),
        }
        order_appends.clear();
    }

    *pipe = redis::pipe();
    pipe.atomic();
    *has_pending_ops = false;
}

fn log_order_append_replies(replies: &[redis::Value], order_appends: &[(usize, String)]) {
    for (reply_index, key) in order_appends {
        match replies.get(*reply_index) {
            Some(redis::Value::Int(1)) => {}
            Some(redis::Value::Int(0)) => {
                log::warn!("Cannot update order in Redis, no existing state at {key}");
            }
            reply => log::error!("Unexpected reply appending order event at {key}: {reply:?}"),
        }
    }
}

/// Queues an order event append, recording its reply index in `order_appends`.
///
/// Returns whether the append was queued.
fn queue_order_event(
    pipe: &mut Pipeline,
    trader_key: &str,
    key: String,
    payload: Option<&[Bytes]>,
    update: OrderIndexUpdate,
    order_appends: &mut Vec<(usize, String)>,
) -> bool {
    let Some(payload) = payload else {
        log::error!("Null `payload` for `append_order_event`");
        return false;
    };

    log::debug!("Processing APPEND_ORDER_EVENT for key: {key}");
    let reply_index = pipe.len();
    if let Err(e) = append_order_event(pipe, trader_key, &key, payload, update) {
        log::error!("{e}");
        return false;
    }

    order_appends.push((reply_index, key));
    true
}

/// Queues an order event append and its index updates as one conditional script in `pipe`.
///
/// The script appends and updates indexes only when the order event list exists, matching the
/// skip of [`update_order_event_log`] for an order with no existing state.
fn append_order_event(
    pipe: &mut Pipeline,
    trader_key: &str,
    key: &str,
    value: &[Bytes],
    update: OrderIndexUpdate,
) -> anyhow::Result<()> {
    check_slice_not_empty(value, stringify!(value))?;

    let order_id = update.client_order_id.to_string();
    let changes: Vec<(&str, bool)> = update.set_changes().collect();

    pipe.cmd("EVAL")
        .arg(APPEND_ORDER_EVENT_SCRIPT)
        .arg(1 + changes.len())
        .arg(key);
    for (index, _) in &changes {
        pipe.arg(full_redis_key(trader_key, index));
    }
    pipe.arg(value[0].as_ref()).arg(order_id.as_bytes());
    for (_, is_member) in &changes {
        pipe.arg(if *is_member { "1" } else { "0" });
    }

    Ok(())
}

async fn update_order_event_log(
    conn: &mut ConnectionManager,
    trader_key: &str,
    encoding: SerializationEncoding,
    key: &str,
    value: &[Bytes],
) -> anyhow::Result<()> {
    check_slice_not_empty(value, stringify!(value))?;

    let result: Vec<Bytes> = conn.lrange(key, 0, -1).await?;
    if result.is_empty() {
        log::warn!("Cannot update order in Redis, no existing state at {key}");
        return Ok(());
    }

    let mut append_pipe = redis::pipe();
    append_pipe.atomic();
    update_list(&mut append_pipe, key, value[0].as_ref());
    append_pipe.query_async::<()>(conn).await?;

    let mut events: Vec<OrderEventAny> = result
        .iter()
        .map(|payload| DatabaseQueries::deserialize_payload(encoding, payload))
        .collect::<anyhow::Result<_>>()
        .with_context(|| {
            format!(
                "Order event append succeeded for {key}, but index replay failed decoding history"
            )
        })?;
    let event: OrderEventAny = DatabaseQueries::deserialize_payload(encoding, value[0].as_ref())
        .with_context(|| {
            format!(
                "Order event append succeeded for {key}, but index replay failed decoding appended event"
            )
        })?;
    events.push(event);
    let order = OrderAny::from_events(events).with_context(|| {
        format!("Order event append succeeded for {key}, but index replay failed rebuilding order")
    })?;

    let mut pipe = redis::pipe();
    pipe.atomic();
    update_order_indexes(&mut pipe, trader_key, &order);
    pipe.query_async::<()>(conn).await?;

    Ok(())
}

fn insert(pipe: &mut Pipeline, collection: &str, key: &str, value: &[Bytes]) -> anyhow::Result<()> {
    check_slice_not_empty(value, stringify!(value))?;

    match collection {
        INDEX => insert_index(pipe, key, value),
        GENERAL | CURRENCIES | INSTRUMENTS | INSTRUMENT_CLOSES | SYNTHETICS | ACTORS
        | STRATEGIES | HEALTH | CUSTOM => {
            insert_string(pipe, key, value[0].as_ref());
            Ok(())
        }
        ACCOUNTS | ORDERS | POSITIONS | SNAPSHOTS => {
            insert_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        _ => anyhow::bail!("Unsupported operation: `insert` for collection '{collection}'"),
    }
}

fn insert_index(pipe: &mut Pipeline, key: &str, value: &[Bytes]) -> anyhow::Result<()> {
    let index_key = get_index_key(key)?;
    match index_key {
        INDEX_ORDER_IDS
        | INDEX_ORDERS
        | INDEX_ORDERS_OPEN
        | INDEX_ORDERS_CLOSED
        | INDEX_ORDERS_EMULATED
        | INDEX_ORDERS_INFLIGHT
        | INDEX_POSITIONS
        | INDEX_POSITIONS_OPEN
        | INDEX_POSITIONS_CLOSED => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDER_POSITION => {
            insert_hset(pipe, key, value[0].as_ref(), value[1].as_ref());
            Ok(())
        }
        INDEX_ORDER_CLIENT => {
            if !value.len().is_multiple_of(2) {
                anyhow::bail!(
                    "Invalid hash index payload for '{index_key}': expected field-value pairs"
                );
            }

            let entries = value
                .as_chunks::<2>()
                .0
                .iter()
                .map(|entry| (entry[0].as_ref(), entry[1].as_ref()))
                .collect::<Vec<(&[u8], &[u8])>>();
            pipe.hset_multiple(key, &entries);
            Ok(())
        }
        _ => anyhow::bail!("Index unknown '{index_key}' on insert"),
    }
}

fn insert_string(pipe: &mut Pipeline, key: &str, value: &[u8]) {
    pipe.set(key, value);
}

fn insert_set(pipe: &mut Pipeline, key: &str, value: &[u8]) {
    pipe.sadd(key, value);
}

fn insert_hset(pipe: &mut Pipeline, key: &str, name: &[u8], value: &[u8]) {
    pipe.hset(key, name, value);
}

fn insert_list(pipe: &mut Pipeline, key: &str, value: &[u8]) {
    pipe.rpush(key, value);
}

fn replace_list(pipe: &mut Pipeline, key: &str, value: &[u8]) {
    pipe.del(key);
    pipe.rpush(key, value);
}

fn replace_list_operation(
    pipe: &mut Pipeline,
    collection: &str,
    key: &str,
    value: &[Bytes],
) -> anyhow::Result<()> {
    check_slice_not_empty(value, stringify!(value))?;

    match collection {
        ACCOUNTS | ORDERS | POSITIONS => {
            replace_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        _ => anyhow::bail!("Unsupported operation: `replace_list` for collection '{collection}'"),
    }
}

fn update(pipe: &mut Pipeline, collection: &str, key: &str, value: &[Bytes]) -> anyhow::Result<()> {
    check_slice_not_empty(value, stringify!(value))?;

    match collection {
        ACCOUNTS | ORDERS | POSITIONS => {
            update_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        _ => anyhow::bail!("Unsupported operation: `update` for collection '{collection}'"),
    }
}

fn update_list(pipe: &mut Pipeline, key: &str, value: &[u8]) {
    pipe.rpush_exists(key, value);
}

fn delete(
    pipe: &mut Pipeline,
    collection: &str,
    key: &str,
    value: Option<Vec<Bytes>>,
) -> anyhow::Result<()> {
    log::debug!(
        "delete: collection={}, key={}, has_payload={}",
        collection,
        key,
        value.is_some()
    );

    match collection {
        INDEX => delete_from_index(pipe, key, value),
        ORDERS | POSITIONS | ACCOUNTS | ACTORS | STRATEGIES => {
            delete_string(pipe, key);
            Ok(())
        }
        _ => anyhow::bail!("Unsupported operation: `delete` for collection '{collection}'"),
    }
}

fn delete_from_index(
    pipe: &mut Pipeline,
    key: &str,
    value: Option<Vec<Bytes>>,
) -> anyhow::Result<()> {
    let value = value.ok_or_else(|| anyhow::anyhow!("Empty `payload` for `delete` '{key}'"))?;
    let index_key = get_index_key(key)?;

    match index_key {
        INDEX_ORDER_IDS
        | INDEX_ORDERS
        | INDEX_ORDERS_OPEN
        | INDEX_ORDERS_CLOSED
        | INDEX_ORDERS_EMULATED
        | INDEX_ORDERS_INFLIGHT
        | INDEX_POSITIONS
        | INDEX_POSITIONS_OPEN
        | INDEX_POSITIONS_CLOSED => {
            remove_from_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDER_POSITION | INDEX_ORDER_CLIENT => {
            remove_from_hash(pipe, key, value[0].as_ref());
            Ok(())
        }
        _ => anyhow::bail!("Unsupported index operation: remove from '{index_key}'"),
    }
}

fn remove_from_set(pipe: &mut Pipeline, key: &str, member: &[u8]) {
    pipe.srem(key, member);
}

fn remove_from_hash(pipe: &mut Pipeline, key: &str, field: &[u8]) {
    pipe.hdel(key, field);
}

fn delete_string(pipe: &mut Pipeline, key: &str) {
    pipe.del(key);
}

fn full_redis_key(trader_key: &str, key: &str) -> String {
    format!("{trader_key}{REDIS_DELIMITER}{key}")
}

fn update_order_indexes(pipe: &mut Pipeline, trader_key: &str, order: &OrderAny) {
    let update = OrderIndexUpdate::from_order(order);
    let order_id_bytes = update.client_order_id.to_string();

    for (index, is_member) in update.set_changes() {
        let key = full_redis_key(trader_key, index);
        if is_member {
            insert_set(pipe, &key, order_id_bytes.as_bytes());
        } else {
            remove_from_set(pipe, &key, order_id_bytes.as_bytes());
        }
    }
}

fn format_timestamp(timestamp: UnixNanos) -> String {
    format!("{:.9}", timestamp.to_datetime_utc())
}

fn get_trader_key(trader_id: TraderId, instance_id: UUID4, config: &CacheConfig) -> String {
    let mut key = String::new();

    if config.use_trader_prefix {
        key.push_str("trader-");
    }

    key.push_str(trader_id.as_str());

    if config.use_instance_id {
        key.push(REDIS_DELIMITER);
        write!(key, "{instance_id}").expect("writing to String cannot fail");
    }

    key
}

fn get_collection_key(key: &str) -> anyhow::Result<&str> {
    key.split_once(REDIS_DELIMITER)
        .map(|(collection, _)| collection)
        .ok_or_else(|| {
            anyhow::anyhow!("Invalid `key`, missing a '{REDIS_DELIMITER}' delimiter, was {key}")
        })
}

#[derive(Debug)]
pub struct RedisCacheDatabaseAdapter {
    pub database: RedisCacheDatabase,
}

impl RedisCacheDatabaseAdapter {
    fn encoding(&self) -> SerializationEncoding {
        self.database.get_encoding()
    }

    fn send_command(
        &self,
        op_type: DatabaseOperation,
        key: String,
        payload: Option<Vec<Bytes>>,
    ) -> anyhow::Result<()> {
        let op = DatabaseCommand::new(op_type, key, payload);
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("{FAILED_TX_CHANNEL}: {e}"))
    }

    fn append_list(&self, key: String, payload: Bytes) -> anyhow::Result<()> {
        self.send_command(DatabaseOperation::Update, key, Some(vec![payload]))
    }

    fn serialize_account_event(&self, account: &AccountAny) -> anyhow::Result<Bytes> {
        let event: AccountState = account.last_event().ok_or_else(|| {
            anyhow::anyhow!("Cannot persist account with no events: {}", account.id())
        })?;
        let payload = DatabaseQueries::serialize_payload(self.encoding(), &event)?;
        Ok(Bytes::from(payload))
    }

    fn serialize_order_event(&self, order_event: &OrderEventAny) -> anyhow::Result<Bytes> {
        let payload = DatabaseQueries::serialize_payload(self.encoding(), order_event)?;
        Ok(Bytes::from(payload))
    }

    fn serialize_position_event(&self, position: &Position) -> anyhow::Result<Bytes> {
        let event: OrderFilled = position.last_event().ok_or_else(|| {
            anyhow::anyhow!("Cannot persist position with no events: {}", position.id)
        })?;
        let payload = DatabaseQueries::serialize_payload(self.encoding(), &event)?;
        Ok(Bytes::from(payload))
    }

    fn load_state(&self, key: String) -> anyhow::Result<AHashMap<String, Bytes>> {
        let mut con = self.database.con.clone();
        let trader_key = self.database.trader_key.clone();
        let encoding = self.encoding();
        let (tx, rx) = mpsc::channel();

        get_runtime().spawn(async move {
            let result = async {
                let full_key = format!("{trader_key}{REDIS_DELIMITER}{key}");
                let value: Option<Bytes> = con.get(&full_key).await?;
                let Some(value) = value else {
                    return Ok(AHashMap::new());
                };

                DatabaseQueries::deserialize_payload(encoding, &value)
            }
            .await;

            if let Err(e) = tx.send(result) {
                log::error!("Failed to send state load result for '{key}': {e:?}");
            }
        });

        blocking_recv(&rx).map_err(|e| anyhow::anyhow!("load_state channel closed: {e}"))?
    }

    fn update_state(&self, key: String, state: &AHashMap<String, Bytes>) -> anyhow::Result<()> {
        let payload = DatabaseQueries::serialize_payload(self.encoding(), state)?;
        self.database.insert(key, Some(vec![Bytes::from(payload)]))
    }

    fn replace_list(&self, key: String, payload: Bytes) -> anyhow::Result<()> {
        self.send_command(DatabaseOperation::ReplaceList, key, Some(vec![payload]))
    }
}

#[async_trait::async_trait]
impl CacheDatabaseFactory for RedisCacheConfig {
    async fn create(
        &self,
        trader_id: TraderId,
        instance_id: UUID4,
        config: CacheConfig,
    ) -> anyhow::Result<Box<dyn CacheDatabaseAdapter>> {
        let database =
            RedisCacheDatabase::new(trader_id, instance_id, config, self.clone()).await?;
        Ok(Box::new(RedisCacheDatabaseAdapter { database }))
    }
}

#[async_trait::async_trait]
impl CacheDatabaseAdapter for RedisCacheDatabaseAdapter {
    fn close(&mut self) -> anyhow::Result<()> {
        self.database.close();
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        self.database.flushdb_sync()
    }

    async fn load_all(&self) -> anyhow::Result<CacheMap> {
        log::debug!("Loading all data");

        let (
            currencies,
            instruments,
            instrument_closes,
            synthetics,
            accounts,
            orders,
            positions,
            greeks,
            yield_curves,
        ) = tokio::try_join!(
            self.load_currencies(),
            self.load_instruments(),
            self.load_instrument_closes(),
            self.load_synthetics(),
            self.load_accounts(),
            self.load_orders(),
            self.load_positions(),
            self.load_greeks(),
            self.load_yield_curves()
        )
        .map_err(|e| anyhow::anyhow!("Error loading cache data: {e}"))?;

        Ok(CacheMap {
            currencies,
            instruments,
            instrument_closes,
            synthetics,
            accounts,
            orders,
            positions,
            greeks,
            yield_curves,
        })
    }

    fn load(&self) -> anyhow::Result<AHashMap<String, Bytes>> {
        let con = self.database.con.clone();
        let trader_key = self.database.trader_key.clone();
        let (tx, rx) = mpsc::channel();

        get_runtime().spawn(async move {
            let result = async {
                let pattern = format!("{trader_key}{REDIS_DELIMITER}{GENERAL}:*");
                let mut con_scan = con.clone();
                let keys = DatabaseQueries::scan_keys(&mut con_scan, pattern).await?;
                if keys.is_empty() {
                    return Ok(AHashMap::new());
                }

                let values = DatabaseQueries::read_bulk(&con, &keys).await?;
                let prefix = format!("{trader_key}{REDIS_DELIMITER}{GENERAL}{REDIS_DELIMITER}");
                let mut general = AHashMap::new();

                for (key, value) in keys.into_iter().zip(values) {
                    let Some(value) = value else {
                        continue;
                    };

                    if let Some(clean_key) = key.strip_prefix(&prefix) {
                        general.insert(clean_key.to_string(), value);
                    }
                }

                Ok(general)
            }
            .await;

            if let Err(e) = tx.send(result) {
                log::error!("Failed to send general load result: {e:?}");
            }
        });

        blocking_recv(&rx).map_err(|e| anyhow::anyhow!("load channel closed: {e}"))?
    }

    async fn load_currencies(&self) -> anyhow::Result<AHashMap<Ustr, Currency>> {
        DatabaseQueries::load_currencies(
            &self.database.con,
            &self.database.trader_key,
            self.encoding(),
        )
        .await
    }

    async fn load_instruments(&self) -> anyhow::Result<AHashMap<InstrumentId, InstrumentAny>> {
        DatabaseQueries::load_instruments(
            &self.database.con,
            &self.database.trader_key,
            self.encoding(),
        )
        .await
    }

    async fn load_instrument_closes(
        &self,
    ) -> anyhow::Result<AHashMap<InstrumentId, InstrumentClose>> {
        DatabaseQueries::load_instrument_closes(
            &self.database.con,
            &self.database.trader_key,
            self.encoding(),
        )
        .await
    }

    async fn load_synthetics(&self) -> anyhow::Result<AHashMap<InstrumentId, SyntheticInstrument>> {
        DatabaseQueries::load_synthetics(
            &self.database.con,
            &self.database.trader_key,
            self.encoding(),
        )
        .await
    }

    async fn load_accounts(&self) -> anyhow::Result<AHashMap<AccountId, AccountAny>> {
        DatabaseQueries::load_accounts(
            &self.database.con,
            &self.database.trader_key,
            self.encoding(),
        )
        .await
    }

    async fn load_orders(&self) -> anyhow::Result<AHashMap<ClientOrderId, OrderAny>> {
        DatabaseQueries::load_orders(
            &self.database.con,
            &self.database.trader_key,
            self.encoding(),
        )
        .await
    }

    async fn load_positions(&self) -> anyhow::Result<AHashMap<PositionId, Position>> {
        DatabaseQueries::load_positions(
            &self.database.con,
            &self.database.trader_key,
            self.encoding(),
        )
        .await
    }

    fn load_index_order_position(&self) -> anyhow::Result<AHashMap<ClientOrderId, PositionId>> {
        let con = self.database.con.clone();
        let trader_key = self.database.trader_key.clone();
        let (tx, rx) = mpsc::channel();

        get_runtime().spawn(async move {
            let result = DatabaseQueries::load_index_order_position(&con, &trader_key).await;
            if let Err(e) = tx.send(result) {
                log::error!("Failed to send load_index_order_position result: {e:?}");
            }
        });

        blocking_recv(&rx)
            .map_err(|e| anyhow::anyhow!("load_index_order_position channel closed: {e}"))?
    }

    fn load_index_order_client(&self) -> anyhow::Result<AHashMap<ClientOrderId, ClientId>> {
        let con = self.database.con.clone();
        let trader_key = self.database.trader_key.clone();
        let (tx, rx) = mpsc::channel();

        get_runtime().spawn(async move {
            let result = DatabaseQueries::load_index_order_client(&con, &trader_key).await;
            if let Err(e) = tx.send(result) {
                log::error!("Failed to send load_index_order_client result: {e:?}");
            }
        });

        blocking_recv(&rx)
            .map_err(|e| anyhow::anyhow!("load_index_order_client channel closed: {e}"))?
    }

    async fn load_currency(&self, code: &Ustr) -> anyhow::Result<Option<Currency>> {
        DatabaseQueries::load_currency(
            &self.database.con,
            &self.database.trader_key,
            code,
            self.encoding(),
        )
        .await
    }

    async fn load_instrument(
        &self,
        instrument_id: &InstrumentId,
    ) -> anyhow::Result<Option<InstrumentAny>> {
        DatabaseQueries::load_instrument(
            &self.database.con,
            &self.database.trader_key,
            instrument_id,
            self.encoding(),
        )
        .await
    }

    async fn load_synthetic(
        &self,
        instrument_id: &InstrumentId,
    ) -> anyhow::Result<Option<SyntheticInstrument>> {
        DatabaseQueries::load_synthetic(
            &self.database.con,
            &self.database.trader_key,
            instrument_id,
            self.encoding(),
        )
        .await
    }

    async fn load_account(&self, account_id: &AccountId) -> anyhow::Result<Option<AccountAny>> {
        DatabaseQueries::load_account(
            &self.database.con,
            &self.database.trader_key,
            account_id,
            self.encoding(),
        )
        .await
    }

    async fn load_order(
        &self,
        client_order_id: &ClientOrderId,
    ) -> anyhow::Result<Option<OrderAny>> {
        DatabaseQueries::load_order(
            &self.database.con,
            &self.database.trader_key,
            client_order_id,
            self.encoding(),
        )
        .await
    }

    async fn load_position(&self, position_id: &PositionId) -> anyhow::Result<Option<Position>> {
        DatabaseQueries::load_position(
            &self.database.con,
            &self.database.trader_key,
            position_id,
            self.encoding(),
        )
        .await
    }

    fn load_actor(&self, actor_id: &ActorId) -> anyhow::Result<AHashMap<String, Bytes>> {
        let key = format!("{ACTORS}{REDIS_DELIMITER}{actor_id}{REDIS_DELIMITER}state");
        self.load_state(key)
    }

    fn load_strategy(&self, strategy_id: &StrategyId) -> anyhow::Result<AHashMap<String, Bytes>> {
        let key = format!("{STRATEGIES}{REDIS_DELIMITER}{strategy_id}{REDIS_DELIMITER}state");
        self.load_state(key)
    }

    fn load_signals(&self, _name: &str) -> anyhow::Result<Vec<Signal>> {
        anyhow::bail!("Loading signals from Redis cache adapter not supported")
    }

    fn load_custom_data(&self, data_type: &DataType) -> anyhow::Result<Vec<CustomData>> {
        self.database.load_custom_data(data_type)
    }

    fn load_order_snapshot(
        &self,
        _client_order_id: &ClientOrderId,
    ) -> anyhow::Result<Option<OrderSnapshot>> {
        anyhow::bail!("Loading order snapshots from Redis cache adapter not supported")
    }

    fn load_position_snapshot(
        &self,
        _position_id: &PositionId,
    ) -> anyhow::Result<Option<PositionSnapshot>> {
        anyhow::bail!("Loading position snapshots from Redis cache adapter not supported")
    }

    fn load_quotes(&self, _instrument_id: &InstrumentId) -> anyhow::Result<Vec<QuoteTick>> {
        anyhow::bail!("Loading quote data for Redis cache adapter not supported")
    }

    fn load_trades(&self, _instrument_id: &InstrumentId) -> anyhow::Result<Vec<TradeTick>> {
        anyhow::bail!("Loading market data for Redis cache adapter not supported")
    }

    fn load_funding_rates(
        &self,
        _instrument_id: &InstrumentId,
    ) -> anyhow::Result<Vec<FundingRateUpdate>> {
        anyhow::bail!("Loading market data for Redis cache adapter not supported")
    }

    fn load_bars(&self, _instrument_id: &InstrumentId) -> anyhow::Result<Vec<Bar>> {
        anyhow::bail!("Loading market data for Redis cache adapter not supported")
    }

    fn add(&self, key: String, value: Bytes) -> anyhow::Result<()> {
        let key = format!("{GENERAL}{REDIS_DELIMITER}{key}");
        self.database.insert(key, Some(vec![value]))
    }

    fn add_currency(&self, currency: &Currency) -> anyhow::Result<()> {
        let key = format!("{CURRENCIES}{REDIS_DELIMITER}{}", currency.code);
        let payload = DatabaseQueries::serialize_payload(self.encoding(), currency)?;
        self.database.insert(key, Some(vec![Bytes::from(payload)]))
    }

    fn add_instrument(&self, instrument: &InstrumentAny) -> anyhow::Result<()> {
        let key = format!("{INSTRUMENTS}{REDIS_DELIMITER}{}", instrument.id());
        let payload = DatabaseQueries::serialize_payload(self.encoding(), instrument)?;
        self.database.insert(key, Some(vec![Bytes::from(payload)]))
    }

    fn add_instrument_close(&self, close: &InstrumentClose) -> anyhow::Result<()> {
        let key = format!(
            "{INSTRUMENT_CLOSES}{REDIS_DELIMITER}{}",
            close.instrument_id
        );
        let payload = DatabaseQueries::serialize_payload(self.encoding(), close)?;
        self.database.insert(key, Some(vec![Bytes::from(payload)]))
    }

    fn add_synthetic(&self, synthetic: &SyntheticInstrument) -> anyhow::Result<()> {
        let key = format!("{SYNTHETICS}{REDIS_DELIMITER}{}", synthetic.id);
        let payload = DatabaseQueries::serialize_payload(self.encoding(), synthetic)?;
        self.database.insert(key, Some(vec![Bytes::from(payload)]))
    }

    fn add_account(&self, account: &AccountAny) -> anyhow::Result<()> {
        let account_id = account.id();
        let key = format!("{ACCOUNTS}{REDIS_DELIMITER}{account_id}");

        let payload = self.serialize_account_event(account)?;
        self.database.insert(key, Some(vec![payload]))
    }

    fn add_order(&self, order: &OrderAny, client_id: Option<ClientId>) -> anyhow::Result<()> {
        let client_order_id = order.client_order_id();
        let key = format!("{ORDERS}{REDIS_DELIMITER}{client_order_id}");

        let event = OrderEventAny::Initialized(order.init_event().clone());
        let payload = self.serialize_order_event(&event)?;
        self.replace_list(key, payload)?;

        let order_id_bytes = Bytes::from(client_order_id.to_string());
        self.database
            .insert(INDEX_ORDERS.to_string(), Some(vec![order_id_bytes.clone()]))?;

        if order.emulation_trigger().is_some() {
            self.database.insert(
                INDEX_ORDERS_EMULATED.to_string(),
                Some(vec![order_id_bytes.clone()]),
            )?;
        }

        if let Some(client_id) = client_id {
            self.database.insert(
                INDEX_ORDER_CLIENT.to_string(),
                Some(vec![order_id_bytes, Bytes::from(client_id.to_string())]),
            )?;
        }

        Ok(())
    }

    fn add_order_snapshot(&self, snapshot: &OrderSnapshot) -> anyhow::Result<()> {
        let key = format!(
            "{SNAPSHOTS}{REDIS_DELIMITER}{ORDERS}{REDIS_DELIMITER}{}",
            snapshot.client_order_id
        );
        let payload = DatabaseQueries::serialize_payload(self.encoding(), snapshot)?;
        self.database.insert(key, Some(vec![Bytes::from(payload)]))
    }

    fn add_position(&self, position: &Position) -> anyhow::Result<()> {
        let position_id = position.id;
        let key = format!("{POSITIONS}{REDIS_DELIMITER}{position_id}");

        let payload = self.serialize_position_event(position)?;
        self.replace_list(key, payload)?;

        let position_id_bytes = Bytes::from(position_id.to_string());
        self.database.insert(
            INDEX_POSITIONS.to_string(),
            Some(vec![position_id_bytes.clone()]),
        )?;
        self.database.insert(
            INDEX_POSITIONS_OPEN.to_string(),
            Some(vec![position_id_bytes.clone()]),
        )?;
        self.send_command(
            DatabaseOperation::Delete,
            INDEX_POSITIONS_CLOSED.to_string(),
            Some(vec![position_id_bytes]),
        )?;

        Ok(())
    }

    fn add_position_snapshot(&self, snapshot: &PositionSnapshot) -> anyhow::Result<()> {
        let key = format!(
            "{SNAPSHOTS}{REDIS_DELIMITER}{POSITIONS}{REDIS_DELIMITER}{}",
            snapshot.position_id
        );
        let payload = DatabaseQueries::serialize_payload(self.encoding(), snapshot)?;
        self.database.insert(key, Some(vec![Bytes::from(payload)]))
    }

    fn add_order_book(&self, _order_book: &OrderBook) -> anyhow::Result<()> {
        anyhow::bail!("Saving market data for Redis cache adapter not supported")
    }

    fn add_signal(&self, _signal: &Signal) -> anyhow::Result<()> {
        anyhow::bail!("Saving signals for Redis cache adapter not supported")
    }

    fn add_custom_data(&self, data: &CustomData) -> anyhow::Result<()> {
        self.database.add_custom_data(data)
    }

    fn add_quote(&self, _quote: &QuoteTick) -> anyhow::Result<()> {
        anyhow::bail!("Saving market data for Redis cache adapter not supported")
    }

    fn add_trade(&self, _trade: &TradeTick) -> anyhow::Result<()> {
        anyhow::bail!("Saving market data for Redis cache adapter not supported")
    }

    fn add_funding_rate(&self, _funding_rate: &FundingRateUpdate) -> anyhow::Result<()> {
        anyhow::bail!("Saving market data for Redis cache adapter not supported")
    }

    fn add_bar(&self, _bar: &Bar) -> anyhow::Result<()> {
        anyhow::bail!("Saving market data for Redis cache adapter not supported")
    }

    fn delete_actor(&self, actor_id: &ActorId) -> anyhow::Result<()> {
        let key = format!("{ACTORS}{REDIS_DELIMITER}{actor_id}{REDIS_DELIMITER}state");
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key, None);
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("{FAILED_TX_CHANNEL}: {e}"))
    }

    fn delete_strategy(&self, component_id: &StrategyId) -> anyhow::Result<()> {
        let key = format!("{STRATEGIES}{REDIS_DELIMITER}{component_id}{REDIS_DELIMITER}state");
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key, None);
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("{FAILED_TX_CHANNEL}: {e}"))
    }

    fn delete_order(&self, client_order_id: &ClientOrderId) -> anyhow::Result<()> {
        self.database.delete_order(client_order_id)
    }

    fn delete_position(&self, position_id: &PositionId) -> anyhow::Result<()> {
        self.database.delete_position(position_id)
    }

    fn delete_account_event(&self, account_id: &AccountId, event_id: &str) -> anyhow::Result<()> {
        self.database.delete_account_event(account_id, event_id)
    }

    fn index_venue_order_id(
        &self,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
    ) -> anyhow::Result<()> {
        self.database.insert(
            INDEX_ORDER_IDS.to_string(),
            Some(vec![Bytes::from(client_order_id.to_string())]),
        )?;
        log::debug!("Indexed {client_order_id:?} -> {venue_order_id:?}");
        Ok(())
    }

    fn index_order_position(
        &self,
        client_order_id: ClientOrderId,
        position_id: PositionId,
    ) -> anyhow::Result<()> {
        self.database.insert(
            INDEX_ORDER_POSITION.to_string(),
            Some(vec![
                Bytes::from(client_order_id.to_string()),
                Bytes::from(position_id.to_string()),
            ]),
        )
    }

    fn index_order_clients(&self, claims: &[(ClientOrderId, ClientId)]) -> anyhow::Result<()> {
        if claims.is_empty() {
            return Ok(());
        }

        let mut payload = Vec::with_capacity(claims.len() * 2);
        for (client_order_id, client_id) in claims {
            payload.push(Bytes::from(client_order_id.to_string()));
            payload.push(Bytes::from(client_id.to_string()));
        }

        self.database
            .insert(INDEX_ORDER_CLIENT.to_string(), Some(payload))
    }

    fn update_actor(
        &self,
        actor_id: &ActorId,
        state: &AHashMap<String, Bytes>,
    ) -> anyhow::Result<()> {
        let key = format!("{ACTORS}{REDIS_DELIMITER}{actor_id}{REDIS_DELIMITER}state");
        self.update_state(key, state)
    }

    fn update_strategy(
        &self,
        strategy_id: &StrategyId,
        state: &AHashMap<String, Bytes>,
    ) -> anyhow::Result<()> {
        let key = format!("{STRATEGIES}{REDIS_DELIMITER}{strategy_id}{REDIS_DELIMITER}state");
        self.update_state(key, state)
    }

    fn update_account(&self, account: &AccountAny) -> anyhow::Result<()> {
        let account_id = account.id();
        let key = format!("{ACCOUNTS}{REDIS_DELIMITER}{account_id}");
        let payload = self.serialize_account_event(account)?;
        self.append_list(key, payload)
    }

    fn update_order(&self, order_event: &OrderEventAny) -> anyhow::Result<()> {
        let client_order_id = order_event.client_order_id();
        let key = format!("{ORDERS}{REDIS_DELIMITER}{client_order_id}");
        let payload = DatabaseQueries::serialize_payload(self.encoding(), order_event)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::UpdateOrder,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("{FAILED_TX_CHANNEL}: {e}"))
    }

    fn update_order_state(&self, order: &OrderAny) -> anyhow::Result<()> {
        let order_event = order.last_event();
        let client_order_id = order_event.client_order_id();
        let key = format!("{ORDERS}{REDIS_DELIMITER}{client_order_id}");
        let payload = self.serialize_order_event(order_event)?;
        let update = OrderIndexUpdate::from_order(order);
        self.send_command(
            DatabaseOperation::AppendOrderEvent(update),
            key,
            Some(vec![payload]),
        )
    }

    fn update_position(&self, position: &Position) -> anyhow::Result<()> {
        let position_id = position.id;
        if position.fill_voids.is_empty() {
            let key = format!("{POSITIONS}{REDIS_DELIMITER}{position_id}");
            let payload = self.serialize_position_event(position)?;
            self.append_list(key, payload)?;
        } else {
            self.add_position_snapshot(&PositionSnapshot::from_replay_state(position, None))?;
        }

        let position_id_bytes = Bytes::from(position_id.to_string());

        if position.is_open() {
            self.database.insert(
                INDEX_POSITIONS_OPEN.to_string(),
                Some(vec![position_id_bytes.clone()]),
            )?;
            self.send_command(
                DatabaseOperation::Delete,
                INDEX_POSITIONS_CLOSED.to_string(),
                Some(vec![position_id_bytes]),
            )?;
        } else if position.is_closed() {
            self.database.insert(
                INDEX_POSITIONS_CLOSED.to_string(),
                Some(vec![position_id_bytes.clone()]),
            )?;
            self.send_command(
                DatabaseOperation::Delete,
                INDEX_POSITIONS_OPEN.to_string(),
                Some(vec![position_id_bytes]),
            )?;
        }

        Ok(())
    }

    fn snapshot_order_state(&self, order: &OrderAny) -> anyhow::Result<()> {
        let snapshot = OrderSnapshot::from(order.clone());
        self.add_order_snapshot(&snapshot)
    }

    fn snapshot_position_state(
        &self,
        position: &Position,
        ts_snapshot: UnixNanos,
        unrealized_pnl: Option<Money>,
    ) -> anyhow::Result<()> {
        let mut snapshot = PositionSnapshot::from(position, unrealized_pnl);
        snapshot.ts_init = ts_snapshot;
        self.add_position_snapshot(&snapshot)
    }

    fn heartbeat(&self, timestamp: UnixNanos) -> anyhow::Result<()> {
        let timestamp = format_timestamp(timestamp);
        self.database.insert(
            format!("{HEALTH}{REDIS_DELIMITER}heartbeat"),
            Some(vec![Bytes::from(timestamp)]),
        )
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::{OrderSide, OrderType, TriggerType},
        events::order::spec::{
            OrderEmulatedSpec, OrderPendingCancelSpec, OrderRejectedSpec, OrderReleasedSpec,
        },
        identifiers::TradeId,
        instruments::stubs::crypto_perpetual_ethusdt,
        orders::{builder::OrderTestBuilder, stubs::TestOrderEventStubs},
        types::{Price, Quantity},
    };
    use rstest::rstest;

    use super::*;

    const TRADER_KEY: &str = "trader-TESTER-001";

    // The order index updates as written before `OrderIndexUpdate` existed, kept as the oracle
    // for the index changes that both order event write paths must apply.
    fn reference_update_order_indexes(pipe: &mut Pipeline, trader_key: &str, order: &OrderAny) {
        let order_id_bytes = order.client_order_id().to_string();
        let key = |index| full_redis_key(trader_key, index);

        insert_set(pipe, &key(INDEX_ORDERS), order_id_bytes.as_bytes());

        if order.venue_order_id().is_some() {
            insert_set(pipe, &key(INDEX_ORDER_IDS), order_id_bytes.as_bytes());
        }

        if order.is_inflight() {
            insert_set(pipe, &key(INDEX_ORDERS_INFLIGHT), order_id_bytes.as_bytes());
        } else {
            remove_from_set(pipe, &key(INDEX_ORDERS_INFLIGHT), order_id_bytes.as_bytes());
        }

        if order.is_open() {
            remove_from_set(pipe, &key(INDEX_ORDERS_CLOSED), order_id_bytes.as_bytes());
            insert_set(pipe, &key(INDEX_ORDERS_OPEN), order_id_bytes.as_bytes());
        } else if order.is_closed() {
            remove_from_set(pipe, &key(INDEX_ORDERS_OPEN), order_id_bytes.as_bytes());
            insert_set(pipe, &key(INDEX_ORDERS_CLOSED), order_id_bytes.as_bytes());
        }

        if order.emulation_trigger().is_some() && !order.is_closed() {
            insert_set(pipe, &key(INDEX_ORDERS_EMULATED), order_id_bytes.as_bytes());
        } else {
            remove_from_set(pipe, &key(INDEX_ORDERS_EMULATED), order_id_bytes.as_bytes());
        }
    }

    fn command_args(cmd: &redis::Cmd) -> Vec<Vec<u8>> {
        cmd.args_iter()
            .map(|arg| match arg {
                redis::Arg::Simple(bytes) => bytes.to_vec(),
                _ => panic!("unexpected non-simple argument"),
            })
            .collect()
    }

    fn apply(order: &mut OrderAny, event: OrderEventAny) -> OrderAny {
        order.apply(event).unwrap();
        order.clone()
    }

    // Orders covering each index-relevant state reached through native order events.
    fn orders_in_each_index_state() -> Vec<OrderAny> {
        let mut states = limit_order_lifecycle_states();
        states.extend(rejected_and_emulated_order_states());
        states
    }

    fn limit_order_lifecycle_states() -> Vec<OrderAny> {
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let account_id = AccountId::new("BINANCE-001");
        let mut states = Vec::new();

        let mut limit = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument.id())
            .side(OrderSide::Buy)
            .quantity(Quantity::from("2.0"))
            .price(Price::from("1000.00"))
            .client_order_id(ClientOrderId::new("O-LIMIT"))
            .build();
        states.push(limit.clone());
        let submitted = TestOrderEventStubs::submitted(&limit, account_id);
        states.push(apply(&mut limit, submitted));
        let accepted = TestOrderEventStubs::accepted(&limit, account_id, VenueOrderId::new("V-1"));
        states.push(apply(&mut limit, accepted));
        let pending_cancel = OrderEventAny::PendingCancel(
            OrderPendingCancelSpec::builder()
                .trader_id(limit.trader_id())
                .strategy_id(limit.strategy_id())
                .instrument_id(limit.instrument_id())
                .client_order_id(limit.client_order_id())
                .account_id(account_id)
                .venue_order_id(VenueOrderId::new("V-1"))
                .build(),
        );
        states.push(apply(&mut limit, pending_cancel));
        let partial_fill = TestOrderEventStubs::filled(
            &limit,
            &instrument,
            Some(TradeId::new("T-1")),
            None,
            None,
            Some(Quantity::from("1.0")),
            None,
            None,
            None,
            Some(account_id),
        );
        states.push(apply(&mut limit, partial_fill));
        let final_fill = TestOrderEventStubs::filled(
            &limit,
            &instrument,
            Some(TradeId::new("T-2")),
            None,
            None,
            Some(Quantity::from("1.0")),
            None,
            None,
            None,
            Some(account_id),
        );
        states.push(apply(&mut limit, final_fill));

        states
    }

    fn rejected_and_emulated_order_states() -> Vec<OrderAny> {
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let account_id = AccountId::new("BINANCE-001");
        let mut states = Vec::new();

        let mut rejected = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(instrument.id())
            .side(OrderSide::Sell)
            .quantity(Quantity::from("1.0"))
            .client_order_id(ClientOrderId::new("O-REJECTED"))
            .build();
        let submitted = TestOrderEventStubs::submitted(&rejected, account_id);
        apply(&mut rejected, submitted);
        let rejection = OrderEventAny::Rejected(
            OrderRejectedSpec::builder()
                .trader_id(rejected.trader_id())
                .strategy_id(rejected.strategy_id())
                .instrument_id(rejected.instrument_id())
                .client_order_id(rejected.client_order_id())
                .account_id(account_id)
                .build(),
        );
        states.push(apply(&mut rejected, rejection));

        let mut emulated = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument.id())
            .side(OrderSide::Buy)
            .quantity(Quantity::from("1.0"))
            .price(Price::from("1000.00"))
            .emulation_trigger(TriggerType::BidAsk)
            .client_order_id(ClientOrderId::new("O-EMULATED"))
            .build();
        states.push(emulated.clone());
        let emulation = OrderEventAny::Emulated(
            OrderEmulatedSpec::builder()
                .trader_id(emulated.trader_id())
                .strategy_id(emulated.strategy_id())
                .instrument_id(emulated.instrument_id())
                .client_order_id(emulated.client_order_id())
                .build(),
        );
        states.push(apply(&mut emulated, emulation));
        let mut released = emulated.clone();
        let canceled = TestOrderEventStubs::canceled(&emulated, account_id, None);
        states.push(apply(&mut emulated, canceled));
        let release = OrderEventAny::Released(
            OrderReleasedSpec::builder()
                .trader_id(released.trader_id())
                .strategy_id(released.strategy_id())
                .instrument_id(released.instrument_id())
                .client_order_id(released.client_order_id())
                .released_price(Price::from("1000.00"))
                .build(),
        );
        states.push(apply(&mut released, release));

        states
    }

    #[rstest]
    fn test_update_order_indexes_matches_reference_for_each_order_state() {
        let orders = orders_in_each_index_state();
        assert!(
            orders
                .iter()
                .any(|order| !order.is_open() && !order.is_closed())
        );
        assert!(orders.iter().any(Order::is_inflight));
        assert!(orders.iter().any(Order::is_open));
        assert!(orders.iter().any(Order::is_closed));
        assert!(orders.iter().any(|order| order.venue_order_id().is_some()));
        assert!(
            orders
                .iter()
                .any(|order| order.emulation_trigger().is_some() && !order.is_closed())
        );

        for order in orders {
            let mut expected = redis::pipe();
            reference_update_order_indexes(&mut expected, TRADER_KEY, &order);
            let mut actual = redis::pipe();
            update_order_indexes(&mut actual, TRADER_KEY, &order);

            assert_eq!(
                actual.get_packed_pipeline(),
                expected.get_packed_pipeline(),
                "index commands differ for {} in {:?}",
                order.client_order_id(),
                order.status()
            );
        }
    }

    #[rstest]
    fn test_append_order_event_script_applies_reference_index_changes() {
        for order in orders_in_each_index_state() {
            let key = full_redis_key(
                TRADER_KEY,
                &format!("{ORDERS}{REDIS_DELIMITER}{}", order.client_order_id()),
            );
            let payload = Bytes::from_static(b"event");
            let mut pipe = redis::pipe();
            append_order_event(
                &mut pipe,
                TRADER_KEY,
                &key,
                std::slice::from_ref(&payload),
                OrderIndexUpdate::from_order(&order),
            )
            .unwrap();
            let mut reference = redis::pipe();
            reference_update_order_indexes(&mut reference, TRADER_KEY, &order);

            let commands: Vec<_> = pipe.cmd_iter().collect();
            assert_eq!(commands.len(), 1);
            let args = command_args(commands[0]);
            let key_count: usize = std::str::from_utf8(&args[2]).unwrap().parse().unwrap();
            let index_count = key_count - 1;
            assert_eq!(args[0], b"EVAL");
            assert_eq!(args[1], APPEND_ORDER_EVENT_SCRIPT.as_bytes());
            assert_eq!(args[3], key.as_bytes());
            assert_eq!(args.len(), 3 + key_count + 2 + index_count);
            assert_eq!(args[3 + key_count], &payload[..]);
            assert_eq!(
                args[4 + key_count],
                order.client_order_id().to_string().as_bytes()
            );

            // Each scripted index change must equal the reference SADD or SREM, in order.
            let scripted: Vec<Vec<Vec<u8>>> = (0..index_count)
                .map(|i| {
                    let command: &[u8] = if args[5 + key_count + i] == b"1" {
                        b"SADD"
                    } else {
                        assert_eq!(args[5 + key_count + i], b"0");
                        b"SREM"
                    };
                    vec![
                        command.to_vec(),
                        args[4 + i].clone(),
                        order.client_order_id().to_string().into_bytes(),
                    ]
                })
                .collect();
            let reference: Vec<Vec<Vec<u8>>> = reference.cmd_iter().map(command_args).collect();
            assert_eq!(
                scripted,
                reference,
                "scripted index changes differ for {} in {:?}",
                order.client_order_id(),
                order.status()
            );
        }
    }

    #[rstest]
    fn test_append_order_event_rejects_empty_payload() {
        let order = orders_in_each_index_state().remove(0);
        let mut pipe = redis::pipe();

        let result = append_order_event(
            &mut pipe,
            TRADER_KEY,
            "trader-TESTER-001:orders:O-LIMIT",
            &[],
            OrderIndexUpdate::from_order(&order),
        );

        assert!(result.is_err());
        assert!(pipe.is_empty());
    }

    #[rstest]
    fn test_get_trader_key_with_prefix_and_instance_id() {
        let trader_id = TraderId::from("tester-123");
        let instance_id = UUID4::new();
        let config = CacheConfig {
            use_instance_id: true,
            ..Default::default()
        };

        let key = get_trader_key(trader_id, instance_id, &config);
        assert!(key.starts_with("trader-tester-123:"));
        assert!(key.ends_with(&instance_id.to_string()));
    }

    #[rstest]
    fn test_get_collection_key_valid() {
        let key = "collection:123";
        assert_eq!(get_collection_key(key).unwrap(), "collection");
    }

    #[rstest]
    fn test_get_collection_key_invalid() {
        let key = "no_delimiter";
        assert!(get_collection_key(key).is_err());
    }
}
