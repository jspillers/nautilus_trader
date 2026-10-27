# Rust

Nautilus has a complete Rust implementation under the `crates/` directory.
You can write actors, strategies, run backtests, and trade live without Python.
The domain model is shared with the Python package, which runs user components
on the same Rust engine through PyO3.

:::warning
The Rust API is under active development. Method signatures and trait
requirements may change between releases.
:::

## System implementations

Nautilus has two paths. Choose the one that matches how you want to author
and deploy the system.

- **Rust**: Pure Rust under `crates/`. Runs without Python.
- **Python**: [Python user components](python.md) running on the Rust core through PyO3
  bindings under `python/nautilus_trader/`.

### Capability matrix

| Component       | Rust | Python |
| --------------- | ---- | ------ |
| Strategy        | ✓    | ✓      |
| Actor           | ✓    | ✓      |
| DataEngine      | ✓    | ✓      |
| ExecutionEngine | ✓    | ✓      |
| RiskEngine      | ✓    | ✓      |
| BacktestEngine  | ✓    | ✓      |
| BacktestNode    | ✓    | ✓      |
| LiveNode        | ✓    | ✓      |
| OrderEmulator   | ✓    | ✓      |
| Matching engine | ✓    | ✓      |
| Portfolio       | ✓    | ✓      |
| Accounts        | ✓    | ✓      |
| Cache           | ✓    | ✓      |
| MessageBus      | ✓    | ✓      |
| Data catalog    | ✓    | ✓      |
| Indicators      | ✓    | ✓      |
| Exec algorithms | TWAP | TWAP   |
| Controller      | ✓    | ✓      |
| Tearsheets      | -    | ✓      |

:::note
The Controller runtime powers both Rust and Python components. Rust registers a
native Controller Actor and compiled strategy factories. Importable controller
class construction through `ImportableControllerConfig` remains Python-only.
:::

### Adapters

| Adapter             | Rust | Python |
| ------------------- | ---- | ------ |
| Architect AX        | ✓    | ✓      |
| Betfair             | ✓    | ✓      |
| Binance             | ✓    | ✓      |
| Blockchain          | ✓    | ✓      |
| Bybit               | ✓    | ✓      |
| Coinbase            | ✓    | ✓      |
| Databento           | ✓    | ✓      |
| Deribit             | ✓    | ✓      |
| Derive              | ✓    | ✓      |
| dYdX                | ✓    | ✓      |
| Hyperliquid         | ✓    | ✓      |
| Interactive Brokers | ✓    | ✓      |
| Kraken              | ✓    | ✓      |
| Lighter             | ✓    | ✓      |
| OKX                 | ✓    | ✓      |
| Polymarket          | ✓    | ✓      |
| Sandbox             | ✓    | ✓      |
| Tardis              | ✓    | ✓      |

### Choosing a path

- **Rust** gives native performance without a Python runtime. All core
  trading functionality is available. Use it for latency-sensitive
  deployments or teams that prefer a compiled language.
- **Python** keeps the Python authoring experience. User components (actors,
  strategies) run on the Rust core for data processing and execution.

## Project setup

The Nautilus crates are published to
[crates.io](https://crates.io/crates/nautilus-backtest). Add them to your
`Cargo.toml`:

```toml
[dependencies]
nautilus-backtest = "0.65"
nautilus-common = "0.65"
nautilus-execution = "0.65"
nautilus-model = { version = "0.65", features = ["test-support"] }
nautilus-trading = { version = "0.65", features = ["examples"] }

anyhow = "1"
log = "0.4"
```

For live trading, add the live crate and the adapter for your venue:

```toml
[dependencies]
nautilus-live = "0.65"
nautilus-okx = "0.65"
```

To track the latest development branch, point all Nautilus dependencies at the
same git source to avoid type mismatches between crates.io and git versions:

```toml
[dependencies]
nautilus-backtest = { git = "https://github.com/nautechsystems/nautilus_trader.git", branch = "develop" }
nautilus-common = { git = "https://github.com/nautechsystems/nautilus_trader.git", branch = "develop" }
nautilus-execution = { git = "https://github.com/nautechsystems/nautilus_trader.git", branch = "develop" }
nautilus-model = { git = "https://github.com/nautechsystems/nautilus_trader.git", branch = "develop", features = ["test-support"] }
nautilus-trading = { git = "https://github.com/nautechsystems/nautilus_trader.git", branch = "develop", features = ["examples"] }
```

The minimum supported Rust version (MSRV) is **1.99.0**.

### Feature flags

| Flag             | Crate               | Effect                                              |
| ---------------- | ------------------- | --------------------------------------------------- |
| `high-precision` | `nautilus-model`    | 16-digit fixed precision (default is 9).            |
| `test-support`   | `nautilus-model`    | Test fixtures, builders, specs, and defaults.       |
| `examples`       | `nautilus-trading`  | Example strategies (`EmaCross`, `GridMarketMaker`). |
| `streaming`      | `nautilus-backtest` | Catalog-based data streaming via `BacktestNode`.    |
| `defi`           | `nautilus-model`    | DeFi data types. Implies `high-precision`.          |

:::tip
Standard 9-digit precision handles most traditional finance instruments.
Enable `high-precision` for crypto venues where prices or quantities need more than nine decimal
places.
:::

### Memory allocator

The `nautilus` CLI and Python wheels use [mimalloc](https://crates.io/crates/mimalloc) for Rust
allocations. A Rust binary chooses its own allocator, so add mimalloc to yours to match:

```toml
[dependencies]
mimalloc = "0.1"
```

```rust
use mimalloc::MiMalloc;
use nautilus_common::logging::headers::register_allocator_mimalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    register_allocator_mimalloc();
}
```

Declaring `GLOBAL` selects mimalloc. Call `register_allocator_mimalloc` at the start of `main`,
before constructing a Nautilus node, so the version header reports `allocator: mimalloc <version>`.
Registration **only updates the header metadata**; it does not select the allocator.

The default system allocator also works. Measure throughput and resident memory on your workload
and platform when comparing allocator choices.
See the [architecture guide](architecture.md#memory-allocation) for background.

## Actors

An actor receives market data, custom data/signals, and system events but does
not manage orders. Implement the `DataActor` trait and use `nautilus_actor!` to
wire your `DataActorCore` field into the runtime contract. Your type
implements or derives `Debug`; the macro supplies the native runtime wiring.
User code normally uses the `DataActor` facade methods for subscriptions,
cache access, and clock access.

### Handler methods

Override any handler on the `DataActor` trait to receive the corresponding
data or event. All handlers have default no-op implementations, so you only
override what you need.

| Handler                | Receives                  |
| ---------------------- | ------------------------- |
| `on_start`             | Actor started.            |
| `on_stop`              | Actor stopped.            |
| `on_quote`             | `QuoteTick`               |
| `on_trade`             | `TradeTick`               |
| `on_bar`               | `Bar`                     |
| `on_book_deltas`       | `OrderBookDeltas`         |
| `on_book`              | `OrderBook` (at interval) |
| `on_instrument`        | `InstrumentAny`           |
| `on_mark_price`        | `MarkPriceUpdate`         |
| `on_index_price`       | `IndexPriceUpdate`        |
| `on_funding_rate`      | `FundingRateUpdate`       |
| `on_option_greeks`     | `OptionGreeks`            |
| `on_option_chain`      | `OptionChainSlice`        |
| `on_instrument_status` | `InstrumentStatus`        |
| `on_time_event`        | `TimeEvent`               |

For a step-by-step walkthrough, see the
[Write an Actor (Rust)](../how_to/write_rust_actor.md) how-to guide.
For a complete example, see
[`BookImbalanceActor`](https://github.com/nautechsystems/nautilus_trader/tree/develop/crates/trading/src/examples/actors/imbalance).

## Strategies

A strategy extends an actor with order management. Implement `DataActor` for
data handling and use `nautilus_strategy!` to wire your `StrategyCore` field
into the strategy runtime contract. `StrategyCore` stores the runtime strategy
state; normal strategy logic reaches it through facade methods on `self`.
Runtime registration requires the native wiring generated by the macro, but
normal strategy logic uses `Strategy` methods and the facade methods on `self`.
Strategies also override order event handlers on the `Strategy` trait, such as
`on_order_filled` (`OrderFilled`) and `on_order_canceled` (`OrderCanceled`).

### Compiled strategy factories

`nautilus_system::StrategyFactory` binds a compiled strategy constructor to the
strategy and configuration paths in `ImportableStrategyConfig`. Register factories
as builder dependencies and select initial instances in native configuration:

```rust
use nautilus_common::{actor::DataActorConfig, enums::Environment};
use nautilus_live::node::LiveNode;
use nautilus_system::StrategyFactory;
use nautilus_trading::ImportableStrategyConfig;
use my_strategy::{MyStrategy, MyStrategyConfig};

let strategy: ImportableStrategyConfig = serde_json::from_str(recipe_json)?;
let mut node = LiveNode::builder("TRADER-001".into(), Environment::Live)?
    .with_native_controller(DataActorConfig {
        actor_id: Some("CONTROLLER-001".into()),
        ..Default::default()
    })
    .with_strategy_factory(StrategyFactory::new::<MyStrategyConfig, _, _>(
        "my_strategy:MyStrategy",
        "my_strategy:MyStrategyConfig",
        MyStrategy::try_new,
    )?)
    .with_strategies(vec![strategy])
    .build()?;
```

`LiveNodeConfig.native_controller` and `strategies` carry the same serialized
selection. `NautilusKernelBuilder` provides the same builder methods. A native
controller is installed only when explicitly configured; manual composition
remains supported. Do not select both a native controller and an importable Python
controller. The kernel registers initial strategies in `READY` before startup;
normal client initialization and reconciliation still precede their activation.
Failed construction removes staged components through the native Trader.

The configuration type implements `serde::de::DeserializeOwned`; its constructor
returns the concrete native Strategy. Once the node is running, a native owner-thread
callback can call `Controller::create_strategy_from_config` or send the standard
`CreateStrategy` command. Each request selects one of the installed factories.
Factories are fixed after the Controller's initial activation. Unknown strategy
paths, mismatched config paths and invalid parameters return errors. Nautilus retains
strategy registration, unique order tags, component clocks, callbacks and startup
rollback. A matched compiled factory does not load arbitrary Rust code or use a
Python runtime. Builds with Python support retain the existing importable Python
fallback for paths without a matching compiled factory.

The configured kernel supplies the native execution engine to its Controller.
Controller creation and `LiveNode::add_strategy` share the Trader registration
sequence for native OMS overrides and external order claims. Claims are read from
`Strategy::external_order_instrument_ids`; a Strategy exposes its configured claims
through that standard interface. A compiled factory rejects nonempty declared
claims when the concrete Strategy's getter returns `None`; existing direct
registration retains the getter's authority. A manually composed Controller using an explicit
OMS override must receive `with_execution_engine`; missing engine context returns
an error. Failed registration restores prior native claims; failed startup restores
prior OMS metadata only after native retirement succeeds. Normal Controller removal
clears the retired instance's OMS override so a replacement does not inherit it.

`CreateStrategy::with_expected_strategy_id` requires the final native prepared ID
to match before registration, clocks, subscriptions or startup. This compares the
constructed Strategy through native identity normalization without inspecting its
configuration schema. `Controller::execute_with_result` returns
`ControllerCommandResult::Strategy(actual_id)` or `Actor(actual_id)`. Existing
`execute` and `CreateStrategy::new` callers retain their original behavior.

The in-process Controller endpoint acknowledges delivery separately from native
execution errors. Returned component IDs also do not mean deferred stopping or
market exit has completed; inspect native state. An external protocol adapter may
use standard Actor/Signal interfaces and preserve its own correlated receipts.

### Order management

The `Strategy` trait provides order methods through the facade:

| Method                | Action                                    |
| --------------------- | ----------------------------------------- |
| `submit_order`        | Submit a new order to the venue.          |
| `submit_order_list`   | Submit a list of contingent orders.       |
| `modify_order`        | Modify price, quantity, or trigger price. |
| `cancel_order`        | Cancel a specific order.                  |
| `cancel_orders`       | Cancel a filtered set of orders.          |
| `cancel_all_orders`   | Cancel all orders for an instrument.      |
| `close_position`      | Close a position with a market order.     |
| `close_all_positions` | Close all open positions.                 |

The `OrderApi` (accessed via `self.order()`) builds orders and order lists:

- `generate_client_order_id`
- `generate_order_list_id`
- `market`
- `limit`
- `stop_market`
- `stop_limit`
- `market_to_limit`
- `market_if_touched`
- `limit_if_touched`
- `trailing_stop_market`
- `trailing_stop_limit`
- `bracket`
- `create_list`

### Cache and clock access

The cache facade's `try_*` lookups return a lookup error's `Access` variant for borrow conflicts;
`NotFound` still means the requested data is absent. The cache facade's `get()` method and native-backed clock
scheduling methods return access errors through `anyhow::Result`. Existing infallible accessors
still panic on conflicts. Release conflicting borrows before retrying; these errors do not defer
callbacks or change their delivery order.

### Core wiring macros

Rust actors, strategies, and execution algorithms keep their runtime core as a
struct field. The macros tell the traits where that field lives.

| Macro                                          | Core field               | Generates                      |
| ---------------------------------------------- | ------------------------ | ------------------------------ |
| `nautilus_actor!(Type)`                        | `DataActorCore`          | Runtime wiring.                |
| `nautilus_strategy!(Type)`                     | `StrategyCore`           | Runtime wiring and `Strategy`. |
| `nautilus_execution_algorithm!(Type, { ... })` | `ExecutionAlgorithmCore` | Runtime wiring and algorithm.  |

The macros expect a field named `core`; pass a field name as the second
argument when needed. They do not make the actor, strategy, or `StrategyCore`
deref to runtime internals.
The execution algorithm macro takes an `on_order()` implementation block because
that method defines the algorithm's required order handling.

### Native traits

Use facade methods by default:

- `actor_id()`
- `trader_id()`
- `is_registered()`
- `config()`
- `strategy_id()`
- `clock()`
- `cache()`
- `order()`
- `portfolio()`

`DataActorNative`, `StrategyNative`, and `ExecutionAlgorithmNative` are for
native-only access below that facade. This section documents engine, runtime, and explicit
latency-sensitive native Rust code, not the portable authoring path.

| Authoring path            | Native traits?   | Normal API                          |
| ------------------------- | ---------------- | ----------------------------------- |
| Native Rust binary        | Only when needed | `Strategy` and `DataActor` facades. |
| Rust launched from Python | Only when needed | Same as native Rust.                |
| Python-authored component | No               | Facades only.                       |

Native traits expose borrowed core state, `Rc<RefCell<_>>`, and runtime
references. Use them when native Rust code intentionally accepts those borrow
rules for an explicit latency-sensitive path. Engine, runtime, registration,
PyO3, and testkit code can import `DataActorNative`, `StrategyNative`, or
`ExecutionAlgorithmNative` when they need actor-core, strategy-core, or
execution-algorithm-core access. Do not use them in ordinary portable actor,
strategy, or execution algorithm logic or Python-authored components, because
those types do not cross the Python boundary.

`ExecutionAlgorithmCore` owns a `DataActorCore`, but it does not deref to one.
Normal execution algorithm logic should use `id()`, `actor_id()`,
`trader_id()`, `clock()`, and `cache()`. Reach for `ExecutionAlgorithmNative`
only when the code needs native execution-algorithm state.

Choose the smallest native handle and keep each borrow scoped. Use `order()`
for normal strategy order construction. Reach for
`order_factory()` only when native code needs the raw mutable factory borrow.

#### `DataActorNative` methods

| Native method | Return shape             | Use when                        |
| ------------- | ------------------------ | ------------------------------- |
| `core()`      | `&DataActorCore`         | Read actor internals.           |
| `core_mut()`  | `&mut DataActorCore`     | Mutate actor internals.         |
| `clock_mut()` | `RefMut<'_, dyn Clock>`  | Need a mutable clock borrow.    |
| `clock_rc()`  | `Rc<RefCell<dyn Clock>>` | Store or pass the shared clock. |
| `cache_ref()` | `Ref<'_, Cache>`         | Need short live-cache reads.    |
| `cache_rc()`  | `Rc<RefCell<Cache>>`     | Mutate, store, or pass cache.   |

Use `try_cache_ref()` and `try_clock_mut()` to handle failed native borrows without panicking.
`try_cache_ref()` returns `ComponentAccessError::ReadConflict` when the cache is mutably borrowed;
`try_clock_mut()` returns `ComponentAccessError::WriteConflict` when the clock is already borrowed.
Both return `ComponentAccessError::NotRegistered` when registration has not supplied the resource.
Import the error type from `nautilus_common::component`. The error identifies the resource and
attempted operation. Callback reentry can cause a conflict, but a conflict alone does not establish its cause.
See [reentrancy diagnostics](../developer_guide/callback_dispatch.md#reentrancy-and-dispatch-diagnostics)
for message meanings and corrective action.

#### `StrategyNative` methods

| Native method         | Return shape                | Use when                         |
| --------------------- | --------------------------- | -------------------------------- |
| `strategy_core()`     | `&StrategyCore`             | Read strategy internals.         |
| `strategy_core_mut()` | `&mut StrategyCore`         | Mutate strategy internals.       |
| `order_factory()`     | `RefMut<'_, OrderFactory>`  | Need raw mutable factory borrow. |
| `order_factory_rc()`  | `Rc<RefCell<OrderFactory>>` | Store or pass the factory.       |
| `portfolio_rc()`      | `Rc<RefCell<Portfolio>>`    | Store or pass the portfolio.     |

#### `ExecutionAlgorithmNative` methods

| Native method               | Return shape                  | Use when                              |
| --------------------------- | ----------------------------- | ------------------------------------- |
| `exec_algorithm_core()`     | `&ExecutionAlgorithmCore`     | Read execution algorithm internals.   |
| `exec_algorithm_core_mut()` | `&mut ExecutionAlgorithmCore` | Mutate execution algorithm internals. |

For a step-by-step walkthrough, see the
[Write a Strategy (Rust)](../how_to/write_rust_strategy.md) how-to guide.
For complete examples, see
[`EmaCross`](https://github.com/nautechsystems/nautilus_trader/tree/develop/crates/trading/src/examples/strategies/ema_cross)
and
[`GridMarketMaker`](https://github.com/nautechsystems/nautilus_trader/tree/develop/crates/trading/src/examples/strategies/grid_mm).

### Running Rust components

Rust strategies and actors can run through two paths. The examples
below use strategies, but the same pattern applies to bundled actors via
`add_actor` (pure Rust) and `add_builtin_actor` (from Python).

#### Pure Rust

Write your strategy and `main` function in Rust, then build a standalone
binary with `cargo build`. This path requires no Python runtime.

```rust
let strategy = GridMarketMaker::new(config);
node.add_strategy(strategy)?;
node.run().await?;
```

See [Run Live Trading (Rust)](../how_to/run_rust_live_trading.md) for a
full walkthrough.

#### Built-in examples from Python

Pass a type name and config to `add_builtin_strategy` to register a
built-in example strategy from Python. This path exists to single-source
the bundled example strategy code across Rust and Python docs, examples,
and tests. It is not a first-class extension path for adding native
strategies. For custom native components, use pure Rust.

```python
from nautilus_trader.trading import GridMarketMakerConfig

config = GridMarketMakerConfig(
    instrument_id=InstrumentId.from_str("BTC-USDT-SWAP.OKX"),
    max_position=Quantity.from_str("10.0"),
    trade_size=Quantity.from_str("0.1"),
    num_levels=5,
    grid_step_bps=15,
)

node.add_builtin_strategy("GridMarketMaker", config)
```

Built-in strategy configs:

| Config                       | Strategy               |
| ---------------------------- | ---------------------- |
| `CompositeMarketMakerConfig` | `CompositeMarketMaker` |
| `DeltaNeutralVolConfig`      | `DeltaNeutralVol`      |
| `EmaCrossConfig`             | `EmaCross`             |
| `ExecTesterConfig`           | `ExecTester`           |
| `GridMarketMakerConfig`      | `GridMarketMaker`      |
| `HurstVpinDirectionalConfig` | `HurstVpinDirectional` |

`add_builtin_actor` follows the same bundled-only rule for actors used by
examples and tests.

Built-in actor configs (via `add_builtin_actor`):

| Config                     | Actor                |
| -------------------------- | -------------------- |
| `BookImbalanceActorConfig` | `BookImbalanceActor` |
| `DataTesterConfig`         | `DataTester`         |

## Backtesting

For annotated walkthroughs of both APIs, see the
[Run a Backtest (Rust)](../how_to/run_rust_backtest.md) how-to guide.

### `BacktestEngine` (low-level API)

Construct the engine, add venues and instruments, load data, register
strategies, and run. See the full working example:

```bash
cargo run -p nautilus-backtest --features examples --example engine-ema-cross
```

Source:
[`crates/backtest/examples/engine_ema_cross.rs`](https://github.com/nautechsystems/nautilus_trader/tree/develop/crates/backtest/examples/engine_ema_cross.rs)

### `BacktestNode` (high-level API)

Loads data from a `ParquetDataCatalog` and supports streaming in
configurable chunk sizes. Requires the `streaming` feature on
`nautilus-backtest`. See the full working example:

```bash
cargo run -p nautilus-backtest --features examples,streaming --example node-ema-cross
```

Source:
[`crates/backtest/examples/node_ema_cross.rs`](https://github.com/nautechsystems/nautilus_trader/tree/develop/crates/backtest/examples/node_ema_cross.rs)

## Live trading

For an annotated walkthrough, see the
[Run Live Trading (Rust)](../how_to/run_rust_live_trading.md) how-to guide.

The `LiveNode` connects to real venues and data sources through adapter clients. The builder
pattern configures data and execution clients, then `run()` starts the async
event loop. Each adapter provides its own factory and config types.

| Adapter             | Example                                         |
| ------------------- | ----------------------------------------------- |
| Architect AX        | `crates/adapters/architect_ax/examples/`        |
| Betfair             | `crates/adapters/betfair/examples/`             |
| Binance             | `crates/adapters/binance/examples/`             |
| Blockchain          | `crates/adapters/blockchain/examples/`          |
| Bybit               | `crates/adapters/bybit/examples/`               |
| Coinbase            | `crates/adapters/coinbase/examples/`            |
| Databento           | `crates/adapters/databento/examples/`           |
| Deribit             | `crates/adapters/deribit/examples/`             |
| Derive              | `crates/adapters/derive/examples/`              |
| dYdX                | `crates/adapters/dydx/examples/`                |
| Hyperliquid         | `crates/adapters/hyperliquid/examples/`         |
| Interactive Brokers | `crates/adapters/interactive_brokers/examples/` |
| Kraken              | `crates/adapters/kraken/examples/`              |
| Lighter             | `crates/adapters/lighter/examples/`             |
| OKX                 | `crates/adapters/okx/examples/`                 |
| Polymarket          | `crates/adapters/polymarket/examples/`          |
| Sandbox             | `crates/adapters/sandbox/examples/`             |
| Tardis              | `crates/adapters/tardis/examples/`              |

Most adapters include `node_data_tester.rs` and `node_exec_tester.rs`
examples. These test data requests, streaming, and order execution
against live venues.

### Online execution settlement

Rust strategies can call `Strategy::query_strategy_settlement()` after pausing entry
admission and making their native orders and positions flat. The method returns a query
UUID and sends an ordinary native execution command. Subscribe to
`events.execution.strategy_settlement.<strategy_id>` for the typed
`StrategySettlementResult`, and match the exact pending query UUID and native instance.
Keep entry admission paused while the result is pending. The result closes no order or
position, stops no component, and grants no authority for later submissions.

`Quiescent` requires `native_boundary = true`, the same running strategy registration
from query dispatch to the execution-event FIFO marker, and supported clients with no
remaining strategy-owned work. The live runner also requires its execution command and
event queues, callback dispatch, and report collection to be clear at that marker.
Unrelated execution activity can therefore conservatively produce `Pending`; the query
does not pause or drain peers or infer a timed quiet period. Callers must retain their own
strategy-incarnation authority and reject results for unknown or obsolete query UUIDs.

The Sandbox client supports inspection of its delayed command queue, matching state and
unapplied execution projections. Other execution clients default to `Unsupported` until
their adapter implements this capability. A client-only snapshot or a runtime without the
matching live-node boundary cannot certify settlement. Unsupported results and missing
replies must remain unqualified. The result is native typed Rust data; this API adds no
dedicated external result decoder or Python facade.

## Related guides

- [Python](python.md) - Python ownership, runtime, and public API boundaries.
- [Write an Actor (Rust)](../how_to/write_rust_actor.md) - Step-by-step actor walkthrough.
- [Write a Strategy (Rust)](../how_to/write_rust_strategy.md) - Step-by-step strategy walkthrough.
- [Run a Backtest (Rust)](../how_to/run_rust_backtest.md) - BacktestEngine and BacktestNode usage.
- [Run Live Trading (Rust)](../how_to/run_rust_live_trading.md) - LiveNode setup and venue connection.
- [Architecture](architecture.md) - System design and data/execution flow.
- [Actors](actors.md) - Actor concepts (applies to both Python and Rust).
- [Strategies](strategies.md) - Strategy concepts and handler reference.
- [Events](events/) - Event types and handler dispatch.
- [Backtesting](backtesting/) - Backtest concepts and matching engine behavior.

### Observing component lifecycle revisions

Standard native data actors, strategies and execution algorithms expose an
optional `Component::lifecycle_revision()` witness backed by `DataActorCore`.
Each accepted lifecycle transition increments the checked revision before its
hook runs, including when the hook fails. Invalid triggers do not advance it;
overflow rejects the transition without changing state or entering the hook.
Reset preserves the revision. `component_lifecycle_revision` reads the same
capability through the existing borrow-checked component registry.

This witness describes native lifecycle transitions, not financial settlement or
component identity. A caller retaining an earlier stopped-state observation must
also verify the same registered object and exact unchanged revision before
reusing that observation. Standard strategies inherit this capability without a
strategy-specific implementation. Custom `Component` implementations default to
`None`, so callers requiring an intervening-transition witness fail closed for
unsupported components. No Python facade, serialized counter or separate event
observer is introduced.
