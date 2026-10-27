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

//! Compiled native strategy construction from importable configuration.

use std::fmt::Debug;

use anyhow::Context;
use nautilus_common::{actor::DataActorNative, component::Component};
use nautilus_model::identifiers::StrategyId;
use nautilus_trading::{ImportableStrategyConfig, Strategy, StrategyNative};
use serde::de::DeserializeOwned;

use crate::controller::Controller;

type StrategyConstructor = dyn Fn(
    &Controller,
    &ImportableStrategyConfig,
    Option<StrategyId>,
    bool,
) -> anyhow::Result<StrategyId>;

/// Constructs one compiled native strategy type through the native controller.
///
/// A factory identifies the strategy and configuration paths it accepts. Those paths
/// select compiled code; they do not cause dynamic library or Python imports. The
/// constructor deserializes strategy-specific parameters, then leaves registration,
/// identity validation, lifecycle and rollback to the controller and trader.
pub struct StrategyFactory {
    strategy_path: String,
    config_path: String,
    constructor: Box<StrategyConstructor>,
}

impl StrategyFactory {
    /// Creates a factory for a compiled native strategy constructor.
    ///
    /// # Errors
    ///
    /// Returns an error if either importable path is empty or contains only whitespace.
    pub fn new<C, T, F>(
        strategy_path: impl Into<String>,
        config_path: impl Into<String>,
        constructor: F,
    ) -> anyhow::Result<Self>
    where
        C: DeserializeOwned,
        T: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
        F: Fn(C) -> anyhow::Result<T> + 'static,
    {
        let strategy_path = strategy_path.into();
        let config_path = config_path.into();
        anyhow::ensure!(!strategy_path.trim().is_empty(), "Strategy path is empty");
        anyhow::ensure!(
            !config_path.trim().is_empty(),
            "Strategy config path is empty"
        );

        Ok(Self {
            strategy_path,
            config_path,
            constructor: Box::new(move |controller, config, expected_id, start| {
                let parameters = serde_json::to_value(&config.config)
                    .context("Failed to map native strategy configuration")?;
                let parameters = serde_json::from_value::<C>(parameters).with_context(|| {
                    format!(
                        "Invalid native strategy configuration '{}'",
                        config.config_path
                    )
                })?;
                let strategy = constructor(parameters)?;
                anyhow::ensure!(
                    StrategyNative::strategy_core(&strategy)
                        .config
                        .external_order_instrument_ids
                        .as_ref()
                        .is_none_or(Vec::is_empty)
                        || strategy.external_order_instrument_ids().is_some(),
                    "Compiled strategy factory does not expose configured external order claims through Strategy::external_order_instrument_ids"
                );
                controller.create_strategy_checked(strategy, expected_id, start)
            }),
        })
    }

    /// Returns the compiled strategy path accepted by this factory.
    #[must_use]
    pub fn strategy_path(&self) -> &str {
        &self.strategy_path
    }

    /// Returns the configuration path accepted by this factory.
    #[must_use]
    pub fn config_path(&self) -> &str {
        &self.config_path
    }

    pub(crate) fn create(
        &self,
        controller: &Controller,
        config: &ImportableStrategyConfig,
        expected_id: Option<StrategyId>,
        start: bool,
    ) -> anyhow::Result<StrategyId> {
        anyhow::ensure!(
            config.strategy_path == self.strategy_path && config.config_path == self.config_path,
            "Native strategy factory '{}' requires config path '{}', received '{}'",
            self.strategy_path,
            self.config_path,
            config.config_path,
        );

        (self.constructor)(controller, config, expected_id, start)
    }
}

impl Debug for StrategyFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(StrategyFactory))
            .field("strategy_path", &self.strategy_path)
            .field("config_path", &self.config_path)
            .finish_non_exhaustive()
    }
}
