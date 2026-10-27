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

use std::{any::type_name, cell::RefCell, fmt::Debug, rc::Rc};

use nautilus_common::{
    actor::DataActorNative,
    component::Component,
    msgbus::{Endpoint, MStr},
};
use nautilus_execution::engine::ExecutionEngine;
use nautilus_model::{
    enums::OmsType,
    identifiers::{ComponentId, InstrumentId, StrategyId},
};
use nautilus_trading::strategy::{Strategy, StrategyNative};

use crate::trader::Trader;

pub(crate) fn strategy_control_endpoint(strategy_id: StrategyId) -> MStr<Endpoint> {
    format!("{strategy_id}.control").into()
}

pub(crate) fn strategy_registration_id<T>(strategy: &T) -> String
where
    T: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
{
    StrategyNative::strategy_core(strategy)
        .config
        .strategy_id
        .map_or_else(
            || {
                let strategy_type = type_name::<T>()
                    .rsplit("::")
                    .next()
                    .unwrap_or_else(|| type_name::<T>());
                strategy_type.to_string()
            },
            |strategy_id| strategy_id.to_string(),
        )
}

pub(crate) fn base_strategy_id(strategy_id: &str) -> String {
    strategy_id
        .rsplit_once('-')
        .map_or_else(|| strategy_id.to_string(), |(base, _)| base.to_string())
}

pub(crate) fn ensure_unique_order_id_tag(
    existing_order_id_tags: &[&str],
    order_id_tag: &str,
) -> anyhow::Result<()> {
    if existing_order_id_tags.contains(&order_id_tag) {
        anyhow::bail!(
            "Strategy order_id_tag conflict for '{order_id_tag}', explicitly define unique order_id_tag values",
        );
    }

    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct StrategyRegistration {
    pub(crate) strategy_id: StrategyId,
    pub(crate) previous_oms_type: Option<OmsType>,
    pub(crate) prior_claims: Vec<InstrumentId>,
}

impl Trader {
    /// Rejects a component ID this trader already tracks, whatever kind registered it.
    ///
    /// Duplicate adds are otherwise checked only within a kind, so an actor sharing an ID with a
    /// live strategy would overwrite that strategy's clock, registry entries, and wrapper, and a
    /// rollback would then remove state the attempt did not create. The lifecycle collections are
    /// checked alongside the clocks because a component registered externally and tracked through
    /// `add_*_id_for_lifecycle` has no trader-owned clock.
    pub(crate) fn ensure_component_id_available(
        &self,
        component_id: ComponentId,
    ) -> anyhow::Result<()> {
        let id = component_id.inner();
        let tracked = self.clocks.contains_key(&component_id)
            || self.actor_ids.iter().any(|actor_id| actor_id.inner() == id)
            || self
                .strategy_ids
                .iter()
                .any(|strategy_id| strategy_id.inner() == id)
            || self
                .exec_algorithm_ids
                .iter()
                .any(|exec_algorithm_id| exec_algorithm_id.inner() == id);

        if tracked {
            anyhow::bail!(
                "Component {component_id} is already registered with trader {}",
                self.trader_id
            );
        }

        Ok(())
    }

    /// Registers a strategy with its native OMS override and external order claims.
    ///
    /// # Errors
    ///
    /// Returns an error if native identity, claims, engine access, or registration fails.
    pub fn add_strategy_with_execution_engine<T>(
        &mut self,
        strategy: T,
        execution_engine: &Rc<RefCell<ExecutionEngine>>,
    ) -> anyhow::Result<StrategyId>
    where
        T: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let execution_engine = StrategyNative::strategy_core(&strategy)
            .config
            .oms_type
            .map(|_| execution_engine);
        self.add_strategy_with_context(strategy, None, execution_engine)
            .map(|registration| registration.strategy_id)
    }

    pub(crate) fn add_strategy_with_context<T>(
        &mut self,
        mut strategy: T,
        expected_id: Option<StrategyId>,
        execution_engine: Option<&Rc<RefCell<ExecutionEngine>>>,
    ) -> anyhow::Result<StrategyRegistration>
    where
        T: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        self.validate_actor_or_strategy_registration()?;
        let strategy_id = self.prepare_strategy_for_registration(&mut strategy)?;

        if let Some(expected) = expected_id {
            anyhow::ensure!(
                expected == strategy_id,
                "Native strategy identity mismatch: expected '{expected}', prepared '{strategy_id}'"
            );
        }
        self.ensure_component_id_available(ComponentId::from(strategy_id))?;
        let oms_type = StrategyNative::strategy_core(&strategy).config.oms_type;
        let claims = strategy.external_order_instrument_ids().unwrap_or_default();
        self.register_strategy_configuration(
            strategy_id,
            oms_type,
            &claims,
            execution_engine,
            move |trader| trader.add_strategy(strategy),
        )
    }

    pub(crate) fn register_strategy_configuration<F>(
        &mut self,
        strategy_id: StrategyId,
        oms_type: Option<OmsType>,
        instrument_ids: &[InstrumentId],
        execution_engine: Option<&Rc<RefCell<ExecutionEngine>>>,
        register: F,
    ) -> anyhow::Result<StrategyRegistration>
    where
        F: FnOnce(&mut Self) -> anyhow::Result<()>,
    {
        let mut engine = oms_type
            .map(|_| {
                execution_engine
                    .ok_or_else(|| {
                        anyhow::anyhow!("An execution engine is required for an OMS override")
                    })?
                    .try_borrow_mut()
                    .map_err(|e| anyhow::anyhow!("Cannot register OMS type: {e}"))
            })
            .transpose()?;
        let previous_oms_type = if let Some(engine) = &engine {
            engine.oms_type_override(strategy_id)
        } else {
            execution_engine
                .map(|engine| {
                    engine
                        .try_borrow()
                        .map(|engine| engine.oms_type_override(strategy_id))
                        .map_err(|e| anyhow::anyhow!("Cannot read prior OMS type: {e}"))
                })
                .transpose()?
                .flatten()
        };
        let mut prior_claims: Vec<_> = self
            .cache
            .try_borrow()
            .map_err(|e| anyhow::anyhow!("Cannot read prior external order claims: {e}"))?
            .external_order_claim_instrument_ids(Some(strategy_id))
            .into_iter()
            .collect();
        prior_claims.sort_unstable();

        if !instrument_ids.is_empty() {
            self.cache
                .try_borrow_mut()
                .map_err(|e| anyhow::anyhow!("Cannot register external order claims: {e}"))?
                .register_external_order_claims(strategy_id, instrument_ids)?;
        }

        if let Err(e) = register(self) {
            if !self.strategy_ids.contains(&strategy_id) {
                self.release_component(ComponentId::from(strategy_id));
            }

            if !instrument_ids.is_empty() {
                self.cache.try_borrow_mut()
                    .map_err(|rollback_err| anyhow::anyhow!("Strategy registration failed: {e}; cannot roll back external claims: {rollback_err}"))?
                    .set_external_order_claims(strategy_id, &prior_claims)
                    .map_err(|rollback_err| anyhow::anyhow!("Strategy registration failed: {e}; cannot roll back external claims: {rollback_err}"))?;
            }
            return Err(e);
        }

        if let Some(engine) = &mut engine
            && let Some(oms_type) = oms_type
        {
            engine.register_oms_type(strategy_id, oms_type);
        }
        Ok(StrategyRegistration {
            strategy_id,
            previous_oms_type,
            prior_claims,
        })
    }
}
