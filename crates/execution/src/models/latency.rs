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

use std::{
    fmt::{Debug, Display},
    rc::Rc,
};

use nautilus_core::DurationNanos;
use nautilus_model::events::OrderInitialized;

/// Trait for latency models used in backtesting.
///
/// Latency models simulate network delays for order operations during backtesting.
/// Implementations can provide static or dynamic (jittered) latency values.
pub trait LatencyModel: Debug {
    /// Returns the latency for order insertion operations.
    fn get_insert_latency(&self) -> DurationNanos;

    /// Returns insertion latency for the immutable submitted order.
    ///
    /// Models which do not distinguish order attributes retain their existing latency.
    fn get_insert_latency_for_order(&self, _order: &OrderInitialized) -> DurationNanos {
        self.get_insert_latency()
    }

    /// Returns the latency for order update/modify operations.
    fn get_update_latency(&self) -> DurationNanos;

    /// Returns the latency for order delete/cancel operations.
    fn get_delete_latency(&self) -> DurationNanos;

    /// Returns the base latency component.
    fn get_base_latency(&self) -> DurationNanos;
}

/// Shared runtime handle for a latency model.
#[derive(Clone)]
pub struct LatencyModelHandle(Rc<dyn LatencyModel>);

impl LatencyModelHandle {
    /// Creates a new [`LatencyModelHandle`] from a latency model.
    #[must_use]
    pub fn new<T>(model: T) -> Self
    where
        T: LatencyModel + 'static,
    {
        Self(Rc::new(model))
    }

    /// Creates a new [`LatencyModelHandle`] from an existing reference-counted model.
    #[must_use]
    pub fn from_rc(model: Rc<dyn LatencyModel>) -> Self {
        Self(model)
    }
}

impl Debug for LatencyModelHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple(stringify!(LatencyModelHandle))
            .field(&"<dyn LatencyModel>")
            .finish()
    }
}

impl LatencyModel for LatencyModelHandle {
    fn get_insert_latency_for_order(&self, order: &OrderInitialized) -> DurationNanos {
        self.0.get_insert_latency_for_order(order)
    }

    fn get_insert_latency(&self) -> DurationNanos {
        self.0.get_insert_latency()
    }

    fn get_update_latency(&self) -> DurationNanos {
        self.0.get_update_latency()
    }

    fn get_delete_latency(&self) -> DurationNanos {
        self.0.get_delete_latency()
    }

    fn get_base_latency(&self) -> DurationNanos {
        self.0.get_base_latency()
    }
}

#[derive(Debug, Clone)]
pub enum LatencyModelAny {
    Static(StaticLatencyModel),
}

impl LatencyModel for LatencyModelAny {
    fn get_insert_latency_for_order(&self, order: &OrderInitialized) -> DurationNanos {
        match self {
            Self::Static(model) => model.get_insert_latency_for_order(order),
        }
    }

    fn get_insert_latency(&self) -> DurationNanos {
        match self {
            Self::Static(model) => model.get_insert_latency(),
        }
    }

    fn get_update_latency(&self) -> DurationNanos {
        match self {
            Self::Static(model) => model.get_update_latency(),
        }
    }

    fn get_delete_latency(&self) -> DurationNanos {
        match self {
            Self::Static(model) => model.get_delete_latency(),
        }
    }

    fn get_base_latency(&self) -> DurationNanos {
        match self {
            Self::Static(model) => model.get_base_latency(),
        }
    }
}

impl From<LatencyModelAny> for LatencyModelHandle {
    fn from(model: LatencyModelAny) -> Self {
        Self::new(model)
    }
}

/// Static latency model with fixed latency values.
///
/// Models the latency for different order operations including base network latency
/// and specific operation latencies for insert, update, and delete operations.
///
/// The base latency is automatically added to each operation latency, matching
/// Python's behavior. For example, if `base_latency_nanos = 100ms` and
/// `insert_latency_nanos = 200ms`, the effective insert latency will be 300ms.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.execution", unsendable, from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.execution")
)]
#[allow(
    clippy::struct_field_names,
    reason = "latency_nanos suffix consistently identifies latency types"
)]
pub struct StaticLatencyModel {
    base_latency_nanos: DurationNanos,
    insert_latency_nanos: DurationNanos,
    post_only_insert_latency_nanos: Option<DurationNanos>,
    update_latency_nanos: DurationNanos,
    delete_latency_nanos: DurationNanos,
}

impl StaticLatencyModel {
    /// Creates a new [`StaticLatencyModel`] instance.
    ///
    /// The base latency is added to each operation latency to get the effective latency.
    ///
    /// # Arguments
    ///
    /// * `base_latency_nanos` - Base network latency added to all operations
    /// * `insert_latency_nanos` - Additional latency for order insertion
    /// * `update_latency_nanos` - Additional latency for order updates
    /// * `delete_latency_nanos` - Additional latency for order cancellation
    #[must_use]
    pub fn new(
        base_latency_nanos: DurationNanos,
        insert_latency_nanos: DurationNanos,
        update_latency_nanos: DurationNanos,
        delete_latency_nanos: DurationNanos,
    ) -> Self {
        Self {
            base_latency_nanos,
            insert_latency_nanos: base_latency_nanos + insert_latency_nanos,
            post_only_insert_latency_nanos: None,
            update_latency_nanos: base_latency_nanos + update_latency_nanos,
            delete_latency_nanos: base_latency_nanos + delete_latency_nanos,
        }
    }
    /// Sets the insertion-operation latency for post-only orders.
    ///
    /// The model adds its base latency once, as with the default insertion latency.
    /// Other orders, updates and cancellations retain their configured latencies.
    #[must_use]
    pub fn with_post_only_insert_latency(mut self, latency: DurationNanos) -> Self {
        self.post_only_insert_latency_nanos = Some(self.base_latency_nanos + latency);
        self
    }
}

impl LatencyModel for StaticLatencyModel {
    fn get_insert_latency_for_order(&self, order: &OrderInitialized) -> DurationNanos {
        if order.post_only {
            self.post_only_insert_latency_nanos
                .unwrap_or(self.insert_latency_nanos)
        } else {
            self.insert_latency_nanos
        }
    }

    fn get_insert_latency(&self) -> DurationNanos {
        self.insert_latency_nanos
    }

    fn get_update_latency(&self) -> DurationNanos {
        self.update_latency_nanos
    }

    fn get_delete_latency(&self) -> DurationNanos {
        self.delete_latency_nanos
    }

    fn get_base_latency(&self) -> DurationNanos {
        self.base_latency_nanos
    }
}

impl Display for StaticLatencyModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LatencyModel()")
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[derive(Debug)]
    struct CustomLatencyModel;

    impl LatencyModel for CustomLatencyModel {
        fn get_insert_latency(&self) -> DurationNanos {
            DurationNanos::new(11)
        }

        fn get_update_latency(&self) -> DurationNanos {
            DurationNanos::new(22)
        }

        fn get_delete_latency(&self) -> DurationNanos {
            DurationNanos::new(33)
        }

        fn get_base_latency(&self) -> DurationNanos {
            DurationNanos::new(44)
        }
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_post_only_insertion_preserves_default_and_handle_dispatch(#[case] post_only: bool) {
        use nautilus_model::{
            enums::{OrderSide, OrderType},
            orders::OrderTestBuilder,
        };

        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id("AUD/USD.SIM".into())
            .side(OrderSide::Buy)
            .quantity("1".into())
            .price("1.00000".into())
            .post_only(post_only)
            .build();
        let static_model = StaticLatencyModel::new(
            DurationNanos::from_millis(40),
            DurationNanos::from_millis(250),
            DurationNanos::ZERO,
            DurationNanos::from_millis(60),
        );
        assert_eq!(
            static_model.get_insert_latency_for_order(order.init_event()),
            DurationNanos::from_millis(290)
        );
        let model = static_model.with_post_only_insert_latency(DurationNanos::ZERO);
        let expected = DurationNanos::from_millis(if post_only { 40 } else { 290 });
        assert_eq!(
            model.get_insert_latency_for_order(order.init_event()),
            expected
        );
        assert_eq!(model.get_insert_latency(), DurationNanos::from_millis(290));
        assert_eq!(model.get_update_latency(), DurationNanos::from_millis(40));
        assert_eq!(model.get_delete_latency(), DurationNanos::from_millis(100));
        let handle: LatencyModelHandle = LatencyModelAny::Static(model).into();
        assert_eq!(
            handle.get_insert_latency_for_order(order.init_event()),
            expected
        );
        let legacy = LatencyModelHandle::new(CustomLatencyModel);
        assert_eq!(
            legacy.get_insert_latency_for_order(order.init_event()),
            DurationNanos::new(11)
        );
    }

    #[rstest]
    fn test_latency_model_handle_calls_custom_model() {
        let model: Rc<dyn LatencyModel> = Rc::new(CustomLatencyModel);
        let handle = LatencyModelHandle::from_rc(model);
        let cloned_handle = handle.clone();
        drop(handle);

        assert_eq!(cloned_handle.get_insert_latency(), DurationNanos::new(11));
        assert_eq!(cloned_handle.get_update_latency(), DurationNanos::new(22));
        assert_eq!(cloned_handle.get_delete_latency(), DurationNanos::new(33));
        assert_eq!(cloned_handle.get_base_latency(), DurationNanos::new(44));
    }

    #[rstest]
    fn test_latency_model_handle_from_any_preserves_model() {
        let model = StaticLatencyModel::new(
            DurationNanos::new(1),
            DurationNanos::new(10),
            DurationNanos::new(20),
            DurationNanos::new(30),
        );
        let handle: LatencyModelHandle = LatencyModelAny::Static(model).into();

        assert_eq!(handle.get_insert_latency(), DurationNanos::new(11));
        assert_eq!(handle.get_update_latency(), DurationNanos::new(21));
        assert_eq!(handle.get_delete_latency(), DurationNanos::new(31));
        assert_eq!(handle.get_base_latency(), DurationNanos::new(1));
    }

    #[rstest]
    fn test_static_latency_model() {
        let model = StaticLatencyModel::new(
            DurationNanos::from_millis(1),
            DurationNanos::from_millis(2),
            DurationNanos::from_millis(3),
            DurationNanos::from_millis(4),
        );

        // Base is added to each operation latency
        assert_eq!(model.get_insert_latency().as_u64(), 3_000_000);
        assert_eq!(model.get_update_latency().as_u64(), 4_000_000);
        assert_eq!(model.get_delete_latency().as_u64(), 5_000_000);
        assert_eq!(model.get_base_latency().as_u64(), 1_000_000);
    }
}
