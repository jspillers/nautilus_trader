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

//! Message senders for standalone clients and live runtime dispatch.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, ThreadId},
};

use super::dispatch::{DispatchMessage, DispatchSendTiming};

#[derive(Debug, Default)]
struct SendTimingState {
    enabled: AtomicBool,
    attempted: AtomicU64,
    failed: AtomicU64,
}

/// A message sender which preserves callback ancestry when bound to a live runtime.
///
/// Sends on the runtime owner thread capture its active root. Sends on other threads
/// and sends without an active root are independent ingress. Conversion from a plain Tokio sender supports standalone
/// clients whose receivers consume domain events directly, without callback tracking.
#[derive(Debug)]
pub struct DispatchSender<T> {
    channel: DispatchChannel<T>,
    timing: Arc<SendTimingState>,
}

/// A sender for data, execution, and system events.
pub type EventSender<T> = DispatchSender<T>;

impl<T> DispatchSender<T> {
    /// Binds a dispatch channel to the calling runtime thread.
    #[must_use]
    pub fn new(sender: tokio::sync::mpsc::UnboundedSender<DispatchMessage<T>>) -> Self {
        Self {
            timing: Arc::default(),
            channel: DispatchChannel::Dispatch {
                sender,
                owner: thread::current().id(),
            },
        }
    }

    // panics-doc-ok
    /// Sends a message, preserving the active owner-thread callback root.
    ///
    /// # Errors
    ///
    /// Returns the undelivered envelope if the receiver is closed.
    ///
    /// # Panics
    ///
    /// Panics if the owner exhausts its channel context IDs.
    pub fn send(
        &self,
        message: T,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<DispatchMessage<T>>> {
        match &self.channel {
            DispatchChannel::Plain(sender) => sender
                .send(message)
                .map_err(|e| tokio::sync::mpsc::error::SendError(e.0.into())),
            DispatchChannel::Dispatch { sender, owner } => {
                let mut envelope = DispatchMessage::new(message, *owner);

                if self.timing.enabled.load(Ordering::Relaxed) {
                    let ordinal = self.timing.attempted.fetch_add(1, Ordering::Relaxed) + 1;
                    envelope.mark_send(DispatchSendTiming {
                        ordinal,
                        send_called_ns: nautilus_core::time::nanos_since_unix_epoch(),
                        dispatch_started_ns: 0,
                    });
                    let result = sender.send(envelope);
                    if result.is_err() {
                        self.timing.failed.fetch_add(1, Ordering::Relaxed);
                    }
                    result
                } else {
                    sender.send(envelope)
                }
            }
        }
    }

    /// Enables bounded-scalar timing on this native channel and existing clones.
    ///
    /// Disabled by default. Plain standalone senders have no dispatch envelope
    /// and return false. This only adds diagnostic metadata; admission and the
    /// original Tokio sender are unchanged.
    pub fn enable_send_timing(&self) -> bool {
        if !matches!(self.channel, DispatchChannel::Dispatch { .. }) {
            return false;
        }
        self.timing.enabled.store(true, Ordering::Relaxed);
        true
    }

    /// Returns diagnostic send attempts and failures across all channel clones.
    #[must_use]
    pub fn send_timing_counts(&self) -> (u64, u64) {
        (
            self.timing.attempted.load(Ordering::Relaxed),
            self.timing.failed.load(Ordering::Relaxed),
        )
    }

    /// Returns whether the receiver has closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        match &self.channel {
            DispatchChannel::Plain(sender) => sender.is_closed(),
            DispatchChannel::Dispatch { sender, .. } => sender.is_closed(),
        }
    }

    /// Returns whether both senders target the same channel.
    #[must_use]
    pub fn same_channel(&self, other: &Self) -> bool {
        match (&self.channel, &other.channel) {
            (DispatchChannel::Plain(left), DispatchChannel::Plain(right)) => {
                left.same_channel(right)
            }
            (
                DispatchChannel::Dispatch { sender: left, .. },
                DispatchChannel::Dispatch { sender: right, .. },
            ) => left.same_channel(right),
            _ => false,
        }
    }
}

impl<T> Clone for DispatchSender<T> {
    fn clone(&self) -> Self {
        let channel = match &self.channel {
            DispatchChannel::Plain(sender) => DispatchChannel::Plain(sender.clone()),
            DispatchChannel::Dispatch { sender, owner } => DispatchChannel::Dispatch {
                sender: sender.clone(),
                owner: *owner,
            },
        };

        Self {
            channel,
            timing: Arc::clone(&self.timing),
        }
    }
}

impl<T> From<tokio::sync::mpsc::UnboundedSender<T>> for DispatchSender<T> {
    fn from(sender: tokio::sync::mpsc::UnboundedSender<T>) -> Self {
        Self {
            timing: Arc::default(),
            channel: DispatchChannel::Plain(sender),
        }
    }
}

#[derive(Debug)]
enum DispatchChannel<T> {
    Plain(tokio::sync::mpsc::UnboundedSender<T>),
    Dispatch {
        sender: tokio::sync::mpsc::UnboundedSender<DispatchMessage<T>>,
        owner: ThreadId,
    },
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::live::dispatch::current_dispatch_send_timing;

    #[rstest]
    fn optional_send_timing_reaches_existing_clones_without_changing_values() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let sender = DispatchSender::new(tx);
        let old_clone = sender.clone();
        sender.send(7_u64).unwrap();
        rx.try_recv().unwrap().dispatch(|v| {
            assert_eq!(v, 7);
            assert!(current_dispatch_send_timing().is_none());
        });
        assert!(sender.enable_send_timing());
        let before = nautilus_core::time::nanos_since_unix_epoch();
        old_clone.send(8).unwrap();
        let after = nautilus_core::time::nanos_since_unix_epoch();
        rx.try_recv().unwrap().dispatch(|v| {
            assert_eq!(v, 8);
            let timing = current_dispatch_send_timing().unwrap();
            assert_eq!(timing.ordinal, 1);
            assert!(timing.dispatch_started_ns >= after);
            assert!((before..=after).contains(&timing.send_called_ns));
        });
        assert!(current_dispatch_send_timing().is_none());
        assert_eq!(sender.send_timing_counts(), (1, 0));
        drop(rx);
        assert!(old_clone.send(9).is_err());
        assert_eq!(sender.send_timing_counts(), (2, 1));
    }

    #[rstest]
    fn plain_standalone_sender_cannot_claim_envelope_timing() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let sender = DispatchSender::from(tx);
        assert!(!sender.enable_send_timing());
        sender.send(5_u64).unwrap();
        assert_eq!(rx.try_recv().unwrap(), 5);
        assert_eq!(sender.send_timing_counts(), (0, 0));
    }

    #[rstest]
    fn nested_and_unwinding_dispatch_restore_the_outer_timing() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let sender = DispatchSender::new(tx);
        sender.enable_send_timing();
        sender.send(1_u64).unwrap();
        rx.try_recv().unwrap().dispatch(|_| {
            let outer = current_dispatch_send_timing();
            let plain = DispatchMessage::from(2_u64);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                plain.dispatch(|_| {
                    assert!(current_dispatch_send_timing().is_none());
                    panic!("diagnostic unwind control");
                });
            }));
            assert!(result.is_err());
            assert_eq!(current_dispatch_send_timing(), outer);
        });
        assert!(current_dispatch_send_timing().is_none());
    }
}
