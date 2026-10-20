// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/LICENSE-3.0.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Nautilus lifecycle and subscription entry points.

use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use anyhow::Context;
use nautilus_common::{
    clients::DataClient,
    messages::data::{
        RequestInstrument, SubscribeCustomData, SubscribeInstrument, SubscribeInstruments,
        SubscribeQuotes, UnsubscribeCustomData, UnsubscribeInstrument, UnsubscribeInstruments,
        UnsubscribeQuotes,
    },
};
use nautilus_live::task::TaskGroup;
use nautilus_model::identifiers::{ClientId, Venue};

use super::{
    client::PredictDataClient,
    discovery::dynamic_selector_key,
    runtime::{PredictCommand, PredictDataSession, PredictSessionInputs},
};
use crate::PREDICT_VENUE;
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(2);

#[async_trait::async_trait(?Send)]
impl DataClient for PredictDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*PREDICT_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!("Starting Predict data client: client_id={}", self.client_id);
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.tasks.begin_shutdown();
        self.command_tx = None;
        self.provider.clear();
        self.dynamic_registry
            .write()
            .expect("Predict dynamic registry lock poisoned")
            .invalidate_generation();
        self.connected.store(false, Ordering::Release);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        if ensure_open_task_generation(&self.tasks).await? {
            self.cancellation = self.tasks.cancellation_token();
        }

        // This precedes WebSocket subscription and guarantees definitions reach the engine first.
        self.bootstrap_markets().await?;
        let dynamic_generation = self
            .dynamic_registry
            .write()
            .expect("Predict dynamic registry lock poisoned")
            .begin_generation();
        let (command_tx, session) = PredictDataSession::connect(
            &self.config,
            PredictSessionInputs {
                contexts: self.provider.contexts().clone(),
                dynamic_registry: Arc::clone(&self.dynamic_registry),
                dynamic_generation,
                data_sender: self.data_sender.clone(),
                clock: self.clock,
                cancellation: self.cancellation.clone(),
                connected: Arc::clone(&self.connected),
                task_spawner: self.tasks.spawner()?,
            },
        )
        .await?;
        self.connected.store(true, Ordering::Release);
        self.tasks
            .spawn(session.run())
            .inspect_err(|_| {
                self.connected.store(false, Ordering::Release);
            })
            .context("failed to spawn Predict WebSocket session")?;
        self.command_tx = Some(command_tx);
        log::info!("Connected: client_id={}", self.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.tasks.begin_shutdown();
        self.command_tx = None;
        self.tasks
            .finish_shutdown(Duration::from_secs(1), DISCONNECT_TIMEOUT)
            .await
            .context("failed to stop Predict tasks")?;
        self.provider.clear();
        self.dynamic_registry
            .write()
            .expect("Predict dynamic registry lock poisoned")
            .invalidate_generation();
        self.connected.store(false, Ordering::Release);
        log::info!("Disconnected: client_id={}", self.client_id);
        Ok(())
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        self.request_instrument_definition(request)
    }

    fn subscribe_instrument(&mut self, subscription: SubscribeInstrument) -> anyhow::Result<()> {
        self.publish_instrument(subscription.instrument_id)
    }

    fn subscribe_instruments(&mut self, _subscription: SubscribeInstruments) -> anyhow::Result<()> {
        // Definitions for rolling selectors are emitted immediately after discovery. There is no
        // Predict venue-wide instrument stream to open; this command only records the Nautilus
        // subscription so those locally published definitions reach venue-level consumers.
        Ok(())
    }

    fn unsubscribe_instrument(
        &mut self,
        _unsubscription: &UnsubscribeInstrument,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn unsubscribe_instruments(
        &mut self,
        _unsubscription: &UnsubscribeInstruments,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn subscribe_quotes(&mut self, command: SubscribeQuotes) -> anyhow::Result<()> {
        let id = command.instrument_id;
        let context = self
            .provider
            .contexts()
            .values()
            .find(|c| c.yes_instrument_id == id || c.no_instrument_id == id)
            .cloned()
            .or_else(|| {
                self.dynamic_registry
                    .read()
                    .expect("Predict dynamic registry lock poisoned")
                    .markets
                    .values()
                    .map(|m| &m.context)
                    .find(|c| c.yes_instrument_id == id || c.no_instrument_id == id)
                    .cloned()
            })
            .with_context(|| format!("Predict quote instrument {id} was not loaded"))?;
        self.send_command(PredictCommand::SubscribeQuotes(id, context))
    }

    fn unsubscribe_quotes(&mut self, command: &UnsubscribeQuotes) -> anyhow::Result<()> {
        self.send_command(PredictCommand::UnsubscribeQuotes(command.instrument_id))
    }

    // Use either explicit snapshots or one rolling selector for a given market.
    // Snapshot dispatch publishes one DataType identity per frame; it does not fan out
    // between explicit and selector metadata (or overlapping rolling selectors).
    // Keep these ownership modes separate when configuring strategies and recording.
    fn subscribe(&mut self, command: SubscribeCustomData) -> anyhow::Result<()> {
        match command.data_type.type_name() {
            "PredictOrderbookSnapshot" => self.send_command(PredictCommand::Subscribe(
                self.validate_snapshot_data_type(&command.data_type)?,
            )),
            "PredictCryptoUpDown" => self.send_command(PredictCommand::StartDynamic(
                Self::dynamic_selector(&command.data_type)?,
            )),
            _ => anyhow::bail!("unsupported Predict custom data type {}", command.data_type),
        }
    }

    fn unsubscribe(&mut self, command: &UnsubscribeCustomData) -> anyhow::Result<()> {
        match command.data_type.type_name() {
            "PredictOrderbookSnapshot" => self.send_command(PredictCommand::Unsubscribe(
                self.validate_snapshot_data_type(&command.data_type)?,
            )),
            "PredictCryptoUpDown" => self.send_command(PredictCommand::StopDynamic(
                dynamic_selector_key(&Self::dynamic_selector(&command.data_type)?)?,
            )),
            _ => anyhow::bail!("unsupported Predict custom data type {}", command.data_type),
        }
    }
}

/// Reopens a task group only after its prior generation has fully drained.
///
/// `DataClient::stop` and `reset` are synchronous, whereas `TaskGroup` deliberately requires
/// the owner to await the previous generation before opening the next one.
pub(super) async fn ensure_open_task_generation(tasks: &TaskGroup) -> anyhow::Result<bool> {
    if tasks.is_open() {
        return Ok(false);
    }
    tasks
        .finish_shutdown(Duration::from_secs(1), DISCONNECT_TIMEOUT)
        .await
        .context("failed to finish previous Predict task generation")?;
    tasks
        .start_generation()
        .context("failed to start Predict task generation")?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use nautilus_live::task::TaskGroup;

    use super::ensure_open_task_generation;
    #[tokio::test]
    async fn reset_generation_is_drained_before_it_is_reopened() {
        let tasks = TaskGroup::new();
        tasks.begin_shutdown();
        assert!(!tasks.is_open());
        assert!(ensure_open_task_generation(&tasks).await.unwrap());
        assert!(tasks.is_open());
        assert!(!ensure_open_task_generation(&tasks).await.unwrap());
    }
}
