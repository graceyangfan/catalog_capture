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

//! Generation-scoped instrument definitions shared by the client and its data session.

use ahash::AHashMap;
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};

use crate::PredictMarketContext;
/// Definitions discovered by a rolling selector during one live-client generation.
///
/// `PredictInstrumentProvider` remains the synchronous bootstrap provider required by the
/// Nautilus provider interface. The transport task owns no durable definitions of its own:
/// it registers a discovered market here before publishing it, so synchronous instrument
/// requests from the engine can resolve the same definitions while the selector is live.
#[derive(Debug, Default)]
pub(super) struct PredictDynamicMarketRegistry {
    pub(super) generation: u64,
    pub(super) markets: AHashMap<u64, PredictDynamicMarket>,
}

#[derive(Debug)]
pub(super) struct PredictDynamicMarket {
    pub(super) context: PredictMarketContext,
    pub(super) instruments: AHashMap<InstrumentId, InstrumentAny>,
}

impl PredictDynamicMarketRegistry {
    pub(super) fn begin_generation(&mut self) -> u64 {
        self.generation = self.generation.wrapping_add(1);
        self.markets.clear();
        self.generation
    }

    pub(super) fn invalidate_generation(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.markets.clear();
    }

    pub(super) fn register(
        &mut self,
        generation: u64,
        context: PredictMarketContext,
        instruments: &[InstrumentAny],
    ) -> bool {
        if self.generation != generation {
            return false;
        }
        self.markets.insert(
            context.market_id,
            PredictDynamicMarket {
                context,
                instruments: instruments
                    .iter()
                    .cloned()
                    .map(|instrument| (instrument.id(), instrument))
                    .collect(),
            },
        );
        true
    }

    pub(super) fn context(&self, market_id: u64) -> Option<PredictMarketContext> {
        self.markets
            .get(&market_id)
            .map(|market| market.context.clone())
    }

    pub(super) fn instrument(&self, instrument_id: InstrumentId) -> Option<InstrumentAny> {
        let market_id = crate::common::parse_market_id(&instrument_id).ok()?;
        self.markets
            .get(&market_id)?
            .instruments
            .get(&instrument_id)
            .cloned()
    }

    pub(super) fn remove(&mut self, generation: u64, market_id: u64) {
        if self.generation == generation {
            self.markets.remove(&market_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PredictDynamicMarketRegistry;
    use crate::{PredictMarketContext, instrument_id_from_outcome};
    fn context(market_id: u64) -> PredictMarketContext {
        PredictMarketContext {
            market_id,
            yes_instrument_id: instrument_id_from_outcome(market_id, 1).unwrap(),
            no_instrument_id: instrument_id_from_outcome(market_id, 2).unwrap(),
            price_precision: 2,
            size_precision: 16,
        }
    }

    #[test]
    fn dynamic_registry_rejects_a_stale_client_generation() {
        let mut registry = PredictDynamicMarketRegistry::default();
        let first_generation = registry.begin_generation();
        assert!(registry.register(first_generation, context(42), &[]));
        assert_eq!(registry.context(42).unwrap().market_id, 42);

        let second_generation = registry.begin_generation();
        assert_ne!(first_generation, second_generation);
        assert!(registry.context(42).is_none());
        assert!(!registry.register(first_generation, context(43), &[]));
        assert!(registry.register(second_generation, context(43), &[]));
        assert_eq!(registry.context(43).unwrap().market_id, 43);
    }

    #[test]
    fn dynamic_registry_removes_only_its_current_generation_market() {
        let mut registry = PredictDynamicMarketRegistry::default();
        let generation = registry.begin_generation();
        assert!(registry.register(generation, context(42), &[]));
        registry.remove(generation.wrapping_add(1), 42);
        assert!(registry.context(42).is_some());
        registry.remove(generation, 42);
        assert!(registry.context(42).is_none());
    }
}
