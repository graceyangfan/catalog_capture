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

//! Numeric REST routing IDs. Exchange hashes remain Native VenueOrderId.
use std::sync::{Arc, Mutex};

use ahash::AHashMap;
use alloy::primitives::U256;
use nautilus_model::identifiers::VenueOrderId;

pub(super) type RestOrderIds = Arc<Mutex<AHashMap<VenueOrderId, U256>>>;

pub(super) fn register(ids: &RestOrderIds, hash: VenueOrderId, id: U256) -> anyhow::Result<()> {
    let mut ids = ids.lock().expect("REST order IDs poisoned");
    match ids.entry(hash) {
        std::collections::hash_map::Entry::Occupied(entry) => anyhow::ensure!(
            *entry.get() == id,
            "Predict hash has conflicting REST order IDs"
        ),
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(id);
        }
    }
    Ok(())
}

// Learning a cancel route is optional to an already identified execution fact.
// Keep strict registration for actual command routing; preserve the existing owner on conflict.
pub(super) fn observe(ids: &RestOrderIds, hash: VenueOrderId, raw: &str) {
    if let Err(error) = parse_id(raw).and_then(|id| register(ids, hash, id)) {
        log::warn!("Predict REST route observation skipped for {hash}: {error}");
    }
}

pub(super) fn register_many(
    ids: &RestOrderIds,
    routes: &[(VenueOrderId, U256)],
) -> anyhow::Result<()> {
    let mut incoming = AHashMap::with_capacity(routes.len());
    for (hash, id) in routes {
        anyhow::ensure!(
            incoming
                .insert(*hash, *id)
                .is_none_or(|previous| previous == *id),
            "Predict hash has conflicting REST order IDs"
        );
    }
    // Validate the whole batch before mutation, retaining the first owner's route.
    let mut ids = ids.lock().expect("REST order IDs poisoned");
    anyhow::ensure!(
        incoming
            .iter()
            .all(|(hash, id)| ids.get(hash).is_none_or(|previous| previous == id)),
        "Predict hash has conflicting REST order IDs"
    );
    ids.extend(incoming);
    Ok(())
}

pub(super) fn parse_id(id: &str) -> anyhow::Result<U256> {
    crate::common::serde::decimal_u256::parse::<serde::de::value::Error>(id).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_routes_preserve_strict_decimal_uint256_bounds() {
        for value in [
            "0",
            "00042",
            "1234567890",
            "115792089237316195423570985008687907853269984665640564039457584007913129639935",
        ] {
            assert_eq!(
                parse_id(value).unwrap(),
                U256::from_str_radix(value, 10).unwrap()
            );
        }
        for value in [
            "",
            "-1",
            "+1",
            "1e3",
            "0x10",
            " 1",
            "1 ",
            "１",
            "115792089237316195423570985008687907853269984665640564039457584007913129639936",
        ] {
            assert!(parse_id(value).is_err(), "accepted invalid route: {value}");
        }
    }

    #[test]
    fn duplicate_routes_are_idempotent_and_conflicts_preserve_every_existing_owner() {
        let ids = RestOrderIds::default();
        let first = VenueOrderId::from("first");
        let other = VenueOrderId::from("other");
        register(&ids, first, U256::from(1)).unwrap();
        register(&ids, first, U256::from(1)).unwrap();
        assert!(register_many(&ids, &[(other, U256::from(2)), (first, U256::from(3))]).is_err());
        assert_eq!(ids.lock().unwrap().len(), 1);
        assert_eq!(
            ids.lock().unwrap().get(&first).copied(),
            Some(U256::from(1))
        );
        assert!(register_many(&ids, &[(other, U256::from(2)), (other, U256::from(3))]).is_err());
        assert!(ids.lock().unwrap().get(&other).copied().is_none());
        assert!(parse_id("-1").is_err());
        assert!(parse_id("1e3").is_err());
    }
}
