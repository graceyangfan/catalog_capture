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
use std::sync::Arc;

use alloy::primitives::U256;
use nautilus_core::AtomicMap;
use nautilus_model::identifiers::VenueOrderId;

pub(super) type RestOrderIds = Arc<AtomicMap<VenueOrderId, U256>>;

pub(super) fn register(ids: &RestOrderIds, hash: VenueOrderId, id: U256) -> anyhow::Result<()> {
    if ids.get_cloned(&hash) == Some(id) {
        return Ok(());
    }
    register_many(ids, &[(hash, id)])
}

pub(super) fn register_many(
    ids: &RestOrderIds,
    routes: &[(VenueOrderId, U256)],
) -> anyhow::Result<()> {
    let mut incoming = std::collections::HashMap::new();
    for (hash, id) in routes {
        anyhow::ensure!(
            incoming
                .insert(*hash, *id)
                .is_none_or(|previous| previous == *id),
            "Predict hash has conflicting REST order IDs"
        );
    }
    let current = ids.load();
    if incoming
        .iter()
        .all(|(hash, id)| current.get(hash) == Some(id))
    {
        return Ok(());
    }
    drop(current);
    // One snapshot update per verified REST batch, with atomic first-owner protection.
    let mut conflict = false;
    ids.rcu(|map| {
        conflict = incoming
            .iter()
            .any(|(hash, id)| map.get(hash).is_some_and(|previous| previous != id));
        if !conflict {
            map.extend(incoming.iter().map(|(hash, id)| (*hash, *id)));
        }
    });
    anyhow::ensure!(!conflict, "Predict hash has conflicting REST order IDs");
    Ok(())
}

pub(super) fn parse_id(id: &str) -> anyhow::Result<U256> {
    #[derive(serde::Deserialize)]
    struct DecimalId(#[serde(with = "crate::common::serde::decimal_u256")] U256);
    Ok(serde_json::from_value::<DecimalId>(serde_json::Value::String(id.to_owned()))?.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_routes_are_idempotent_and_conflicts_preserve_every_existing_owner() {
        let ids = RestOrderIds::default();
        let first = VenueOrderId::from("first");
        let other = VenueOrderId::from("other");
        register(&ids, first, U256::from(1)).unwrap();
        register(&ids, first, U256::from(1)).unwrap();
        assert!(register_many(&ids, &[(other, U256::from(2)), (first, U256::from(3))]).is_err());
        assert_eq!(ids.len(), 1);
        assert_eq!(ids.get_cloned(&first), Some(U256::from(1)));
        assert!(register_many(&ids, &[(other, U256::from(2)), (other, U256::from(3))]).is_err());
        assert!(ids.get_cloned(&other).is_none());
        assert!(parse_id("-1").is_err());
        assert!(parse_id("1e3").is_err());
    }
}
