// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
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

//! Read-only MatchedSports discovery. All streaming remains in the maintained venue adapters.
use anyhow::{Context, Result};
use catalog_capture_core::paired_markets::{
    MatchRejection, MatchedMarket, MatchedOutcome, MatchedSportsCaptureConfig, MatchedUniverse,
};
use catalog_capture_runtime_adapter::matched_sports::MatchedSportsDiscoveryInbox;
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::identifiers::InstrumentId;
use nautilus_polymarket::http::{
    gamma::PolymarketGammaRawHttpClient, models::GammaMarket, query::GetGammaMarketsParams,
};
use nautilus_predict::{
    PredictHttpClient,
    common::predict_outcome_instrument_id,
    http::sports::{PredictSportsCategory, PredictSportsMarket, PredictSportsTeam},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

mod selection;
use selection::SelectionHttp;

/// Aborting the run also cancels any in-flight discovery; no detached task outlives the node.
pub struct DiscoveryTask(tokio::task::JoinHandle<()>);
impl Drop for DiscoveryTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub fn spawn(
    config: MatchedSportsCaptureConfig,
    api_key: &str,
    inbox: Arc<MatchedSportsDiscoveryInbox>,
) -> Result<DiscoveryTask> {
    let predict = PredictHttpClient::new(api_key, None, 10)?;
    let gamma = PolymarketGammaRawHttpClient::new(None, 10)?;
    let selection = SelectionHttp::new()?;
    Ok(DiscoveryTask(tokio::spawn(async move {
        loop {
            if let Some(generation) = inbox.active_generation() {
                let now = get_atomic_clock_realtime().get_time_ns().as_u64();
                let result = tokio::time::timeout(
                    Duration::from_secs(120),
                    discover(&predict, &gamma, &selection, &config, now),
                )
                .await
                .map_err(|_| anyhow::anyhow!("MatchedSports discovery deadline exceeded"))
                .and_then(|r| r)
                .map_err(|e| format!("{e:#}"));
                inbox.publish(generation, result);
            }
            tokio::time::sleep(Duration::from_secs(
                if inbox.active_generation().is_some() {
                    config.refresh_secs
                } else {
                    1
                },
            ))
            .await;
        }
    })))
}
async fn discover(
    predict: &PredictHttpClient,
    gamma: &PolymarketGammaRawHttpClient,
    selection: &SelectionHttp,
    config: &MatchedSportsCaptureConfig,
    now: u64,
) -> Result<MatchedUniverse> {
    let mut categories = BTreeMap::new();
    for tag in &config.tag_ids {
        for category in predict
            .request_sports_categories(*tag, config.max_pages)
            .await?
        {
            categories.insert(category.id, category);
        }
    }
    let mut candidates = Vec::new();
    let mut result = MatchedUniverse {
        observed_ns: now,
        ..Default::default()
    };
    let mut seen = BTreeSet::new();
    for category in categories.values() {
        if !category.tags.iter().any(|t| {
            t.id.parse::<u64>()
                .is_ok_and(|id| config.tag_ids.contains(&id))
        }) {
            continue;
        }
        for market in &category.markets {
            if !matches!(
                market.market_type.as_deref(),
                Some("SPORTS_MONEYLINE" | "SPORTS_CHILD_MONEYLINE")
            ) {
                continue;
            }
            if market.market_type.as_deref() == Some("SPORTS_CHILD_MONEYLINE")
                && !config.include_map_winners
            {
                continue;
            }
            if market.market_type.as_deref() == Some("SPORTS_MONEYLINE")
                && !config.include_match_winners
            {
                continue;
            }
            anyhow::ensure!(
                seen.insert(market.id),
                "duplicate Predict market in catalogue"
            );
            let finished = category
                .variant_details
                .as_ref()
                .and_then(|v| v.pointer("/sports/scoreboard/finished"))
                .and_then(|v| v.as_bool())
                == Some(true);
            if market.trading_status != "OPEN" || finished || category.status != "OPEN" {
                result.rejected.push(MatchRejection {
                    predict_market_id: market.id,
                    reason: "closed_or_event_finished".into(),
                });
            } else if market.polymarket_condition_ids.len() != 1
                || !valid_condition(&market.polymarket_condition_ids[0])
            {
                result.rejected.push(MatchRejection {
                    predict_market_id: market.id,
                    reason: "missing_or_ambiguous_condition_id".into(),
                });
            } else {
                candidates.push((category, market));
            }
        }
    }
    let ids: Vec<_> = candidates
        .iter()
        .map(|(_, m)| m.polymarket_condition_ids[0].clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut targets = BTreeMap::new();
    for batch in ids.chunks(50) {
        for closed in [false, true] {
            let markets = gamma
                .get_gamma_markets(GetGammaMarketsParams {
                    condition_ids: Some(batch.to_vec()),
                    closed: Some(closed),
                    limit: Some(100),
                    ..Default::default()
                })
                .await?;
            for market in markets {
                anyhow::ensure!(
                    batch.contains(&market.condition_id),
                    "Gamma ignored condition ID filter"
                );
                let key = market.condition_id.clone();
                anyhow::ensure!(
                    targets.insert(key, market).is_none(),
                    "duplicate Gamma condition identity"
                );
            }
        }
    }
    // Binding/closure facts cover the complete catalogue, regardless of admission budgets.
    let mut phases = BTreeMap::new();
    let mut pm_volumes = BTreeMap::new();
    let mut activity_pool = BTreeSet::new();
    let mut selection_metadata = BTreeMap::new();
    for (category, market) in &candidates {
        let poly = targets
            .get(&market.polymarket_condition_ids[0])
            .context("mapped Gamma market not yet readable")?;
        anyhow::ensure!(poly.closed.is_some(), "Gamma closure state unavailable");
        match match_market(category, market, poly) {
            Ok(pair) => {
                let phase = selection_phase(category, &pair, poly);
                phases.insert(market.id, phase);
                let volume = poly
                    .volume_24hr
                    .filter(|v| *v >= rust_decimal::Decimal::ZERO);
                let pm_eligible = poly.accepting_orders == Some(true)
                    && poly.enable_order_book == Some(true)
                    && volume.is_some_and(|v| v >= config.min_volume24h_usd);
                if pm_eligible {
                    activity_pool.insert(market.id);
                    pm_volumes.insert(market.id, volume.unwrap());
                }
                let holders = category
                    .stats
                    .as_ref()
                    .and_then(|v| v.get("holdersCount"))
                    .and_then(|v| {
                        v.as_u64()
                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                    });
                selection_metadata.insert(market.id, serde_json::json!({
                    "predict_market_id":market.id,"phase":format!("{phase:?}"),
                    "polymarket_volume24h_usd":volume,
                    "predict_event_holders_count":holders,
                    "holders_scope":"Predict event/category only; not compared to PM market holders",
                    "starts_at":category.starts_at,"coarse_eligible":false,
                    "status":if pm_eligible { "outside_activity_budget" } else { "coarse_filter" }}));
                result.markets.push(pair);
            }
            Err(error) => result.rejected.push(MatchRejection {
                predict_market_id: market.id,
                reason: error.to_string(),
            }),
        }
    }
    let mut activity_scans = Vec::new();
    let activity_ids = if activity_pool.len() <= config.max_activity_candidates {
        activity_pool.iter().copied().collect::<Vec<_>>()
    } else {
        let mut ranks = BTreeMap::new();
        for tag in &config.tag_ids {
            let eligible: BTreeSet<_> = candidates
                .iter()
                .filter(|(category, market)| {
                    activity_pool.contains(&market.id)
                        && category
                            .tags
                            .iter()
                            .any(|t| t.id.parse::<u64>() == Ok(*tag))
                })
                .map(|(_, market)| market.id)
                .collect();
            // Failure only defers new admissions; it must not erase complete binding facts.
            match predict
                .request_sports_activity_ids(
                    *tag,
                    &eligible,
                    config.activity_scan_pages,
                    config.max_activity_candidates,
                )
                .await
            {
                Ok(scan) => {
                    for (rank, id) in scan.ids.iter().enumerate() {
                        ranks
                            .entry(*id)
                            .and_modify(|r: &mut usize| *r = (*r).min(rank))
                            .or_insert(rank);
                    }
                    activity_scans.push(serde_json::json!({"tag_id":tag,"pages":scan.pages,
                        "has_more":scan.has_more,"eligible_returned":scan.ids.len()}));
                }
                Err(error) => activity_scans.push(serde_json::json!({"tag_id":tag,
                    "status":"unavailable","error":format!("{error:#}")})),
            }
        }
        activity_preselection(
            &activity_pool,
            &phases,
            &ranks,
            config.max_activity_candidates,
        )
    };
    let mut statistics = BTreeMap::new();
    for batch in activity_ids.chunks(8) {
        let mut jobs = tokio::task::JoinSet::new();
        for id in batch {
            let http = predict.clone();
            let id = *id;
            jobs.spawn(async move { (id, http.request_sports_stats(id).await) });
        }
        while let Some(job) = jobs.join_next().await {
            let (id, stats) = job?;
            match stats {
                Ok(stats) => {
                    statistics.insert(id, stats);
                }
                Err(error) => {
                    let metadata = selection_metadata.get_mut(&id).unwrap();
                    metadata["status"] = serde_json::json!("statistics_unavailable");
                    metadata["check_error"] = serde_json::json!(format!("{error:#}"));
                }
            }
        }
    }
    let mut scores = BTreeMap::new();
    for (id, stats) in &statistics {
        let metadata = selection_metadata.get_mut(id).unwrap();
        let volume = pm_volumes[id];
        let eligible = stats.volume24h_usd >= config.min_volume24h_usd;
        if eligible {
            scores.insert(*id, (phases[id], stats.volume24h_usd, volume));
        }
        metadata["predict_volume24h_usd"] = serde_json::to_value(stats.volume24h_usd)?;
        metadata["coarse_eligible"] = serde_json::json!(eligible);
        metadata["status"] = serde_json::json!(if eligible {
            "outside_shortlist"
        } else {
            "coarse_filter"
        });
    }
    let shortlist: Vec<_> = rank_admissions(&scores)
        .into_iter()
        .take(config.selection_candidates)
        .collect();
    let mut checks = BTreeMap::new();
    for batch in shortlist.chunks(4) {
        let mut jobs = tokio::task::JoinSet::new();
        for id in batch {
            let pair = result
                .markets
                .iter()
                .find(|m| m.predict_market_id == *id)
                .unwrap()
                .clone();
            let http = selection.clone();
            let predict = predict.clone();
            let config = config.clone();
            jobs.spawn(async move {
                (
                    pair.predict_market_id,
                    http.check(&predict, &pair, &config).await,
                )
            });
        }
        while let Some(job) = jobs.join_next().await {
            let (id, check) = job?;
            let metadata = selection_metadata.get_mut(&id).unwrap();
            match check {
                Ok(check) => {
                    if check.qualified {
                        checks.insert(id, true);
                    }
                    metadata["status"] = serde_json::json!(if check.qualified {
                        "qualified"
                    } else {
                        "book_or_recent_trade_floor"
                    });
                    metadata["checks"] = serde_json::to_value(check)?;
                }
                Err(error) => {
                    // New admissions defer on enrichment failure; valid incumbents remain in the universe.
                    metadata["status"] = serde_json::json!("check_unavailable");
                    metadata["check_error"] = serde_json::json!(format!("{error:#}"));
                }
            }
        }
    }
    result.admission_ids = Some(
        shortlist
            .into_iter()
            .filter(|id| checks.contains_key(id))
            .collect(),
    );
    result.metadata = serde_json::json!({
        "selection":"live_first_then_predict_volume24h_then_polymarket_volume24h_with_bilateral_book_and_recent_trade_gates",
        "statistics":selection_metadata.into_values().collect::<Vec<_>>(),
        "activity_pool_size":activity_pool.len(),"statistics_requested":activity_ids.len(),
        "activity_scans":activity_scans,
        "recent_trade_scope":"Newest 50 rows per venue; distinct execution seconds are a bounded activity signal, not complete turnover or participant counts",
        "retention":"Valid incumbents are not evicted by rank or activity changes"});
    Ok(result)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SelectionPhase {
    Live,
    Other,
}

fn activity_preselection(
    pool: &BTreeSet<u64>,
    phases: &BTreeMap<u64, SelectionPhase>,
    ranks: &BTreeMap<u64, usize>,
    limit: usize,
) -> Vec<u64> {
    let mut ids: Vec<_> = pool
        .iter()
        .copied()
        .filter(|id| phases[id] == SelectionPhase::Live || ranks.contains_key(id))
        .collect();
    ids.sort_by_key(|id| {
        (
            phases[id],
            ranks.get(id).copied().unwrap_or(usize::MAX),
            *id,
        )
    });
    ids.truncate(limit);
    ids
}
fn selection_phase(
    category: &PredictSportsCategory,
    pair: &MatchedMarket,
    poly: &GammaMarket,
) -> SelectionPhase {
    let scoreboard = category
        .variant_details
        .as_ref()
        .and_then(|v| v.pointer("/sports/scoreboard"));
    let predict_live = scoreboard.and_then(|v| v["isLive"].as_bool());
    // The maintained Gamma model retains the original response, including optional sports state.
    let raw: serde_json::Value = serde_json::from_str(&poly.raw).unwrap_or_default();
    let pm_live = raw["events"].as_array().and_then(|events| {
        let states: BTreeSet<_> = events.iter().filter_map(|e| e["live"].as_bool()).collect();
        (states.len() == 1).then(|| *states.first().unwrap())
    });
    let conflict = predict_live.zip(pm_live).is_some_and(|(a, b)| a != b);
    let correct_map = pair.game_number.is_none_or(|number| {
        scoreboard.and_then(|v| v["currentGame"].as_u64()) == Some(u64::from(number))
    });
    if !conflict && correct_map && (predict_live == Some(true) || pm_live == Some(true)) {
        return SelectionPhase::Live;
    }
    SelectionPhase::Other
}

fn rank_admissions(
    scores: &BTreeMap<u64, (SelectionPhase, rust_decimal::Decimal, rust_decimal::Decimal)>,
) -> Vec<u64> {
    let mut ids: Vec<_> = scores.keys().copied().collect();
    ids.sort_by_key(|id| {
        let (phase, predict, pm) = scores[id];
        (
            phase,
            std::cmp::Reverse(predict),
            std::cmp::Reverse(pm),
            *id,
        )
    });
    ids
}

fn valid_condition(s: &str) -> bool {
    s.len() == 66 && s.starts_with("0x") && s[2..].bytes().all(|b| b.is_ascii_hexdigit())
}
fn normalized(s: &str) -> String {
    s.chars()
        .flat_map(char::to_lowercase)
        .filter(|c| c.is_alphanumeric())
        .collect()
}
fn aliases(team: &PredictSportsTeam) -> BTreeSet<String> {
    std::iter::once(team.name.as_str())
        .chain(team.abbreviation.as_deref())
        .chain(team.alias.as_deref())
        .map(normalized)
        .filter(|v| !v.is_empty())
        .collect()
}
fn map_number(question: &str) -> Option<u32> {
    let q = question.to_ascii_lowercase();
    let suffix = q.rsplit(" - ").next()?;
    let number = suffix
        .strip_prefix("map ")
        .or_else(|| suffix.strip_prefix("game "))?
        .strip_suffix(" winner")?;
    number.parse().ok().filter(|v| *v > 0 && *v <= 9)
}

fn polymarket_event_ended(category: &PredictSportsCategory, poly: &GammaMarket) -> bool {
    if category
        .variant_details
        .as_ref()
        .and_then(|v| v.pointer("/sports/scoreboard/isLive"))
        .and_then(|v| v.as_bool())
        == Some(true)
    {
        return false;
    }
    let Some(events) = poly.events.as_ref() else {
        return false;
    };
    let explicit_parent = category
        .variant_details
        .as_ref()
        .and_then(|v| v.pointer("/sports"))
        .filter(|v| v["provider"].as_str() == Some("POLYMARKET"))
        .and_then(|v| v["providerEventId"].as_str());
    let parent = explicit_parent.or_else(|| (events.len() == 1).then(|| events[0].id.as_str()));
    let Some(parent) = parent.filter(|id| events.iter().any(|e| e.id == *id)) else {
        return false;
    };
    let raw: serde_json::Value = serde_json::from_str(&poly.raw).unwrap_or_default();
    let Some(raw_events) = raw["events"].as_array() else {
        return false;
    };
    let matching: Vec<_> = raw_events
        .iter()
        .filter(|e| e["id"].as_str() == Some(parent))
        .collect();
    matching.len() == 1
        && matching[0]["ended"].as_bool() == Some(true)
        && matching[0]["live"].as_bool() != Some(true)
}

fn match_market(
    category: &PredictSportsCategory,
    predict: &PredictSportsMarket,
    poly: &GammaMarket,
) -> Result<MatchedMarket> {
    anyhow::ensure!(
        predict.polymarket_condition_ids.as_slice() == [poly.condition_id.clone()],
        "condition identity mismatch"
    );
    anyhow::ensure!(valid_condition(&poly.condition_id), "invalid condition ID");
    anyhow::ensure!(
        poly.version.as_deref() == Some("v1"),
        "unsupported Polymarket protocol version"
    );
    anyhow::ensure!(
        poly.closed == Some(false),
        "Polymarket closed or closure state unknown"
    );
    let rules = predict
        .description
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    anyhow::ensure!(
        !rules.is_empty()
            && rules
                == poly
                    .description
                    .as_deref()
                    .unwrap_or("")
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" "),
        "rules differ or missing"
    );
    let game = match predict.market_type.as_deref() {
        Some("SPORTS_MONEYLINE") => {
            anyhow::ensure!(
                poly.sports_market_type.as_deref() == Some("moneyline"),
                "market type mismatch"
            );
            None
        }
        Some("SPORTS_CHILD_MONEYLINE") => {
            anyhow::ensure!(
                poly.sports_market_type.as_deref() == Some("child_moneyline"),
                "market type mismatch"
            );
            let number = map_number(predict.question.as_deref().unwrap_or(""))
                .context("missing Predict map number")?;
            anyhow::ensure!(
                map_number(&poly.question) == Some(number),
                "map number mismatch"
            );
            if let Some(structured) = predict
                .variant_details
                .as_ref()
                .and_then(|v| v.pointer("/sports/gameNumber"))
                .and_then(|v| v.as_u64())
            {
                anyhow::ensure!(
                    structured == u64::from(number),
                    "conflicting Predict map number"
                );
            }
            anyhow::ensure!(
                category
                    .variant_details
                    .as_ref()
                    .and_then(|v| v.pointer("/sports/scoreboard/games"))
                    .and_then(|v| v.as_array())
                    .is_none_or(|games| !games
                        .iter()
                        .any(|g| g["gameNumber"].as_u64() == Some(u64::from(number))
                            && g["finished"].as_bool() == Some(true))),
                "map finished"
            );
            Some(number)
        }
        _ => anyhow::bail!("unsupported market type"),
    };
    if let Some(event_id) = category
        .variant_details
        .as_ref()
        .and_then(|v| v.pointer("/sports"))
        .filter(|v| v["provider"].as_str() == Some("POLYMARKET"))
        .and_then(|v| v["providerEventId"].as_str())
    {
        anyhow::ensure!(
            poly.events
                .as_ref()
                .is_some_and(|events| events.iter().any(|e| e.id == event_id)),
            "parent event mismatch"
        );
    }
    anyhow::ensure!(
        !polymarket_event_ended(category, poly),
        "Polymarket event ended"
    );
    let names: Vec<String> = serde_json::from_str(&poly.outcomes)?;
    let assets: Vec<String> = serde_json::from_str(&poly.clob_token_ids)?;
    anyhow::ensure!(
        names.len() == 2
            && assets.len() == 2
            && assets[0] != assets[1]
            && predict.outcomes.len() == 2,
        "expected two unique outcome assets"
    );
    let structured_teams: Vec<PredictSportsTeam> = category
        .variant_details
        .as_ref()
        .and_then(|v| v.pointer("/sports/teams"))
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()?
        .unwrap_or_default();
    let mut outcomes = Vec::new();
    let mut used = BTreeSet::new();
    for index in [1, 2] {
        let source = predict
            .outcomes
            .iter()
            .find(|o| o.index_set == index)
            .context("missing indexSet")?;
        let label = normalized(&source.name);
        anyhow::ensure!(!label.is_empty(), "empty outcome label");
        let mut labels = BTreeSet::from([label]);
        if let Some(t) = &source.team {
            labels.extend(aliases(t));
        }
        if let Some(team) = source
            .variant_details
            .as_ref()
            .and_then(|v| v.pointer("/sports/team"))
        {
            let team: PredictSportsTeam = serde_json::from_value(team.clone())?;
            labels.extend(aliases(&team));
        }
        for team in category.teams.iter().flatten().chain(&structured_teams) {
            let set = aliases(team);
            if !set.is_disjoint(&labels) {
                labels.extend(set);
            }
        }
        let matches: Vec<_> = names
            .iter()
            .enumerate()
            .filter(|(_, n)| labels.contains(&normalized(n)))
            .collect();
        anyhow::ensure!(
            matches.len() == 1 && used.insert(matches[0].0),
            "ambiguous outcome direction"
        );
        let (target, label) = matches[0];
        anyhow::ensure!(
            !assets[target].is_empty()
                && assets[target].bytes().all(|b| b.is_ascii_digit())
                && assets[target] != "0",
            "invalid outcome asset"
        );
        outcomes.push(MatchedOutcome {
            predict_index_set: index,
            predict_label: source.name.clone(),
            polymarket_label: label.clone(),
            predict_instrument: predict_outcome_instrument_id(predict.id, index)?,
            polymarket_instrument: InstrumentId::from_str(&format!(
                "{}-{}.POLYMARKET",
                poly.condition_id, assets[target]
            ))?,
        });
    }
    Ok(MatchedMarket {
        event_slug: category.slug.clone(),
        predict_market_id: predict.id,
        polymarket_condition_id: poly.condition_id.clone(),
        game_number: game,
        rules,
        outcomes: outcomes
            .try_into()
            .map_err(|_| anyhow::anyhow!("expected two bound outcomes"))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(serde::Deserialize)]
    struct Fixture {
        category: PredictSportsCategory,
        polymarket: GammaMarket,
    }
    fn fixtures() -> Vec<Fixture> {
        serde_json::from_str(include_str!("../test_data/matched_sports.json")).unwrap()
    }
    #[test]
    fn recorded_lol_football_nba_valorant_and_map_bind_by_identity() {
        let fixtures = fixtures();
        for f in &fixtures {
            let pair = match_market(&f.category, &f.category.markets[0], &f.polymarket).unwrap();
            assert_eq!(pair.polymarket_condition_id, f.polymarket.condition_id);
            assert_eq!(pair.outcomes[0].predict_index_set, 1);
            assert_eq!(pair.outcomes[1].predict_index_set, 2);
            assert_ne!(
                pair.outcomes[0].polymarket_instrument,
                pair.outcomes[1].polymarket_instrument
            );
        }
        assert_eq!(
            match_market(
                &fixtures[4].category,
                &fixtures[4].category.markets[0],
                &fixtures[4].polymarket
            )
            .unwrap()
            .game_number,
            Some(1)
        );
        // Football is an independent Yes/No proposition, including the possibility of a draw.
        let football = match_market(
            &fixtures[1].category,
            &fixtures[1].category.markets[0],
            &fixtures[1].polymarket,
        )
        .unwrap();
        assert!(
            football
                .outcomes
                .iter()
                .all(|o| ["Yes", "No"].contains(&o.predict_label.as_str()))
        );
    }
    #[test]
    fn reversed_polymarket_arrays_keep_the_same_asset_binding() {
        let f = fixtures().remove(0);
        let expected = match_market(&f.category, &f.category.markets[0], &f.polymarket).unwrap();
        let mut poly = f.polymarket;
        let mut names: Vec<String> = serde_json::from_str(&poly.outcomes).unwrap();
        let mut assets: Vec<String> = serde_json::from_str(&poly.clob_token_ids).unwrap();
        names.reverse();
        assets.reverse();
        poly.outcomes = serde_json::to_string(&names).unwrap();
        poly.clob_token_ids = serde_json::to_string(&assets).unwrap();
        assert_eq!(
            match_market(&f.category, &f.category.markets[0], &poly).unwrap(),
            expected
        );
    }
    #[test]
    fn unsafe_rules_protocol_outcome_and_map_changes_are_rejected() {
        let f = fixtures().remove(4);
        let market = &f.category.markets[0];
        let mut poly = f.polymarket.clone();
        poly.description = Some("different cancellation rules".into());
        assert!(match_market(&f.category, market, &poly).is_err());
        poly = f.polymarket.clone();
        poly.version = Some("v2".into());
        assert!(match_market(&f.category, market, &poly).is_err());
        poly = f.polymarket.clone();
        poly.question = poly.question.replace("Map 1", "Map 2");
        assert!(match_market(&f.category, market, &poly).is_err());
        poly = f.polymarket.clone();
        poly.outcomes = "[\"unknown\",\"unknown\"]".into();
        assert!(match_market(&f.category, market, &poly).is_err());
        poly = f.polymarket.clone();
        poly.closed = Some(true);
        assert!(match_market(&f.category, market, &poly).is_err());
        poly = f.polymarket.clone();
        poly.condition_id = format!("0x{}", "a".repeat(64));
        assert!(match_market(&f.category, market, &poly).is_err());
    }
    #[test]
    fn operator_profile_resolves_with_both_venues_and_no_static_ids() {
        let file: crate::config::CliConfigFile = toml::from_str(include_str!(
            "../../../examples/capture.matched-sports.toml"
        ))
        .unwrap();
        let config = crate::config::resolve_config(file).unwrap();
        assert!(config.plan.is_empty());
        let capture = config.matched_sports.unwrap();
        assert_eq!(capture.tag_ids, vec![84, 93, 96, 132, 16, 78, 45]);
        assert_eq!(capture.max_markets, 1);
        assert_eq!(MatchedSportsCaptureConfig::default().max_markets, 1);
    }
    #[test]
    fn volume_shortlist_prefers_phase_then_predict_then_pm_and_stable_id() {
        let scores = BTreeMap::from([
            (1, (SelectionPhase::Live, 100.into(), 1000.into())),
            (2, (SelectionPhase::Other, 100_000.into(), 1_000_000.into())),
            (3, (SelectionPhase::Live, 100.into(), 2000.into())),
            (4, (SelectionPhase::Live, 200.into(), 500.into())),
            (5, (SelectionPhase::Live, 100.into(), 2000.into())),
        ]);
        assert_eq!(rank_admissions(&scores), vec![4, 3, 5, 1, 2]);
    }
    #[test]
    fn activity_budget_prefers_live_then_merged_tag_rank_and_stable_identity() {
        let pool = BTreeSet::from([1, 2, 3, 4, 5]);
        let phases = BTreeMap::from([
            (1, SelectionPhase::Other),
            (2, SelectionPhase::Other),
            (3, SelectionPhase::Live),
            (4, SelectionPhase::Other),
            (5, SelectionPhase::Other),
        ]);
        let ranks = BTreeMap::from([(1, 0), (2, 0), (4, 1)]);
        assert_eq!(
            activity_preselection(&pool, &phases, &ranks, 3),
            vec![3, 1, 2]
        );
        assert_eq!(
            activity_preselection(&pool, &phases, &ranks, 10),
            vec![3, 1, 2, 4]
        );
        assert_eq!(
            activity_preselection(&pool, &phases, &BTreeMap::new(), 3),
            vec![3]
        );
    }
    #[test]
    fn only_identified_unambiguous_nonlive_pm_parent_completion_retires_a_pair() {
        let f = fixtures().remove(0);
        let mut category = f.category;
        category.variant_details = None;
        let mut poly = f.polymarket;
        let event = poly.events.as_ref().unwrap()[0].clone();
        let raw = |id: &str, ended: bool, live: bool| {
            serde_json::json!({"events":[{"id":id,"ended":ended,"live":live}]}).to_string()
        };
        poly.raw = raw(&event.id, true, false);
        assert!(
            match_market(&category, &category.markets[0], &poly)
                .unwrap_err()
                .to_string()
                .contains("event ended")
        );
        category.variant_details =
            Some(serde_json::json!({"sports":{"scoreboard":{"isLive":true}}}));
        assert!(match_market(&category, &category.markets[0], &poly).is_ok());
        category.variant_details = None;
        poly.raw = raw(&event.id, true, true);
        assert!(match_market(&category, &category.markets[0], &poly).is_ok());
        poly.raw = raw("unrelated", true, false);
        assert!(match_market(&category, &category.markets[0], &poly).is_ok());
        let mut other = event.clone();
        other.id = "other".into();
        poly.events = Some(vec![event.clone(), other]);
        poly.raw = raw(&event.id, true, false);
        assert!(!polymarket_event_ended(&category, &poly));
        category.variant_details = Some(
            serde_json::json!({"sports":{"provider":"POLYMARKET","providerEventId":event.id}}),
        );
        assert!(polymarket_event_ended(&category, &poly));
        poly.raw = "{}".into();
        assert!(!polymarket_event_ended(&category, &poly));
    }
    #[tokio::test]
    async fn broad_catalogue_keeps_all_bindings_while_bounding_and_failing_activity_enrichment() {
        use axum::{Router, http::Uri, routing::get};
        use std::sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        };
        let f = fixtures().remove(0);
        let mut category = f.category;
        category.tags = vec![nautilus_predict::http::sports::PredictSportsTag {
            id: "93".into(),
            name: "CS2".into(),
        }];
        let template = category.markets[0].clone();
        category.markets.clear();
        let mut polys = Vec::new();
        // The statistics budget is deliberately smaller than this complete catalogue.
        for id in 1..=6 {
            let mut market = template.clone();
            market.id = id;
            let condition = format!("0x{id:064x}");
            market.polymarket_condition_ids = vec![condition.clone()];
            category.markets.push(market);
            let mut poly = f.polymarket.clone();
            poly.condition_id = condition;
            poly.volume_24hr = Some(1000.into());
            polys.push(poly);
        }
        let requested = Arc::new(Mutex::new(Vec::new()));
        let fail_stats = Arc::new(AtomicBool::new(false));
        let fail_rank = Arc::new(AtomicBool::new(false));
        let ended = Arc::new(AtomicBool::new(false));
        let state = (
            requested.clone(),
            fail_stats.clone(),
            fail_rank.clone(),
            ended.clone(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let app=Router::new().fallback(get(move |uri:Uri| {
            let (category,polys,state)=(category.clone(),polys.clone(),state.clone());
            async move {
                let (requested,fail_stats,fail_rank,ended)=state;
                let url=nautilus_network::http::Url::parse(&format!("http://localhost{uri}")).unwrap();
                let q:BTreeMap<_,_>=url.query_pairs().into_owned().collect();
                match uri.path() {
                    "/v1/categories"=>serde_json::json!({"success":true,"data":[category],"cursor":null}),
                    "/markets"=> {
                        let ids:Vec<_>=url.query_pairs().filter(|(k,_)| k=="condition_ids").map(|(_,v)|v.into_owned()).collect();
                        let mut rows:Vec<_>=if q["closed"]=="true" {vec![]} else {
                            polys.into_iter().filter(|p| ids.contains(&p.condition_id)).map(|p|serde_json::to_value(p).unwrap()).collect()
                        };
                        if ended.load(Ordering::Relaxed) {
                            for row in &mut rows {
                                if row["conditionId"]==format!("0x{:064x}",6) {
                                    row["events"][0]["ended"]=serde_json::json!(true);
                                    row["events"][0]["live"]=serde_json::json!(false);
                                }
                            }
                        }
                        serde_json::json!(rows)
                    },
                    "/v1/markets"=> {
                        assert_eq!(q["sort"],"VOLUME_24H_DESC");
                        serde_json::json!({"success":!fail_rank.load(Ordering::Relaxed),"data":[{"id":2},{"id":1}],"cursor":"more"})
                    },
                    path if path.ends_with("/stats")=> {
                        let id:u64=path.split('/').nth(3).unwrap().parse().unwrap();
                        requested.lock().unwrap().push(id);
                        serde_json::json!({"success":!fail_stats.load(Ordering::Relaxed),"data":{"volume24hUsd":0,"totalLiquidityUsd":1000}})
                    },
                    _=>panic!("unexpected request: {uri}"),
                }.to_string()
            }
        }));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let predict = PredictHttpClient::new("test-key", Some(&origin), 2).unwrap();
        let gamma = PolymarketGammaRawHttpClient::new(Some(origin.clone()), 2).unwrap();
        let selection = SelectionHttp::for_test(&origin);
        let config = MatchedSportsCaptureConfig {
            tag_ids: vec![93],
            max_activity_candidates: 2,
            selection_candidates: 2,
            activity_scan_pages: 1,
            ..Default::default()
        };
        let scan = || discover(&predict, &gamma, &selection, &config, 1);
        config.validate().unwrap();
        let first = scan().await.unwrap();
        assert_eq!(first.markets.len(), 6);
        assert_eq!(first.metadata["statistics_requested"], 2);
        assert_eq!(first.metadata["activity_scans"][0]["has_more"], true);
        assert_eq!(
            requested
                .lock()
                .unwrap()
                .iter()
                .copied()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([1, 2])
        );
        assert_eq!(requested.lock().unwrap().len(), 2);
        fail_stats.store(true, Ordering::Relaxed);
        ended.store(true, Ordering::Relaxed);
        let terminal = scan().await.unwrap();
        assert_eq!(terminal.markets.len(), 5);
        assert!(!terminal.markets.iter().any(|m| m.predict_market_id == 6));
        assert!(
            terminal.markets.iter().any(|m| m.predict_market_id == 5),
            "a binding outside the shortlist must survive"
        );
        assert_eq!(terminal.admission_ids, Some(vec![]));
        assert_eq!(
            terminal.metadata["statistics"][0]["status"],
            "statistics_unavailable"
        );
        fail_rank.store(true, Ordering::Relaxed);
        let unavailable = scan().await.unwrap();
        assert_eq!(unavailable.markets, terminal.markets);
        assert_eq!(unavailable.metadata["statistics_requested"], 0);
        let maps_only = MatchedSportsCaptureConfig {
            include_match_winners: false,
            include_map_winners: true,
            ..config.clone()
        };
        let filtered = discover(&predict, &gamma, &selection, &maps_only, 1)
            .await
            .unwrap();
        assert!(filtered.markets.is_empty());
        assert_eq!(filtered.metadata["statistics_requested"], 0);
        server.abort();
    }
    #[tokio::test]
    async fn first_rest_scan_admits_active_future_market_without_waiting_or_fixed_ids() {
        use axum::{
            Router,
            http::{HeaderMap, Uri},
            routing::get,
        };
        use std::sync::atomic::{AtomicBool, Ordering};
        let f = fixtures().remove(0);
        let mut category = f.category;
        let mut poly = f.polymarket;
        category.starts_at = Some("2099-01-01T00:00:00Z".into());
        category.tags = vec![nautilus_predict::http::sports::PredictSportsTag {
            id: "84".into(),
            name: "LoL".into(),
        }];
        category.stats = Some(serde_json::json!({"holdersCount":88}));
        poly.volume_24hr = Some(1000.into());
        let id = category.markets[0].id;
        let condition = poly.condition_id.clone();
        let tokens: Vec<String> = serde_json::from_str(&poly.clob_token_ids).unwrap();
        let stale = Arc::new(AtomicBool::new(false));
        let spoof = Arc::new(AtomicBool::new(false));
        let unavailable = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));
        let state = (
            stale.clone(),
            spoof.clone(),
            unavailable.clone(),
            finished.clone(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().fallback(get(move |uri: Uri, headers: HeaderMap| {
            let (category, poly, tokens, condition) = (category.clone(), poly.clone(), tokens.clone(), condition.clone());
            let (stale, spoof, unavailable, finished) = state.clone();
            async move {
                let url = nautilus_network::http::Url::parse(&format!("http://localhost{uri}")).unwrap();
                let q: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
                if uri.path().starts_with("/v1/") { assert_eq!(headers["x-api-key"], "test-key"); }
                else { assert!(!headers.contains_key("x-api-key")); }
                let now = get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000;
                let value = match uri.path() {
                    "/v1/categories" => {
                        let mut category = category;
                        if finished.load(Ordering::Relaxed) {
                            category.variant_details = Some(serde_json::json!({"sports":{"scoreboard":{"finished":true}}}));
                        }
                        serde_json::json!({"success":true,"data":[category],"cursor":null})
                    },
                    "/markets" => if q["closed"] == "true" { serde_json::json!([]) } else { serde_json::json!([poly]) },
                    "/book" => {
                        assert!(tokens.contains(&q["token_id"]));
                        serde_json::json!({"market":condition,"asset_id":if spoof.load(Ordering::Relaxed) {"wrong"} else {&q["token_id"]},
                            "bids":[{"price":"0.49","size":"100"}],"asks":[{"price":"0.51","size":"100"}]})
                    },
                    "/v2/trades" => {
                        assert_eq!(q["condition"], condition);
                        assert_eq!(q["taker_only"], "true");
                        assert_eq!(q["limit"], "50");
                        let offset = if stale.load(Ordering::Relaxed) {1000} else {0};
                        if unavailable.load(Ordering::Relaxed) { serde_json::json!({"error":"index unavailable"}) }
                        else { serde_json::json!({"data":[{"condition_id":condition,"timestamp":now-5-offset},
                            {"condition_id":condition,"timestamp":now-25-offset}],"pagination":{"has_more":true}}) }
                    },
                    "/v1/orders/matches" => {
                        assert_eq!(q["marketId"], id.to_string());
                        assert_eq!(q["first"], "50");
                        assert!(!q.contains_key("signerAddress"));
                        let timestamp = |offset| chrono::DateTime::from_timestamp((now-offset) as i64, 0).unwrap().to_rfc3339();
                        serde_json::json!({"success":true,"cursor":"more","data":[
                            {"market":{"id":id},"executedAt":timestamp(5)},
                            {"market":{"id":id},"executedAt":timestamp(25)}]})
                    },
                    path if path.ends_with("/stats") => serde_json::json!({"success":true,"data":{"volume24hUsd":1000,"totalLiquidityUsd":1000}}),
                    path if path.ends_with("/orderbook") => serde_json::json!({"success":true,"data":{"marketId":id,"bids":[[0.49,100]],"asks":[[0.51,100]]}}),
                    _ => panic!("unexpected request: {uri}"),
                };
                value.to_string()
            }
        }));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let predict = PredictHttpClient::new("test-key", Some(&origin), 2).unwrap();
        let gamma = PolymarketGammaRawHttpClient::new(Some(origin.clone()), 2).unwrap();
        let selection = SelectionHttp::for_test(&origin);
        let config = MatchedSportsCaptureConfig::default();
        let scan = || {
            discover(
                &predict,
                &gamma,
                &selection,
                &config,
                get_atomic_clock_realtime().get_time_ns().as_u64(),
            )
        };
        let first = scan().await.unwrap();
        assert_eq!(
            first.admission_ids,
            Some(vec![id]),
            "the FIRST scan must admit existing recent activity"
        );
        assert_eq!(first.markets.len(), 1);
        assert_eq!(
            first.metadata["statistics"][0]["predict_event_holders_count"],
            88
        );
        stale.store(true, Ordering::Relaxed);
        let cold = scan().await.unwrap();
        assert_eq!(cold.admission_ids, Some(vec![]));
        assert_eq!(
            cold.markets, first.markets,
            "activity checks cannot remove incumbent eligibility"
        );
        stale.store(false, Ordering::Relaxed);
        spoof.store(true, Ordering::Relaxed);
        let mismatch = scan().await.unwrap();
        assert_eq!(mismatch.admission_ids, Some(vec![]));
        assert_eq!(
            mismatch.metadata["statistics"][0]["status"],
            "check_unavailable"
        );
        spoof.store(false, Ordering::Relaxed);
        unavailable.store(true, Ordering::Relaxed);
        let failure = scan().await.unwrap();
        assert_eq!(failure.markets, first.markets);
        assert_eq!(failure.admission_ids, Some(vec![]));
        unavailable.store(false, Ordering::Relaxed);
        finished.store(true, Ordering::Relaxed);
        let terminal = scan().await.unwrap();
        assert!(terminal.markets.is_empty());
        assert_eq!(terminal.admission_ids, Some(vec![]));
        assert!(
            terminal
                .rejected
                .iter()
                .any(|r| r.predict_market_id == id && r.reason == "closed_or_event_finished")
        );
        server.abort();
    }
    #[test]
    fn only_confirmed_live_state_gets_priority_and_map_identity_is_required() {
        let f = fixtures().remove(0);
        let mut category = f.category;
        let mut poly = f.polymarket;
        let mut pair = match_market(&category, &category.markets[0], &poly).unwrap();
        category.variant_details = None;
        poly.raw = "{}".into();
        for start in [
            Some("2099-01-01T00:00:00Z"),
            Some("2000-01-01T00:00:00Z"),
            None,
        ] {
            category.starts_at = start.map(str::to_owned);
            assert_eq!(
                selection_phase(&category, &pair, &poly),
                SelectionPhase::Other
            );
        }
        category.variant_details =
            Some(serde_json::json!({"sports":{"scoreboard":{"isLive":true,"currentGame":1}}}));
        assert_eq!(
            selection_phase(&category, &pair, &poly),
            SelectionPhase::Live
        );
        pair.game_number = Some(2);
        assert_eq!(
            selection_phase(&category, &pair, &poly),
            SelectionPhase::Other
        );
        pair.game_number = Some(1);
        assert_eq!(
            selection_phase(&category, &pair, &poly),
            SelectionPhase::Live
        );
        poly.raw = r#"{"events":[{"live":false}]}"#.into();
        assert_eq!(
            selection_phase(&category, &pair, &poly),
            SelectionPhase::Other
        );
    }
}
