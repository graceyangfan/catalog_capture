// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Settled gross hourly funding and separately identified real-time REST estimates.

use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc, time::Duration};

use anyhow::Context;
use nautilus_common::{live::sender::EventSender, messages::DataEvent};
use nautilus_core::{Params, UnixNanos, time::AtomicTime};
use nautilus_model::{
    custom_data,
    data::{CustomData, Data, DataType, FundingRateUpdate},
    identifiers::InstrumentId,
};
use nautilus_serialization::arrow_custom_data;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    common::market_from_instrument_id, http::BulletHttpClient, websocket::parse::micros_to_nanos,
};

fn funding_market(id: InstrumentId) -> anyhow::Result<String> {
    let market = market_from_instrument_id(id)?;
    anyhow::ensure!(
        !market.is_empty()
            && market
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
        "invalid Bullet funding market selector"
    );
    Ok(market)
}

pub const ESTIMATE_TYPE: &str = "BulletFundingEstimate";
const HOUR_US: u64 = 3_600_000_000;

/// Source rates/caps are eight-hour gross rates; multiplier is already applied upstream.
/// Decimal fields use Native serde storage to avoid the generic Arrow scale-16 conversion.
#[arrow_custom_data]
#[custom_data]
pub struct BulletFundingEstimate {
    pub instrument_id: InstrumentId,
    #[custom_data_field(serde)]
    pub estimated_rate_8h: Decimal,
    #[custom_data_field(serde)]
    pub reported_last_rate_8h: Decimal,
    /// Settlement cadence; the source rates themselves retain their eight-hour basis.
    pub interval_minutes: u64,
    pub next_funding_ns: UnixNanos,
    #[custom_data_field(serde)]
    pub cap_8h: Decimal,
    #[custom_data_field(serde)]
    pub floor_8h: Decimal,
    #[custom_data_field(serde)]
    pub funding_multiplier: Decimal,
    #[custom_data_field(serde)]
    pub funding_spread_rate: Decimal,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}
pub fn register_bullet_custom_data() {
    nautilus_serialization::ensure_custom_data_registered::<BulletFundingEstimate>();
}
pub fn estimate_data_type(id: InstrumentId) -> DataType {
    DataType::new(
        ESTIMATE_TYPE,
        Some(Params::new(
            [(
                "instrument_id".to_string(),
                serde_json::json!(id.to_string()),
            )]
            .into(),
        )),
        Some(id.to_string()),
    )
}
pub fn estimate_instrument(data_type: &DataType) -> anyhow::Result<InstrumentId> {
    anyhow::ensure!(
        data_type.type_name() == ESTIMATE_TYPE,
        "unsupported Bullet custom type"
    );
    let id: InstrumentId = data_type
        .metadata()
        .as_ref()
        .and_then(|p| p.get("instrument_id"))
        .and_then(serde_json::Value::as_str)
        .context("BulletFundingEstimate requires metadata.instrument_id")?
        .parse()?;
    funding_market(id)?;
    anyhow::ensure!(
        *data_type == estimate_data_type(id),
        "BulletFundingEstimate requires canonical identifier and metadata"
    );
    Ok(id)
}
impl BulletFundingEstimate {
    pub fn into_custom_data(self) -> CustomData {
        let data_type = estimate_data_type(self.instrument_id);
        CustomData::new(Arc::new(self), data_type)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FundingInfo {
    pub symbol: String,
    pub adjusted_funding_rate_cap: String,
    pub adjusted_funding_rate_floor: String,
    pub funding_interval_hours: u16,
    pub funding_multiplier: String,
    pub funding_spread_rate: String,
}
impl FundingInfo {
    fn validate(&self, market: &str) -> anyhow::Result<()> {
        anyhow::ensure!(self.symbol == market, "Bullet funding info symbol mismatch");
        // The venue documents hourly settlement at rate/8; do not extrapolate other cadences.
        anyhow::ensure!(
            self.funding_interval_hours == 1,
            "Bullet funding currently requires hourly settlement"
        );
        let cap = decimal(&self.adjusted_funding_rate_cap)?;
        let floor = decimal(&self.adjusted_funding_rate_floor)?;
        anyhow::ensure!(floor <= cap, "Bullet funding floor exceeds cap");
        decimal(&self.funding_multiplier)?;
        decimal(&self.funding_spread_rate)?;
        Ok(())
    }
}
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FundingRow {
    pub symbol: String,
    pub funding_rate: String,
    pub funding_time: i64,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PremiumIndex {
    pub symbol: String,
    pub last_funding_rate: String,
    pub estimated_funding_rate: String,
    pub next_funding_time: i64,
    pub time: i64,
}
fn decimal(value: &str) -> anyhow::Result<Decimal> {
    Decimal::from_str_exact(value).context("invalid exact Bullet funding decimal")
}
fn hourly(rate: Decimal) -> anyhow::Result<Decimal> {
    rate.checked_div(Decimal::from(8))
        .filter(|hourly| hourly.checked_mul(Decimal::from(8)) == Some(rate))
        .context("Bullet hourly funding rate is not exactly representable")
}
fn positive_us(value: i64) -> anyhow::Result<UnixNanos> {
    anyhow::ensure!(value > 0, "nonpositive Bullet funding timestamp");
    micros_to_nanos(u64::try_from(value)?)
}
fn positive_ms(value: i64) -> anyhow::Result<UnixNanos> {
    anyhow::ensure!(value > 0, "nonpositive Bullet premium timestamp");
    micros_to_nanos(
        u64::try_from(value)?
            .checked_mul(1000)
            .context("Bullet milliseconds overflow")?,
    )
}
fn parse_info(bytes: &[u8], market: &str) -> anyhow::Result<FundingInfo> {
    let mut rows: Vec<FundingInfo> = serde_json::from_slice(bytes)?;
    anyhow::ensure!(
        rows.len() == 1,
        "Bullet fundingInfo expected exactly one market"
    );
    let info = rows.remove(0);
    info.validate(market)?;
    Ok(info)
}
pub fn parse_settlements(
    bytes: &[u8],
    id: InstrumentId,
    info: &FundingInfo,
    ts_init: UnixNanos,
) -> anyhow::Result<Vec<FundingRateUpdate>> {
    let market = funding_market(id)?;
    info.validate(&market)?;
    let rows: Vec<FundingRow> = serde_json::from_slice(bytes)?;
    let mut last = None;
    rows.into_iter()
        .map(|r| {
            anyhow::ensure!(r.symbol == market, "Bullet settled funding symbol mismatch");
            let time = u64::try_from(r.funding_time).context("negative Bullet settlement time")?;
            anyhow::ensure!(
                time > 0 && time % HOUR_US == 0,
                "Bullet settlement timestamp must be an hourly microsecond boundary"
            );
            anyhow::ensure!(
                last.is_none_or(|previous| time > previous),
                "Bullet funding history is not strictly increasing"
            );
            last = Some(time);
            Ok(FundingRateUpdate::new(
                id,
                hourly(decimal(&r.funding_rate)?)?,
                Some(60),
                None,
                positive_us(r.funding_time)?,
                ts_init,
            ))
        })
        .collect()
}
pub fn parse_estimate(
    bytes: &[u8],
    id: InstrumentId,
    info: &FundingInfo,
    ts_init: UnixNanos,
) -> anyhow::Result<BulletFundingEstimate> {
    let market = funding_market(id)?;
    info.validate(&market)?;
    let mut rows: Vec<PremiumIndex> = serde_json::from_slice(bytes)?;
    anyhow::ensure!(
        rows.len() == 1,
        "Bullet premiumIndex expected exactly one market"
    );
    let r = rows.remove(0);
    anyhow::ensure!(
        r.symbol == market,
        "Bullet funding estimate symbol mismatch"
    );
    let estimated = decimal(&r.estimated_funding_rate)?;
    Ok(BulletFundingEstimate {
        instrument_id: id,
        estimated_rate_8h: estimated,
        reported_last_rate_8h: decimal(&r.last_funding_rate)?,
        interval_minutes: 60,
        next_funding_ns: positive_ms(r.next_funding_time)?,
        cap_8h: decimal(&info.adjusted_funding_rate_cap)?,
        floor_8h: decimal(&info.adjusted_funding_rate_floor)?,
        funding_multiplier: decimal(&info.funding_multiplier)?,
        funding_spread_rate: decimal(&info.funding_spread_rate)?,
        ts_event: positive_ms(r.time)?,
        ts_init,
    })
}
impl BulletHttpClient {
    async fn funding_info(&self, id: InstrumentId) -> anyhow::Result<FundingInfo> {
        let market = funding_market(id)?;
        let bytes = self
            .request_public(&format!("/fapi/v1/fundingInfo?symbol={market}"))
            .await?;
        parse_info(&bytes, &market)
    }
    pub async fn request_funding_estimate(
        &self,
        id: InstrumentId,
        clock: &AtomicTime,
    ) -> anyhow::Result<BulletFundingEstimate> {
        let info = self.funding_info(id).await?;
        let market = funding_market(id)?;
        let bytes = self
            .request_public(&format!("/fapi/v1/premiumIndex?symbol={market}"))
            .await?;
        parse_estimate(&bytes, id, &info, clock.get_time_ns())
    }
    async fn request_settlements(
        &self,
        id: InstrumentId,
        after: Option<u64>,
        clock: &AtomicTime,
    ) -> anyhow::Result<Vec<FundingRateUpdate>> {
        let info = self.funding_info(id).await?;
        let market = funding_market(id)?;
        let suffix = match after {
            Some(time) => format!(
                "&startTime={}&limit=1000",
                time.checked_add(1)
                    .context("Bullet funding cursor overflow")?
            ),
            None => "&limit=1".to_string(),
        };
        let bytes = self
            .request_public(&format!("/fapi/v1/fundingRate?symbol={market}{suffix}"))
            .await?;
        let rows = parse_settlements(&bytes, id, &info, clock.get_time_ns())?;
        anyhow::ensure!(
            rows.iter()
                .all(|row| after.is_none_or(|time| row.ts_event.as_u64() / 1000 > time)),
            "Bullet funding history ignored its startTime cursor"
        );
        Ok(rows)
    }
}

#[derive(Debug)]
pub(crate) enum FundingCommand {
    Subscribe(InstrumentId),
    Unsubscribe(InstrumentId),
}
#[derive(Debug)]
struct FundingOwner {
    references: usize,
    generation: u64,
    last_us: Option<u64>,
}
#[derive(Debug, Default)]
struct FundingState {
    owners: BTreeMap<InstrumentId, FundingOwner>,
    next_generation: u64,
}
impl FundingState {
    fn command(&mut self, command: FundingCommand) -> anyhow::Result<()> {
        match command {
            FundingCommand::Subscribe(id) => {
                if let Some(owner) = self.owners.get_mut(&id) {
                    owner.references = owner
                        .references
                        .checked_add(1)
                        .context("Bullet funding refcount overflow")?;
                } else {
                    self.next_generation = self
                        .next_generation
                        .checked_add(1)
                        .context("Bullet funding generation overflow")?;
                    self.owners.insert(
                        id,
                        FundingOwner {
                            references: 1,
                            generation: self.next_generation,
                            last_us: None,
                        },
                    );
                }
            }
            FundingCommand::Unsubscribe(id) => {
                if let Some(owner) = self.owners.get_mut(&id) {
                    owner.references -= 1;
                    if owner.references == 0 {
                        self.owners.remove(&id);
                    }
                }
            }
        }
        Ok(())
    }
    fn apply(
        &mut self,
        id: InstrumentId,
        generation: u64,
        rows: Vec<FundingRateUpdate>,
        sender: &EventSender<DataEvent>,
    ) -> anyhow::Result<()> {
        let Some(owner) = self
            .owners
            .get_mut(&id)
            .filter(|o| o.generation == generation)
        else {
            return Ok(());
        };
        for row in rows {
            let time = row.ts_event.as_u64() / 1000;
            anyhow::ensure!(
                row.instrument_id == id,
                "Bullet funding result instrument mismatch"
            );
            if owner.last_us.is_some_and(|previous| time <= previous) {
                continue;
            }
            sender.send(DataEvent::Data(Data::FundingRate(row)))?;
            owner.last_us = Some(time);
        }
        Ok(())
    }
}
type FundingResult = Vec<(InstrumentId, u64, anyhow::Result<Vec<FundingRateUpdate>>)>;
type FundingPoll = Pin<Box<dyn Future<Output = FundingResult> + Send>>;
pub(crate) async fn run_funding_worker(
    http: BulletHttpClient,
    mut commands: mpsc::Receiver<FundingCommand>,
    sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
    cancel: CancellationToken,
    interval: Duration,
) {
    let mut state = FundingState::default();
    let mut poll: Option<FundingPoll> = None;
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            command = commands.recv() => {
                let Some(command) = command else { break };
                if let Err(error) = state.command(command) {
                    log::error!("Bullet funding command failed: {error:#}");
                }
            }
            results = async {
                match &mut poll {
                    Some(poll) => poll.await,
                    None => std::future::pending().await,
                }
            } => {
                poll = None;
                for (id, generation, result) in results {
                    match result {
                        Ok(rows) => {
                            if let Err(error) = state.apply(id, generation, rows, &sender) {
                                log::error!("Bullet funding delivery failed: {error:#}");
                            }
                        }
                        Err(error) => log::error!("Bullet settled funding poll failed for {id}: {error:#}; cursor retained"),
                    }
                }
            }
            _ = tick.tick(), if poll.is_none() => {
                let targets = state.owners.iter()
                    .map(|(id, owner)| (*id, owner.generation, owner.last_us))
                    .collect::<Vec<_>>();
                if !targets.is_empty() {
                    let http = http.clone();
                    poll = Some(Box::pin(async move {
                        let mut results = Vec::new();
                        for (id, generation, after) in targets {
                            results.push((id, generation, http.request_settlements(id, after, clock).await));
                        }
                        results
                    }));
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "funding_tests.rs"]
mod tests;
