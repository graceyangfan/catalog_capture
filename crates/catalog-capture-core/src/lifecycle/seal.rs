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

use anyhow::{Context, Result};
use chrono::{DateTime, LocalResult, NaiveDate, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

const NANOSECONDS_IN_SECOND: u64 = 1_000_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSealSchedule {
    pub schedule_ns: u64,
    pub timezone: Tz,
    pub interval_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SealConfigFile {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_seal_schedule")]
    pub schedule: String,
    #[serde(default = "default_seal_timezone")]
    pub timezone: String,
    #[serde(default = "default_seal_interval_secs")]
    pub interval_secs: u64,
}

impl Default for SealConfigFile {
    fn default() -> Self {
        Self {
            enabled: false,
            schedule: default_seal_schedule(),
            timezone: default_seal_timezone(),
            interval_secs: default_seal_interval_secs(),
        }
    }
}

pub fn parse_seal_schedule(value: &str) -> Result<u64> {
    let trimmed = value.trim();
    let parsed = NaiveTime::parse_from_str(trimmed, "%H:%M")
        .or_else(|_| NaiveTime::parse_from_str(trimmed, "%H:%M:%S"))
        .with_context(|| format!("invalid seal schedule {value}; expected HH:MM or HH:MM:SS"))?;
    let secs = parsed.num_seconds_from_midnight() as u64;
    let nanos = parsed.nanosecond() as u64;
    Ok(secs
        .saturating_mul(NANOSECONDS_IN_SECOND)
        .saturating_add(nanos))
}

pub fn parse_seal_timezone(value: &str) -> Result<Tz> {
    value
        .trim()
        .parse::<Tz>()
        .with_context(|| format!("invalid seal timezone {value}"))
}

pub fn resolve_seal_schedule(config: &SealConfigFile) -> Result<ResolvedSealSchedule> {
    Ok(ResolvedSealSchedule {
        schedule_ns: parse_seal_schedule(&config.schedule)?,
        timezone: parse_seal_timezone(&config.timezone)?,
        interval_ns: config
            .interval_secs
            .max(1)
            .saturating_mul(NANOSECONDS_IN_SECOND),
    })
}

/// Next seal boundary strictly after `now_ns`.
#[must_use]
pub fn next_seal_boundary_ns(now_ns: u64, seal: &ResolvedSealSchedule) -> u64 {
    let now_tz = utc_datetime_from_ns(now_ns).with_timezone(&seal.timezone);
    let mut next = next_boundary_datetime(now_tz, seal);
    while datetime_to_ns(&next) <= now_ns {
        next = add_interval(next, seal.interval_ns);
    }
    datetime_to_ns(&next)
}

#[must_use]
pub fn should_seal_at(now_ns: u64, next_seal_ns: u64) -> bool {
    now_ns >= next_seal_ns
}

fn next_boundary_datetime(now_tz: DateTime<Tz>, seal: &ResolvedSealSchedule) -> DateTime<Tz> {
    let schedule = schedule_time_naive(seal.schedule_ns);
    let mut next = resolve_local_schedule_datetime(seal.timezone, now_tz.date_naive(), schedule);

    if next <= now_tz {
        while next <= now_tz {
            next = add_interval(next, seal.interval_ns);
        }
    }

    next
}

fn resolve_local_schedule_datetime(
    timezone: Tz,
    date: NaiveDate,
    schedule: NaiveTime,
) -> DateTime<Tz> {
    match timezone.from_local_datetime(&date.and_time(schedule)) {
        LocalResult::Single(value) => value,
        LocalResult::Ambiguous(earlier, _) => earlier,
        LocalResult::None => timezone
            .from_local_datetime(&(date + chrono::Duration::days(1)).and_time(schedule))
            .single()
            .unwrap_or_else(|| timezone.from_utc_datetime(&date.and_time(schedule))),
    }
}

fn schedule_time_naive(schedule_ns: u64) -> NaiveTime {
    let secs = u32::try_from(schedule_ns / NANOSECONDS_IN_SECOND).unwrap_or(0);
    let nanos = u32::try_from(schedule_ns % NANOSECONDS_IN_SECOND).unwrap_or(0);
    NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
        .unwrap_or_else(|| NaiveTime::from_hms_opt(0, 0, 0).expect("midnight"))
}

fn utc_datetime_from_ns(now_ns: u64) -> DateTime<Utc> {
    let secs = i64::try_from(now_ns / NANOSECONDS_IN_SECOND).unwrap_or(i64::MAX);
    let nanos = u32::try_from(now_ns % NANOSECONDS_IN_SECOND).unwrap_or(0);
    Utc.timestamp_opt(secs, nanos)
        .single()
        .unwrap_or_else(Utc::now)
}

fn datetime_to_ns(value: &DateTime<Tz>) -> u64 {
    let utc = value.with_timezone(&Utc);
    let timestamp_ns = utc.timestamp_nanos_opt().unwrap_or(0);
    u64::try_from(timestamp_ns.max(0)).unwrap_or(0)
}

fn add_interval(value: DateTime<Tz>, interval_ns: u64) -> DateTime<Tz> {
    let secs = i64::try_from(interval_ns / NANOSECONDS_IN_SECOND).unwrap_or(i64::MAX);
    let nanos = i32::try_from(interval_ns % NANOSECONDS_IN_SECOND).unwrap_or(0);
    value + chrono::Duration::seconds(secs) + chrono::Duration::nanoseconds(i64::from(nanos))
}

fn default_seal_schedule() -> String {
    "06:00".to_string()
}

fn default_seal_timezone() -> String {
    "UTC".to_string()
}

const fn default_seal_interval_secs() -> u64 {
    86_400
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use chrono_tz::UTC;

    use super::{
        next_seal_boundary_ns, parse_seal_schedule, resolve_seal_schedule, should_seal_at,
        SealConfigFile, NANOSECONDS_IN_SECOND,
    };

    fn utc_ns(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> u64 {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .single()
            .expect("valid timestamp")
            .timestamp_nanos_opt()
            .expect("timestamp fits in ns") as u64
    }

    fn daily_seal_at_six_utc() -> super::ResolvedSealSchedule {
        resolve_seal_schedule(&SealConfigFile {
            enabled: true,
            schedule: "06:00".to_string(),
            timezone: "UTC".to_string(),
            interval_secs: 86_400,
        })
        .expect("seal schedule should resolve")
    }

    #[test]
    fn parse_seal_schedule_accepts_hour_minute() {
        assert_eq!(
            parse_seal_schedule("06:00").expect("should parse"),
            6 * 3_600 * NANOSECONDS_IN_SECOND
        );
    }

    #[test]
    fn next_seal_boundary_targets_following_day_boundary() {
        let seal = daily_seal_at_six_utc();
        let next = next_seal_boundary_ns(utc_ns(2026, 6, 22, 5, 30), &seal);
        assert_eq!(next, utc_ns(2026, 6, 22, 6, 0));
    }

    #[test]
    fn seal_timezone_parses_utc() {
        let seal = daily_seal_at_six_utc();
        assert_eq!(seal.timezone, UTC);
    }

    #[test]
    fn next_seal_boundary_handles_us_spring_forward_gap() {
        use chrono_tz::America::New_York;

        let seal = resolve_seal_schedule(&SealConfigFile {
            enabled: true,
            schedule: "02:30".to_string(),
            timezone: "America/New_York".to_string(),
            interval_secs: 86_400,
        })
        .expect("seal schedule should resolve");
        assert_eq!(seal.timezone, New_York);

        // 2026-03-08 01:30 EST: 02:30 does not exist on spring-forward day.
        let now = utc_ns(2026, 3, 8, 6, 30);
        let next = next_seal_boundary_ns(now, &seal);
        assert!(
            next > now,
            "next seal boundary must be strictly after now (got now={now}, next={next})"
        );
    }

    #[test]
    fn next_seal_boundary_handles_us_fall_back_ambiguity() {
        let seal = resolve_seal_schedule(&SealConfigFile {
            enabled: true,
            schedule: "01:30".to_string(),
            timezone: "America/New_York".to_string(),
            interval_secs: 86_400,
        })
        .expect("seal schedule should resolve");

        // 2026-11-01 05:30 UTC ~= 01:30 EDT during fall-back ambiguity window.
        let now = utc_ns(2026, 11, 1, 5, 30);
        let next = next_seal_boundary_ns(now, &seal);
        assert!(
            next > now,
            "ambiguous local schedule should still yield a future boundary"
        );
    }

    #[test]
    fn should_seal_at_triggers_only_on_boundary_crossing() {
        let seal = daily_seal_at_six_utc();
        let boundary = next_seal_boundary_ns(utc_ns(2026, 6, 22, 5, 30), &seal);
        assert!(!should_seal_at(boundary.saturating_sub(1), boundary));
        assert!(should_seal_at(boundary, boundary));
    }
}
