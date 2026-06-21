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

const ONE_BILLION: u64 = 1_000_000_000;

/// Adaptive HIP-4 rotation poll delay in seconds (mirrors `hyperliquid_stale_quote/rotation.py`).
#[must_use]
pub fn next_rotation_delay_secs(
    now_ns: u64,
    expiration_ns: Option<u64>,
    idle_poll_secs: u64,
    active_poll_secs: u64,
    pre_expiry_window_secs: u64,
) -> u64 {
    let idle_secs = idle_poll_secs.max(1);
    let active_secs = active_poll_secs.max(1);
    let pre_window_ns = pre_expiry_window_secs.saturating_mul(ONE_BILLION);

    let Some(expiration_ns) = expiration_ns.filter(|value| *value > 0) else {
        return active_secs;
    };

    let fast_window_start_ns = expiration_ns.saturating_sub(pre_window_ns);
    if now_ns < fast_window_start_ns {
        let secs_until_fast_window = ((fast_window_start_ns - now_ns) / ONE_BILLION).max(1);
        return idle_secs.min(secs_until_fast_window);
    }

    active_secs
}

#[cfg(test)]
mod tests {
    use super::{next_rotation_delay_secs, ONE_BILLION};

    #[test]
    fn uses_idle_poll_when_far_from_expiry() {
        let delay = next_rotation_delay_secs(0, Some(7_200 * ONE_BILLION), 1800, 10, 900);
        assert_eq!(delay, 1800);
    }

    #[test]
    fn caps_idle_poll_to_fast_window_start() {
        let delay = next_rotation_delay_secs(
            1_000 * ONE_BILLION,
            Some(1_500 * ONE_BILLION),
            1800,
            10,
            300,
        );
        assert_eq!(delay, 200);
    }

    #[test]
    fn uses_active_poll_near_and_after_expiry() {
        let near = next_rotation_delay_secs(
            1_250 * ONE_BILLION,
            Some(1_500 * ONE_BILLION),
            1800,
            10,
            300,
        );
        let post = next_rotation_delay_secs(
            1_620 * ONE_BILLION,
            Some(1_500 * ONE_BILLION),
            1800,
            10,
            300,
        );
        assert_eq!(near, 10);
        assert_eq!(post, 10);
    }

    #[test]
    fn uses_active_poll_when_expiry_unknown() {
        let delay = next_rotation_delay_secs(0, None, 1800, 10, 900);
        assert_eq!(delay, 10);
    }
}
