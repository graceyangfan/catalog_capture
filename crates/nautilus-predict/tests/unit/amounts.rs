// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Numeric conversion must preserve the previous exact string decoder's domain and scale.
use super::*;

fn compare(value: U256) {
    let divisor = U256::from(1_000_000_000_000_000_000u64);
    let text = format!("{}.{:018}", value / divisor, (value % divisor).to::<u64>());
    let reference = Decimal::from_str_exact(text.trim_end_matches('0').trim_end_matches('.'));
    match (exact_wei(value), reference) {
        (Ok(actual), Ok(expected)) => {
            assert_eq!(actual, expected, "{value}");
            assert_eq!(actual.scale(), expected.scale(), "{value}");
            assert_eq!(decimal_to_wei(actual).unwrap(), value);
        }
        (Err(_), Err(_)) => {}
        result => panic!("conversion domain changed for {value}: {result:?}"),
    }
}

#[test]
fn exact_wei_preserves_decimal_boundaries_and_all_atomic_scales() {
    for value in 0..=1000u64 {
        compare(U256::from(value));
    }
    for power in 0..=77 {
        let value = U256::from(10).pow(U256::from(power));
        for offset in [U256::ZERO, U256::from(1)] {
            compare(value + offset);
            compare(value - offset);
        }
    }
    let limit = (U256::from(1) << 96) - U256::from(1);
    for scale in 0..=18 {
        let multiplier = U256::from(10).pow(U256::from(scale));
        for mantissa in [limit - U256::from(1), limit, limit + U256::from(1)] {
            for offset in [U256::ZERO, U256::from(1)] {
                compare(mantissa * multiplier + offset);
            }
        }
    }
    compare(U256::from(u128::MAX));
    compare(U256::MAX);
}

#[test]
fn exact_wei_matches_string_decoder_for_deterministic_amounts() {
    let mut seed = 1u128;
    for index in 0..10000 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        compare(U256::from(seed));
        compare(U256::from(seed % (1u128 << 96)) * U256::from(10u64.pow(index % 19)));
        compare(U256::from_limbs([
            seed as u64,
            (seed >> 64) as u64,
            index.into(),
            1,
        ]));
    }
}
