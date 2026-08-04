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

use nautilus_model::enums::BookType;

use crate::plan::{
    BookDeltasCaptureSpec, CapturePlan, ForwardPriceCaptureSpec, FundingRateCaptureSpec,
    IndexPriceCaptureSpec, InstrumentCaptureSpec, InstrumentCloseCaptureSpec,
    InstrumentStatusCaptureSpec, MarkPriceCaptureSpec, OptionGreeksCaptureSpec, QuoteCaptureSpec,
    TradeCaptureSpec,
};

use super::{OptionUniverseFamily, OptionUniverseSpec, ResolvedOptionUniverse};

pub fn expand_option_universe(
    spec: &OptionUniverseSpec,
    resolved: &ResolvedOptionUniverse,
) -> CapturePlan {
    let mut plan = CapturePlan::default();

    for family in &spec.families {
        match family {
            OptionUniverseFamily::Instruments => {
                for instrument_id in &resolved.all_instrument_ids {
                    plan.instruments.push(InstrumentCaptureSpec {
                        instrument_id: *instrument_id,
                    });
                }
            }
            OptionUniverseFamily::Quotes => {
                for instrument_id in &resolved.all_instrument_ids {
                    plan.quotes.push(QuoteCaptureSpec {
                        instrument_id: *instrument_id,
                    });
                }
            }
            OptionUniverseFamily::Trades => {
                for instrument_id in &resolved.all_instrument_ids {
                    plan.trades.push(TradeCaptureSpec {
                        instrument_id: *instrument_id,
                    });
                }
            }
            OptionUniverseFamily::MarkPrices => {
                for instrument_id in &resolved.all_instrument_ids {
                    plan.mark_prices.push(MarkPriceCaptureSpec {
                        instrument_id: *instrument_id,
                    });
                }
            }
            OptionUniverseFamily::IndexPrices => {
                if let Some(instrument_id) = resolved.perp_instrument_id {
                    plan.index_prices
                        .push(IndexPriceCaptureSpec { instrument_id });
                }
            }
            OptionUniverseFamily::FundingRates => {
                if let Some(instrument_id) = resolved.perp_instrument_id {
                    plan.funding_rates
                        .push(FundingRateCaptureSpec { instrument_id });
                }
            }
            OptionUniverseFamily::InstrumentStatuses => {
                for instrument_id in &resolved.all_instrument_ids {
                    plan.instrument_statuses.push(InstrumentStatusCaptureSpec {
                        instrument_id: *instrument_id,
                    });
                }
            }
            OptionUniverseFamily::InstrumentCloses => {
                for instrument_id in &resolved.all_instrument_ids {
                    plan.instrument_closes.push(InstrumentCloseCaptureSpec {
                        instrument_id: *instrument_id,
                    });
                }
            }
            OptionUniverseFamily::OptionGreeks => {
                for instrument_id in &resolved.option_instrument_ids {
                    plan.option_greeks.push(OptionGreeksCaptureSpec {
                        instrument_id: *instrument_id,
                    });
                }
            }
            OptionUniverseFamily::ForwardPrices => {
                for instrument_id in &resolved.option_instrument_ids {
                    plan.forward_prices.push(ForwardPriceCaptureSpec {
                        instrument_id: *instrument_id,
                    });
                }
            }
            OptionUniverseFamily::BookDeltas => {
                for instrument_id in &resolved.option_instrument_ids {
                    plan.book_deltas.push(BookDeltasCaptureSpec {
                        instrument_id: *instrument_id,
                        book_type: BookType::L2_MBP,
                    });
                }
            }
        }
    }

    plan
}

pub fn merge_capture_plans(base: &CapturePlan, addition: &CapturePlan) -> CapturePlan {
    let mut merged = base.clone();
    extend_unique(
        &mut merged.instruments,
        addition.instruments.iter().cloned(),
    );
    extend_unique(&mut merged.quotes, addition.quotes.iter().cloned());
    extend_unique(&mut merged.trades, addition.trades.iter().cloned());
    extend_unique(&mut merged.bars, addition.bars.iter().cloned());
    extend_unique(
        &mut merged.book_deltas,
        addition.book_deltas.iter().cloned(),
    );
    extend_unique(
        &mut merged.mark_prices,
        addition.mark_prices.iter().cloned(),
    );
    extend_unique(
        &mut merged.index_prices,
        addition.index_prices.iter().cloned(),
    );
    extend_unique(
        &mut merged.funding_rates,
        addition.funding_rates.iter().cloned(),
    );
    extend_unique(
        &mut merged.instrument_statuses,
        addition.instrument_statuses.iter().cloned(),
    );
    extend_unique(
        &mut merged.instrument_closes,
        addition.instrument_closes.iter().cloned(),
    );
    extend_unique(
        &mut merged.option_greeks,
        addition.option_greeks.iter().cloned(),
    );
    extend_unique(
        &mut merged.forward_prices,
        addition.forward_prices.iter().cloned(),
    );
    extend_unique(
        &mut merged.custom_data,
        addition.custom_data.iter().cloned(),
    );
    extend_unique(
        &mut merged.custom_data_requests,
        addition.custom_data_requests.iter().cloned(),
    );
    merged
}

fn extend_unique<T>(target: &mut Vec<T>, items: impl IntoIterator<Item = T>)
where
    T: PartialEq,
{
    for item in items {
        if !target.contains(&item) {
            target.push(item);
        }
    }
}
