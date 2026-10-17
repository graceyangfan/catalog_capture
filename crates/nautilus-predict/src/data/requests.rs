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

//! Current venue instrument definitions through the standard Native request/response path.

use anyhow::Context;
use nautilus_common::messages::{
    DataEvent, DataResponse,
    data::{InstrumentResponse, RequestInstrument},
};
use nautilus_core::datetime::datetime_to_unix_nanos;
use nautilus_model::instruments::Instrument;

use super::client::PredictDataClient;
use crate::providers::{instruments_from_market, parse_market_id};

impl PredictDataClient {
    pub(super) fn request_instrument_definition(
        &self,
        request: RequestInstrument,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            request.client_id.is_none_or(|id| id == self.client_id),
            "Predict instrument request targets another client"
        );
        let market_id = parse_market_id(&request.instrument_id)?;
        if request.start.is_some() || request.end.is_some() {
            log::warn!(
                "Predict supplies current instrument definitions only; request dates have no effect"
            );
        }
        let http = self.provider.http_client().clone();
        let sender = self.data_sender.clone();
        let client_id = self.client_id;
        let clock = self.clock;
        let cancellation = self.tasks.cancellation_token();
        self.tasks
            .spawn_named("predict-request-instrument", async move {
                let result = async {
                    let market = http.request_market(market_id).await?;
                    let (_, instruments) = instruments_from_market(&market, clock.get_time_ns())?;
                    let instrument = instruments
                        .into_iter()
                        .find(|instrument| instrument.id() == request.instrument_id)
                        .context("Predict market lacks the requested outcome instrument")?;
                    // Definition publication updates other venue clients; the separate response
                    // completes the initiating actor's request, as in Polymarket.
                    sender.send(DataEvent::Instrument(instrument.clone()))?;
                    sender.send(DataEvent::Response(DataResponse::Instrument(Box::new(
                        InstrumentResponse::new(
                            request.request_id,
                            client_id,
                            request.instrument_id,
                            instrument,
                            datetime_to_unix_nanos(request.start),
                            datetime_to_unix_nanos(request.end),
                            clock.get_time_ns(),
                            request.params,
                        ),
                    ))))?;
                    Ok::<_, anyhow::Error>(())
                };
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => {},
                    result = result => {
                        if let Err(error) = result {
                            log::error!("Predict instrument request failed: {error:#}");
                        }
                    },
                }
            })?;
        Ok(())
    }
}
