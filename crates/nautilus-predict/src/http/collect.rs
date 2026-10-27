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

//! Complete raw HTTP observations. No Native Cache access, report conversion or event publication.

use std::collections::HashSet;

use crate::http::{
    PredictAuthenticatedHttpClient,
    account_models::{PredictMatchData, PredictOrderData, PredictPage, PredictPositionData},
    query::{PredictMatchesQuery, PredictOrdersQuery, PredictPositionsQuery},
};

// The enclosing command/report bounds the whole collection and validates the session once.
// An empty page can still have a cursor. Only the venue cursor ends a complete observation.
async fn pages<T, F, Fut>(
    kind: &'static str,
    max_pages: usize,
    mut request: F,
) -> anyhow::Result<Vec<T>>
where
    F: FnMut(Option<String>) -> Fut,
    Fut: Future<Output = anyhow::Result<PredictPage<T>>>,
{
    let mut rows = Vec::new();
    let mut after = None;
    let mut seen = HashSet::new();
    for _ in 0..max_pages {
        let page = request(after.take()).await?;
        rows.extend(page.data);
        match page.cursor {
            None => return Ok(rows),
            Some(cursor) => {
                anyhow::ensure!(
                    seen.insert(cursor.clone()),
                    "Predict {kind} pagination repeated a cursor"
                );
                after = Some(cursor);
            }
        }
    }
    anyhow::bail!("Predict {kind} pagination exceeded configured page bound ({max_pages})")
}

impl PredictAuthenticatedHttpClient {
    pub(crate) async fn collect_open_orders(
        &self,
        first: u32,
        max_pages: usize,
        on_page: impl Fn(),
    ) -> anyhow::Result<Vec<PredictOrderData>> {
        pages("order", max_pages, |after| {
            on_page();
            async move {
                Ok(self
                    .request_orders(&PredictOrdersQuery {
                        first: Some(first),
                        after: after.as_deref(),
                        status: Some(crate::http::query::PredictOrderStatusFilter::Open),
                    })
                    .await?)
            }
        })
        .await
    }

    pub(crate) async fn collect_positions(
        &self,
        market_id: Option<u64>,
        first: u32,
        max_pages: usize,
    ) -> anyhow::Result<Vec<PredictPositionData>> {
        pages("position", max_pages, |after| async move {
            Ok(self
                .request_positions(&PredictPositionsQuery {
                    first: Some(first),
                    after: after.as_deref(),
                    market_id,
                    include_lost: Some(true),
                    ..Default::default()
                })
                .await?)
        })
        .await
    }

    pub(crate) async fn collect_matches(
        &self,
        market_id: Option<u64>,
        first: u32,
        max_pages: usize,
    ) -> anyhow::Result<Vec<PredictMatchData>> {
        pages("match", max_pages, |after| async move {
            let page = self
                .request_matches(&PredictMatchesQuery {
                    first: Some(first),
                    after: after.as_deref(),
                    market_id,
                    ..Default::default()
                })
                .await?;
            anyhow::ensure!(
                page.data
                    .iter()
                    .all(|row| market_id.is_none_or(|id| row.market.id == id)),
                "Predict match response differs from requested market"
            );
            Ok(page)
        })
        .await
    }
}
