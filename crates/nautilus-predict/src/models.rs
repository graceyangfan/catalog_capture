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

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct PredictResponse<T> {
    pub success: bool,
    pub data: T,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PredictListResponse<T> {
    pub success: bool,
    pub cursor: Option<String>,
    pub data: Vec<T>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PredictSearchResponse {
    pub success: bool,
    pub data: PredictSearchData,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PredictSearchData {
    #[serde(default)]
    pub markets: Vec<PredictMarket>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PredictCategoryResponse {
    pub success: bool,
    pub data: PredictCategory,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PredictCategory {
    #[serde(default)]
    pub markets: Vec<PredictMarket>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictMarket {
    pub id: u64,
    pub title: String,
    pub decimal_precision: u8,
    pub outcomes: Vec<PredictOutcome>,
    pub created_at: Option<String>,
    pub variant_data: Option<PredictVariantData>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictVariantData {
    #[serde(rename = "type")]
    pub variant_type: String,
    pub price_feed_symbol: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictOutcome {
    pub name: String,
    pub index_set: u8,
    pub on_chain_id: String,
}
