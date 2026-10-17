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

//! Private socket failures retain classifications, never JWT topics or server error text.

use serde::Deserialize;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, PredictWalletWsError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredictWalletResponseErrorCode {
    InvalidJson,
    InvalidTopic,
    InvalidCredentials,
    InternalServerError,
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum PredictWalletWsError {
    #[error("invalid wallet socket configuration: {0}")]
    Configuration(&'static str),
    #[error("wallet WebSocket transport failed")]
    Transport,
    #[error("wallet WebSocket connect timed out")]
    ConnectTimeout,
    #[error("wallet WebSocket handshake rejected: HTTP {0}")]
    HandshakeRejected(u16),
    #[error("wallet subscription timed out")]
    SubscriptionTimeout,
    #[error("wallet subscription rejected: {0:?}")]
    SubscriptionRejected(PredictWalletResponseErrorCode),
    #[error("invalid wallet socket payload")]
    Decode,
    #[error("wallet event violates protocol invariants")]
    Protocol,
    #[error("wallet event belongs to another account")]
    AccountMismatch,
    #[error("unsupported wallet event requires reconciliation")]
    UnsupportedEvent,
    #[error("wallet event queue overflow requires reconciliation")]
    QueueOverflow,
    #[error("wallet socket closed")]
    Closed,
    #[error("wallet socket task failed")]
    Task,
}
