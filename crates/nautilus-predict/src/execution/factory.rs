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

//! Nautilus factory for the incremental Predict execution client.

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::ExecutionClient,
    clock::Clock,
    factories::{ClientConfig, ExecutionClientFactory},
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{ClientId, TraderId},
};

use super::PredictExecutionClient;
use crate::{
    common::{PREDICT, PREDICT_VENUE},
    config::PredictExecutionClientConfig,
};

impl ClientConfig for PredictExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct PredictExecutionClientFactory;

impl PredictExecutionClientFactory {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl ExecutionClientFactory for PredictExecutionClientFactory {
    fn create(
        &self,
        trader_id: TraderId,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
        clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let config = config
            .as_any()
            .downcast_ref::<PredictExecutionClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!("Predict execution factory requires PredictExecutionClientConfig")
            })?
            .clone();
        let core = ExecutionClientCore::new(
            trader_id,
            ClientId::from(name),
            *PREDICT_VENUE,
            OmsType::Netting,
            config.account_id,
            AccountType::Cash,
            None,
            cache,
        );
        Ok(Box::new(
            PredictExecutionClient::new(core, config)?.with_reconciliation_clock(clock),
        ))
    }
    fn name(&self) -> &'static str {
        PREDICT
    }
    fn config_type(&self) -> &'static str {
        "PredictExecutionClientConfig"
    }
}
