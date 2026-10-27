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

//! Nautilus data and execution client factories.

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
#[cfg(feature = "execution")]
use nautilus_common::{clients::ExecutionClient, factories::ExecutionClientFactory};
#[cfg(feature = "execution")]
use nautilus_live::ExecutionClientCore;
use nautilus_model::identifiers::ClientId;
#[cfg(feature = "execution")]
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::TraderId,
};

use crate::{PREDICT, PredictDataClient, PredictDataClientConfig};
#[cfg(feature = "execution")]
use crate::{PredictExecutionClient, PredictExecutionClientConfig, common::PREDICT_VENUE};

impl ClientConfig for PredictDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct PredictDataClientFactory;

impl PredictDataClientFactory {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl DataClientFactory for PredictDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<PredictDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for PredictDataClientFactory. Expected PredictDataClientConfig, was {config:?}",
                )
            })?
            .clone();
        Ok(Box::new(PredictDataClient::new(
            ClientId::from(name),
            config,
        )?))
    }

    fn name(&self) -> &'static str {
        PREDICT
    }

    fn config_type(&self) -> &'static str {
        "PredictDataClientConfig"
    }
}

#[cfg(feature = "execution")]
impl ClientConfig for PredictExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(feature = "execution")]
#[derive(Debug, Clone, Default)]
pub struct PredictExecutionClientFactory;

#[cfg(feature = "execution")]
impl PredictExecutionClientFactory {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

#[cfg(feature = "execution")]
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
