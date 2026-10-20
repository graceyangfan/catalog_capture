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

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{config::LighterBinaryDataClientConfig, data::LighterBinaryDataClient};

impl ClientConfig for LighterBinaryDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct LighterBinaryDataClientFactory;

impl LighterBinaryDataClientFactory {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl DataClientFactory for LighterBinaryDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<LighterBinaryDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for LighterBinaryDataClientFactory. Expected LighterBinaryDataClientConfig, was {config:?}",
                )
            })?
            .clone();
        Ok(Box::new(LighterBinaryDataClient::new(
            ClientId::from(name),
            config,
        )?))
    }

    fn name(&self) -> &'static str {
        "LIGHTER"
    }

    fn config_type(&self) -> &'static str {
        "LighterBinaryDataClientConfig"
    }
}
