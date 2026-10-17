// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Shared LiveNode wiring for loopback replay and explicit read-only mainnet strategy runs.
use nautilus_common::enums::Environment;
use nautilus_live::node::{LiveNode, config::LiveExecutionEngineConfig};
use nautilus_model::identifiers::TraderId;
use nautilus_predict::{
    PredictDataClientConfig, PredictDataClientFactory, PredictExecutionClientConfig,
    PredictExecutionClientFactory,
};

pub fn build_node(
    trader_id: TraderId,
    data: PredictDataClientConfig,
    execution: PredictExecutionClientConfig,
) -> anyhow::Result<LiveNode> {
    build_node_with_engine(
        trader_id,
        data,
        execution,
        LiveExecutionEngineConfig {
            reconciliation: false,
            inflight_check_interval_ms: 0,
            open_check_interval_secs: None,
            position_check_interval_secs: None,
            ..Default::default()
        },
    )
}

pub fn build_node_with_engine(
    trader_id: TraderId,
    data: PredictDataClientConfig,
    execution: PredictExecutionClientConfig,
    engine: LiveExecutionEngineConfig,
) -> anyhow::Result<LiveNode> {
    build_node_with_engine_and_stop_delay(trader_id, data, execution, engine, 0)
}

/// Explicit grace period lets native strategy on_stop cancellations drain before disconnect.
pub fn build_node_with_engine_and_stop_delay(
    trader_id: TraderId,
    data: PredictDataClientConfig,
    execution: PredictExecutionClientConfig,
    engine: LiveExecutionEngineConfig,
    delay_secs: u64,
) -> anyhow::Result<LiveNode> {
    LiveNode::builder(trader_id, Environment::Live)?
        .with_name("PredictRustStrategyCalibration")
        // Tests select native reconciliation explicitly; current Predict reports require known ownership.
        .with_exec_engine_config(engine)
        .with_timeout_connection(20)
        .with_timeout_portfolio(20)
        .with_delay_post_stop_secs(delay_secs)
        .with_timeout_disconnection_secs(5)
        .add_data_client(
            Some("PREDICT".into()),
            Box::new(PredictDataClientFactory::new()),
            Box::new(data),
        )?
        .add_exec_client(
            Some("PREDICT".into()),
            Box::new(PredictExecutionClientFactory::new()),
            Box::new(execution),
        )?
        .build()
}
