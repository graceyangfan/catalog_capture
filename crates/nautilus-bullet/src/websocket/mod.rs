// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Public WebSocket message decoding and canonical data conversion.

pub mod messages;
pub mod parse;
mod session;
pub use session::BulletWebSocketSession;

#[cfg(test)]
mod tests;
