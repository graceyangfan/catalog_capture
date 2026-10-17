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

#![cfg(feature = "live")]

use nautilus_predict::{PredictInstrumentProvider, http::PredictHttpClient};

#[test]
fn public_client_debug_does_not_expose_transport_credentials() {
    let key = "predict-debug-secret-marker";
    let client = PredictHttpClient::new(key, None, 5).unwrap();
    let debug = format!("{client:?}");
    assert!(!debug.contains(key));
    assert!(debug.contains("https://api.predict.fun"));
    assert!(!format!("{:?}", PredictInstrumentProvider::new(client)).contains(key));
}

#[test]
fn public_client_rejects_unsafe_origins_and_headers_without_echoing_secrets() {
    let key = "predict-validation-secret-marker";
    for origin in [
        "http://example.invalid",
        "https://user:predict-validation-secret-marker@example.invalid",
        "https://example.invalid/api",
        "https://example.invalid?key=predict-validation-secret-marker",
        "https://example.invalid#predict-validation-secret-marker",
        "not a URL predict-validation-secret-marker",
    ] {
        let error = PredictHttpClient::new(key, Some(origin), 5).unwrap_err();
        assert!(!format!("{error:?}").contains(key));
    }
    for invalid in [format!("{key}\n"), format!(" {key}"), String::new()] {
        let error = PredictHttpClient::new(&invalid, None, 5).unwrap_err();
        assert!(!format!("{error:?}").contains(key));
    }
    assert!(PredictHttpClient::new(key, None, 0).is_err());
    for origin in [
        "https://api.predict.fun/",
        "http://127.0.0.1:12345",
        "http://[::1]:12345",
        "http://localhost:12345",
    ] {
        PredictHttpClient::new(key, Some(origin), 5).unwrap();
    }
}
