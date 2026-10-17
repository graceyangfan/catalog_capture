// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Shared native quote and custom snapshot subscriptions over the existing socket.
use nautilus_common::messages::data::{SubscribeQuotes, UnsubscribeQuotes};
use nautilus_model::{
    identifiers::InstrumentId,
    types::{Price, Quantity},
};

use super::*;

fn quote_subscribe(client: &mut PredictDataClient, id: InstrumentId) {
    client
        .subscribe_quotes(SubscribeQuotes::new(
            id,
            Some(ClientId::new("PREDICT")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
}
fn quote_unsubscribe(client: &mut PredictDataClient, id: InstrumentId) {
    client
        .unsubscribe_quotes(&UnsubscribeQuotes::new(
            id,
            Some(ClientId::new("PREDICT")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn quotes_and_custom_share_reconnect_topic_with_either_last_owner() {
    for (custom_last, custom_at_start, backend) in [
        (true, true, TransportBackend::default()),
        (false, true, TransportBackend::default()),
        (false, false, TransportBackend::default()),
        (true, true, TransportBackend::Tungstenite),
        (false, true, TransportBackend::Tungstenite),
        (false, false, TransportBackend::Tungstenite),
    ] {
        let (request_tx, mut requests) = mpsc::unbounded_channel();
        let state = VenueState {
            connections: Arc::new(AtomicUsize::new(0)),
            requests: request_tx,
        };
        let router = Router::new()
            .route("/v1/markets/{id}", get(market))
            .route("/ws", get(websocket))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (event_tx, mut events) = mpsc::unbounded_channel();
        replace_data_event_sender(event_tx);
        let config = PredictDataClientConfig {
            api_key: SecretString::from("fixture-key"),
            market_ids: vec![MARKET_ID],
            base_url_http: Some(format!("http://{address}")),
            base_url_ws: Some(format!("ws://{address}/ws")),
            http_timeout_secs: 5,
            ws_timeout_secs: 5,
            transport_backend: backend,
            dynamic_refresh_secs: 1,
        };
        let mut client = PredictDataClient::new(ClientId::new("PREDICT"), config).unwrap();
        client.connect().await.unwrap();
        for _ in 0..2 {
            assert!(matches!(
                events.recv().await,
                Some(DataEvent::Instrument(_))
            ));
        }
        let yes = InstrumentId::from(format!("{MARKET_ID}-1.PREDICT"));
        let no = InstrumentId::from(format!("{MARKET_ID}-2.PREDICT"));
        assert!(
            client
                .subscribe_quotes(SubscribeQuotes::new(
                    InstrumentId::from("999-1.PREDICT"),
                    Some(ClientId::new("PREDICT")),
                    None,
                    UUID4::new(),
                    UnixNanos::default(),
                    None,
                    None
                ))
                .is_err()
        );
        if custom_at_start {
            subscribe(&mut client);
        }
        quote_subscribe(&mut client, yes);
        quote_subscribe(&mut client, yes);
        quote_subscribe(&mut client, no);
        let mut recorded = vec![];
        tokio::time::timeout(Duration::from_secs(10), async {
            while recorded
                .iter()
                .filter(|r: &&Value| r["method"] == "heartbeat")
                .count()
                < 2
            {
                recorded.push(requests.recv().await.unwrap());
            }
        })
        .await
        .unwrap();
        let mut quotes = vec![];
        let mut snapshots = 0;
        while let Ok(event) = events.try_recv() {
            match event {
                DataEvent::Data(Data::Quote(q)) => quotes.push(q),
                DataEvent::Data(Data::Custom(_)) => snapshots += 1,
                other => panic!("unexpected quote replay event {other:?}"),
            }
        }
        assert_eq!(
            snapshots,
            if custom_at_start { 2 } else { 0 },
            "existing snapshot repeats are preserved across reconnect"
        );
        assert_eq!(
            quotes.len(),
            2,
            "one quote per outcome; duplicate and replay suppressed"
        );
        for q in quotes {
            assert_eq!(q.ts_event, UnixNanos::from_millis(1_700_000_000_123));
            if q.instrument_id == yes {
                assert_eq!(
                    (q.bid_price, q.ask_price),
                    (Price::from("0.62"), Price::from("0.63"))
                );
                assert_eq!(
                    (q.bid_size, q.ask_size),
                    (Quantity::from("1500.25"), Quantity::from("800.5"))
                );
            } else {
                assert_eq!(q.instrument_id, no);
                assert_eq!(
                    (q.bid_price, q.ask_price),
                    (Price::from("0.37"), Price::from("0.38"))
                );
                assert_eq!(
                    (q.bid_size, q.ask_size),
                    (Quantity::from("800.5"), Quantity::from("1500.25"))
                );
            }
        }
        if !custom_at_start {
            // Unmatched custom removals cannot consume a quote owner's reference.
            unsubscribe(&mut client);
            unsubscribe(&mut client);
        }
        if custom_at_start && !custom_last {
            unsubscribe(&mut client);
        }
        quote_unsubscribe(&mut client, yes);
        quote_unsubscribe(&mut client, yes);
        quote_unsubscribe(&mut client, no);
        if custom_at_start && custom_last {
            unsubscribe(&mut client);
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let request = requests.recv().await.unwrap();
                let done = request["data"] == "retired-topic-barrier";
                recorded.push(request);
                if done {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            recorded
                .iter()
                .filter(|r| r["method"] == "subscribe")
                .count(),
            2
        );
        assert_eq!(
            recorded
                .iter()
                .filter(|r| r["method"] == "unsubscribe")
                .count(),
            1
        );
        assert!(
            events.try_recv().is_err(),
            "last owner retirement blocks stale frames"
        );
        client.disconnect().await.unwrap();
        client.reset().unwrap();
        client.connect().await.unwrap();
        for _ in 0..2 {
            assert!(matches!(
                events.recv().await,
                Some(DataEvent::Instrument(_))
            ));
        }
        subscribe(&mut client);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if requests.recv().await.unwrap()["method"] == "heartbeat" {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            events.try_recv(),
            Ok(DataEvent::Data(Data::Custom(_)))
        ));
        assert!(
            events.try_recv().is_err(),
            "quote intent must not survive reset"
        );
        client.disconnect().await.unwrap();
        server.abort();
    }
}
