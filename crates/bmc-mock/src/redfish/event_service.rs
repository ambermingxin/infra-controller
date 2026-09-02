/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Redfish `EventService` with a Server-Sent Events stream.
//!
//! Subscribers receive events published after they connect. Events are
//! published either by `EventService.SubmitTestEvent` or by the mock's own
//! injection surface, which lets a test drive a specific event — an Xid error
//! against a named GPU chassis, for example — and observe how a collector
//! attributes it.

use std::borrow::Cow;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::response::Response;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::routing::{get, post};
use futures::Stream;
use serde_json::json;
use tokio::sync::broadcast;

use crate::bmc_state::BmcState;
use crate::json::{JsonExt, JsonPatch};
use crate::redfish;

const EVENT_SERVICE_TYPE: &str = "#EventService.v1_10_0.EventService";
const EVENT_TYPE: &str = "#Event.v1_7_0.Event";

/// Bound on buffered events per subscriber. A slow reader loses the oldest
/// events rather than stalling the publisher.
const EVENT_CHANNEL_CAPACITY: usize = 64;

pub(crate) fn resource<'a>() -> redfish::Resource<'a> {
    redfish::Resource {
        odata_id: Cow::Borrowed("/redfish/v1/EventService"),
        odata_type: Cow::Borrowed(EVENT_SERVICE_TYPE),
        id: Cow::Borrowed("EventService"),
        name: Cow::Borrowed("Event Service"),
    }
}

fn sse_odata_id() -> &'static str {
    "/redfish/v1/EventService/SSE"
}

fn submit_test_event_odata_id() -> &'static str {
    "/redfish/v1/EventService/Actions/EventService.SubmitTestEvent"
}

/// Publishes Redfish events to connected SSE subscribers.
pub struct EventServiceState {
    sender: broadcast::Sender<serde_json::Value>,
    next_event_id: AtomicU64,
}

impl Default for EventServiceState {
    fn default() -> Self {
        Self::new()
    }
}

impl EventServiceState {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            sender,
            next_event_id: AtomicU64::new(1),
        }
    }

    /// Publishes one already-formed `Event` payload.
    ///
    /// Returns the number of subscribers that received it. Zero is normal and
    /// not an error: the mock accepts events with nobody listening.
    pub fn publish(&self, event: serde_json::Value) -> usize {
        self.sender.send(event).unwrap_or(0)
    }

    /// Publishes a single-record `Event` describing a condition on a resource.
    ///
    /// `origin_of_condition` is the Redfish URI of the resource the event is
    /// about, such as `/redfish/v1/Chassis/HGX_GPU_SXM_1`.
    pub fn publish_event_record(
        &self,
        message_id: &str,
        message: &str,
        severity: &str,
        origin_of_condition: Option<&str>,
    ) -> usize {
        let id = self.next_event_id.fetch_add(1, Ordering::Relaxed);
        self.publish(event_payload(
            id,
            message_id,
            message,
            severity,
            origin_of_condition,
        ))
    }

    fn subscribe(&self) -> broadcast::Receiver<serde_json::Value> {
        self.sender.subscribe()
    }
}

/// Builds an `Event` resource carrying exactly one `EventRecord`.
fn event_payload(
    id: u64,
    message_id: &str,
    message: &str,
    severity: &str,
    origin_of_condition: Option<&str>,
) -> serde_json::Value {
    let mut record = json!({
        "@odata.id": format!("{}#/Events/{id}", sse_odata_id()),
        "MemberId": id.to_string(),
        "EventId": id.to_string(),
        "EventTimestamp": event_timestamp(),
        "EventType": "Alert",
        "Message": message,
        "MessageId": message_id,
        "MessageSeverity": severity,
    });

    if let Some(origin) = origin_of_condition {
        record
            .as_object_mut()
            .expect("event record is a JSON object")
            .insert(
                "OriginOfCondition".to_string(),
                json!({ "@odata.id": origin }),
            );
    }

    json!({
        "@odata.id": format!("{}#/Event{id}", sse_odata_id()),
        "@odata.type": EVENT_TYPE,
        "Id": id.to_string(),
        "Name": "Event Array",
        "Events": [record],
    })
}

/// RFC 3339 timestamp, which is what Redfish `EventTimestamp` requires.
fn event_timestamp() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Fixed-format conversion avoids pulling a date library into the mock.
    let days = now / 86_400;
    let seconds_of_day = now % 86_400;
    let (year, month, day) = civil_from_days(days as i64);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}+00:00",
        seconds_of_day / 3600,
        (seconds_of_day % 3600) / 60,
        seconds_of_day % 60,
    )
}

/// Converts days since the Unix epoch to a civil date.
///
/// Howard Hinnant's `civil_from_days`, shifted to a March-based year so that
/// the leap day lands at the end.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m as u32, d as u32)
}

pub(crate) fn add_routes(r: Router<BmcState>) -> Router<BmcState> {
    r.route(&resource().odata_id, get(get_event_service))
        .route(sse_odata_id(), get(get_sse_stream))
        .route(submit_test_event_odata_id(), post(post_submit_test_event))
}

async fn get_event_service() -> Response {
    resource()
        .json_patch()
        .patch(json!({
            "ServiceEnabled": true,
            "DeliveryRetryAttempts": 3,
            "DeliveryRetryIntervalSeconds": 60,
            "ServerSentEventUri": sse_odata_id(),
            "EventFormatTypes": ["Event"],
            "ResourceTypes": ["Chassis", "ComputerSystem", "Processor"],
            "Subscriptions": {
                "@odata.id": "/redfish/v1/EventService/Subscriptions"
            },
            "Actions": {
                "#EventService.SubmitTestEvent": {
                    "target": submit_test_event_odata_id()
                }
            },
        }))
        .into_ok_response()
}

async fn get_sse_stream(
    State(state): State<BmcState>,
) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>> {
    let receiver = state.event_service_state.subscribe();

    let stream = futures::stream::unfold(receiver, |mut receiver| async move {
        loop {
            match receiver.recv().await {
                Ok(event) => {
                    let sse_event = SseEvent::default().data(event.to_string());
                    return Some((Ok(sse_event), receiver));
                }
                // A slow subscriber missed events. Keep the stream open; the
                // collector under test should tolerate gaps.
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    });

    // Comment heartbeats keep intermediaries from closing an idle stream.
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

/// Redfish `EventService.SubmitTestEvent`.
///
/// Accepts the standard body fields and republishes them on the SSE stream.
async fn post_submit_test_event(
    State(state): State<BmcState>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let message_id = body
        .get("MessageId")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("Alert.1.0.TestEvent");
    let message = body
        .get("Message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("Test event");
    let severity = body
        .get("MessageSeverity")
        .or_else(|| body.get("Severity"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("OK");
    let origin_of_condition = body
        .get("OriginOfCondition")
        .and_then(|origin| match origin {
            // Redfish allows a bare string here; some clients send a link object.
            serde_json::Value::String(uri) => Some(uri.as_str()),
            serde_json::Value::Object(_) => origin.get("@odata.id").and_then(|id| id.as_str()),
            _ => None,
        });

    let subscribers = state.event_service_state.publish_event_record(
        message_id,
        message,
        severity,
        origin_of_condition,
    );

    json!({ "Subscribers": subscribers }).into_ok_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_payload_matches_redfish_event_shape() {
        let payload = event_payload(
            7,
            "Nvidia.1.0.XidError",
            "Xid 79 on GPU",
            "Critical",
            Some("/redfish/v1/Chassis/HGX_GPU_SXM_3"),
        );

        assert_eq!(payload["@odata.type"], EVENT_TYPE);
        let record = &payload["Events"][0];
        assert_eq!(record["MessageId"], "Nvidia.1.0.XidError");
        assert_eq!(record["MessageSeverity"], "Critical");
        assert_eq!(
            record["OriginOfCondition"]["@odata.id"],
            "/redfish/v1/Chassis/HGX_GPU_SXM_3"
        );
    }

    #[test]
    fn event_payload_omits_origin_when_absent() {
        let payload = event_payload(1, "Alert.1.0.TestEvent", "Test event", "OK", None);

        assert!(payload["Events"][0].get("OriginOfCondition").is_none());
    }

    #[test]
    fn civil_from_days_matches_known_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(19_723), (2024, 1, 1));
        // 2024 was a leap year, so day 60 of that year is February 29.
        assert_eq!(civil_from_days(19_723 + 59), (2024, 2, 29));
    }

    #[tokio::test]
    async fn publish_reaches_a_subscriber() {
        let state = EventServiceState::new();
        let mut receiver = state.subscribe();

        assert_eq!(
            state.publish_event_record("Test.1.0", "hello", "OK", None),
            1
        );

        let event = receiver.recv().await.expect("subscriber receives event");
        assert_eq!(event["Events"][0]["Message"], "hello");
    }

    #[test]
    fn publish_without_subscribers_is_not_an_error() {
        let state = EventServiceState::new();

        assert_eq!(
            state.publish_event_record("Test.1.0", "hello", "OK", None),
            0
        );
    }
}
