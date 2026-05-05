use crate::{
    TaskToken,
    abstractions::take_cell::TakeCell,
    telemetry::metrics::MetricsContext,
    worker::{activities::PendingActivityCancel, client::WorkerClient},
};
use futures_util::StreamExt;
use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
    time::{Duration, Instant},
};
use temporalio_common::protos::{
    coresdk::{
        ActivityHeartbeat, IntoPayloadsExt,
        activity_task::{ActivityCancelReason, ActivityCancellationDetails, ActivityTask},
    },
    temporal::api::{
        common::v1::Payload, workflowservice::v1::RecordActivityTaskHeartbeatResponse,
    },
};
use tokio::{
    sync::{
        Notify,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

/// Used to supply new heartbeat events to the activity heartbeat manager, or to send a shutdown
/// request.
pub(crate) struct ActivityHeartbeatManager {
    shutdown_token: CancellationToken,
    /// Used during `shutdown` to await until all inflight requests are sent.
    join_handle: TakeCell<JoinHandle<()>>,
    heartbeat_tx: UnboundedSender<HeartbeatAction>,
}

#[derive(Debug)]
enum HeartbeatAction {
    SendHeartbeat(ValidActivityHeartbeat),
    Evict {
        token: TaskToken,
        on_complete: Arc<Notify>,
        should_flush: bool,
    },
    CompleteReport(TaskToken),
    CompleteThrottle(TaskToken),
}

#[derive(Debug)]
struct ValidActivityHeartbeat {
    task_token: TaskToken,
    details: Vec<Payload>,
    throttle_interval: Duration,
    timeout_resetter: Option<Arc<Notify>>,
}

#[derive(Debug)]
enum HeartbeatExecutorAction {
    /// Heartbeats are throttled for this task token, sleep until duration or wait to be cancelled
    Sleep(TaskToken, Duration, CancellationToken),
    /// Report heartbeat to the server
    Report {
        task_token: TaskToken,
        details: Vec<Payload>,
    },
}

/// Errors thrown when heartbeating
#[derive(thiserror::Error, Debug)]
pub(crate) enum ActivityHeartbeatError {
    /// Heartbeat referenced an activity that we don't think exists. It may have completed already.
    #[error(
        "Heartbeat has been sent for activity that either completed or never started on this worker."
    )]
    UnknownActivity,
    /// There was a set heartbeat timeout, but it was not parseable. A valid timeout is requried
    /// to heartbeat.
    #[error("Unable to parse activity heartbeat timeout.")]
    InvalidHeartbeatTimeout,
}

/// Manages activity heartbeating for a worker. Allows sending new heartbeats or requesting and
/// awaiting for the shutdown. When shutdown is requested, signal gets sent to all processors, which
/// allows them to complete gracefully.
impl ActivityHeartbeatManager {
    /// Creates a new instance of an activity heartbeat manager and returns a handle to the user,
    /// which allows to send new heartbeats and initiate the shutdown.
    /// Returns the manager and a channel that buffers cancellation notifications to be sent to Lang.
    pub(super) fn new(
        client: Arc<dyn WorkerClient>,
        cancels_tx: UnboundedSender<PendingActivityCancel>,
        metrics: MetricsContext,
    ) -> Self {
        let (heartbeat_stream_state, heartbeat_tx_source, shutdown_token) =
            HeartbeatStreamState::new();
        let heartbeat_tx = heartbeat_tx_source.clone();

        let join_handle = tokio::spawn(
            // The stream of incoming heartbeats uses unfold to carry state across each item in the
            // stream. The closure checks if, for any given activity, we should heartbeat or not
            // depending on its delay and when we last issued a heartbeat for it.
            futures_util::stream::unfold(heartbeat_stream_state, move |mut hb_states| {
                async move {
                    let hb = tokio::select! {
                        biased;

                        _ = hb_states.cancellation_token.cancelled() => {
                            return None
                        }
                        hb = hb_states.incoming_hbs.recv() => match hb {
                            None => return None,
                            Some(hb) => hb,
                        }
                    };

                    Some((
                        match hb {
                            HeartbeatAction::SendHeartbeat(hb) => hb_states.record(hb),
                            HeartbeatAction::CompleteReport(tt) => hb_states.handle_report_completed(tt),
                            HeartbeatAction::CompleteThrottle(tt) => hb_states.handle_throttle_completed(tt),
                            HeartbeatAction::Evict{ token, on_complete, should_flush } => hb_states.evict(token, on_complete, should_flush),
                        },
                        hb_states,
                    ))
                }
            })
                // Filters out `None`s
                .filter_map(|opt| async { opt })
                .for_each_concurrent(None, move |action| {
                    let heartbeat_tx = heartbeat_tx_source.clone();
                    let sg = client.clone();
                    let cancels_tx = cancels_tx.clone();
                    let metrics = metrics.clone();
                    async move {
                        match action {
                            HeartbeatExecutorAction::Sleep(tt, duration, cancellation_token) => {
                                tokio::select! {
                                _ = cancellation_token.cancelled() => (),
                                _ = tokio::time::sleep(duration) => {
                                    let _ = heartbeat_tx.send(HeartbeatAction::CompleteThrottle(tt));
                                },
                            };
                            }
                            HeartbeatExecutorAction::Report { task_token: tt, details } => {
                                // Per-call deadline on the heartbeat RPC. Without this, a
                                // silently-wedged TCP socket (no FIN/RST/GOAWAY, e.g. mid-path
                                // idle eviction or transparent proxy bug) leaves this `.await`
                                // hung indefinitely. The state's `is_record_in_flight` flag
                                // stays true forever, and all subsequent heartbeats get
                                // silently coalesced into pending details — so the server
                                // observes radio silence and fires TIMEOUT_TYPE_HEARTBEAT.
                                //
                                // The server-side `grpc-timeout` header (set in client/src/lib.rs
                                // OTHER_CALL_TIMEOUT) does not protect against this: it is
                                // server-enforced, and a wedged socket means the server never
                                // sees the request, so the deadline is never enforced.
                                //
                                // 10s is tuned to fire well before the 30s server-side
                                // activity heartbeat_timeout while leaving generous headroom
                                // for legitimate slow heartbeats over high-latency paths
                                // (e.g. mTLS, mesh networks). On timeout, the Tonic future
                                // is dropped; for h2 send streams that are not yet closed,
                                // drop typically results in RST_STREAM(CANCEL 0x08) on the
                                // wire, signaling cancellation to the peer. The in-flight
                                // flag is reset via the CompleteReport message sent below
                                // regardless of which match arm fires.
                                const HEARTBEAT_RPC_LOCAL_TIMEOUT: Duration =
                                    Duration::from_secs(10);
                                let rpc = sg.record_activity_heartbeat(
                                    tt.clone(),
                                    details.into_payloads(),
                                );
                                match tokio::time::timeout(HEARTBEAT_RPC_LOCAL_TIMEOUT, rpc).await
                                {
                                    Ok(Ok(RecordActivityTaskHeartbeatResponse {
                                           cancel_requested, activity_paused, activity_reset
                                       })) => {
                                        if cancel_requested || activity_paused || activity_reset {
                                            // Prioritize Cancel / reset over pause
                                            let reason = if cancel_requested {
                                                ActivityCancelReason::Cancelled
                                            } else if activity_reset {
                                                ActivityCancelReason::Reset
                                            } else {
                                                ActivityCancelReason::Paused
                                            };
                                            cancels_tx
                                                .send(PendingActivityCancel::new(
                                                    tt.clone(),
                                                    reason,
                                                    ActivityCancellationDetails {
                                                        is_cancelled: cancel_requested,
                                                        is_paused: activity_paused,
                                                        is_reset: activity_reset,
                                                        ..Default::default()
                                                    }
                                                ))
                                                .expect(
                                                    "Receive half of heartbeat cancels not blocked",
                                                );
                                        }
                                    }
                                    // Send cancels for any activity that learns its workflow already
                                    // finished (which is one thing not found implies - other reasons
                                    // would seem equally valid).
                                    Ok(Err(s)) if s.code() == tonic::Code::NotFound => {
                                        debug!(task_token = %tt,
                                           "Activity not found when recording heartbeat");
                                        cancels_tx
                                            .send(PendingActivityCancel::new(
                                                tt.clone(),
                                                ActivityCancelReason::NotFound,
                                                ActivityTask::primary_reason_to_cancellation_details(ActivityCancelReason::NotFound)
                                            ))
                                            .expect("Receive half of heartbeat cancels not blocked");
                                    }
                                    Ok(Err(e)) => {
                                        warn!("Error when recording heartbeat: {:?}", e);
                                    }
                                    Err(_elapsed) => {
                                        metrics.heartbeat_rpc_local_timeout();
                                        warn!(
                                            task_token = %tt,
                                            timeout_secs =
                                                HEARTBEAT_RPC_LOCAL_TIMEOUT.as_secs(),
                                            "Heartbeat RPC timed out locally; assuming socket \
                                             wedged. In-flight flag will be reset via \
                                             CompleteReport so subsequent heartbeats are not \
                                             coalesced."
                                        );
                                    }
                                };
                                let _ = heartbeat_tx.send(HeartbeatAction::CompleteReport(tt));
                            }
                        }
                    }
                }),
        );

        Self {
            join_handle: TakeCell::new(join_handle),
            shutdown_token,
            heartbeat_tx,
        }
    }
    /// Records a new heartbeat, the first call will result in an immediate call to the server,
    /// while rapid successive calls would accumulate for up to `delay` and then latest heartbeat
    /// details will be sent to the server.
    ///
    /// It is important that this function is never called with a task token equal to one given
    /// to [Self::evict] after it has been called, doing so will cause a memory leak, as there is
    /// no longer an efficient way to forget about that task token.
    pub(super) fn record(
        &self,
        hb: ActivityHeartbeat,
        throttle_interval: Duration,
        timeout_resetter: Option<Arc<Notify>>,
    ) -> Result<(), ActivityHeartbeatError> {
        self.heartbeat_tx
            .send(HeartbeatAction::SendHeartbeat(ValidActivityHeartbeat {
                task_token: TaskToken(hb.task_token),
                details: hb.details,
                throttle_interval,
                timeout_resetter,
            }))
            .expect("Receive half of the heartbeats event channel must not be dropped");

        Ok(())
    }

    /// Tell the heartbeat manager we are done forever with a certain task, so it may be forgotten.
    /// If should_flush is true, will also force-flush the most recently provided details.
    /// Record *should* not be called with the same TaskToken after calling this.
    pub(super) async fn evict(&self, task_token: TaskToken, should_flush: bool) {
        let completed = Arc::new(Notify::new());
        let _ = self.heartbeat_tx.send(HeartbeatAction::Evict {
            token: task_token,
            on_complete: completed.clone(),
            should_flush,
        });
        completed.notified().await;
    }

    /// Initiates shutdown procedure by stopping lifecycle loop and awaiting for all in-flight
    /// heartbeat requests to be flushed to the server.
    pub(super) async fn shutdown(&self) {
        self.shutdown_token.cancel();
        if let Some(h) = self.join_handle.take_once() {
            let handle_r = h.await;
            if let Err(e) = handle_r
                && !e.is_cancelled()
            {
                error!(
                    "Unexpected error joining heartbeating tasks during shutdown: {:?}",
                    e
                )
            }
        }
    }
}

#[derive(Debug)]
struct ActivityHeartbeatState {
    /// If None and throttle interval is over, untrack this task token
    last_recorded_details: Option<Vec<Payload>>,
    /// True if we've queued up a request to record against server, but it hasn't yet completed
    is_record_in_flight: bool,
    last_send_requested: Instant,
    throttle_interval: Duration,
    throttled_cancellation_token: Option<CancellationToken>,
    timeout_resetter: Option<Arc<Notify>>,
}

impl ActivityHeartbeatState {
    /// Get duration to sleep by subtracting `throttle_interval` by elapsed time since
    /// `last_send_requested`
    fn get_throttle_sleep_duration(&self) -> Duration {
        let time_since_last_sent = self.last_send_requested.elapsed();

        if time_since_last_sent > Duration::ZERO && self.throttle_interval > time_since_last_sent {
            self.throttle_interval - time_since_last_sent
        } else {
            Duration::ZERO
        }
    }
}

#[derive(Debug)]
struct HeartbeatStreamState {
    tt_to_state: HashMap<TaskToken, ActivityHeartbeatState>,
    tt_needs_flush: HashMap<TaskToken, Arc<Notify>>,
    incoming_hbs: UnboundedReceiver<HeartbeatAction>,
    /// Token that can be used to cancel the entire stream.
    /// Requests to the server are not cancelled with this token.
    cancellation_token: CancellationToken,
}

impl HeartbeatStreamState {
    fn new() -> (Self, UnboundedSender<HeartbeatAction>, CancellationToken) {
        let (heartbeat_tx, incoming_hbs) = unbounded_channel();
        let cancellation_token = CancellationToken::new();
        (
            Self {
                cancellation_token: cancellation_token.clone(),
                tt_to_state: Default::default(),
                tt_needs_flush: Default::default(),
                incoming_hbs,
            },
            heartbeat_tx,
            cancellation_token,
        )
    }

    /// Record a heartbeat received from lang
    fn record(&mut self, hb: ValidActivityHeartbeat) -> Option<HeartbeatExecutorAction> {
        match self.tt_to_state.entry(hb.task_token.clone()) {
            Entry::Vacant(e) => {
                let state = ActivityHeartbeatState {
                    throttle_interval: hb.throttle_interval,
                    last_send_requested: Instant::now(),
                    // Don't record here because we already flush out these details.
                    // None is used to mark that after throttling we can stop tracking this task
                    // token.
                    last_recorded_details: None,
                    is_record_in_flight: true,
                    throttled_cancellation_token: None,
                    timeout_resetter: hb.timeout_resetter,
                };
                e.insert(state);
                Some(HeartbeatExecutorAction::Report {
                    task_token: hb.task_token,
                    details: hb.details,
                })
            }
            Entry::Occupied(mut o) => {
                let state = o.get_mut();
                state.last_recorded_details = Some(hb.details);
                state.timeout_resetter = hb.timeout_resetter;
                None
            }
        }
    }

    /// Heartbeat report to server completed
    fn handle_report_completed(&mut self, tt: TaskToken) -> Option<HeartbeatExecutorAction> {
        if let Some(not) = self.tt_needs_flush.remove(&tt) {
            not.notify_one();
        }
        if let Some(st) = self.tt_to_state.get_mut(&tt) {
            st.is_record_in_flight = false;
            let cancellation_token = self.cancellation_token.child_token();
            st.throttled_cancellation_token = Some(cancellation_token.clone());
            if let Some(ref r) = st.timeout_resetter {
                r.notify_one();
            }
            // Always sleep for simplicity even if the duration is 0
            Some(HeartbeatExecutorAction::Sleep(
                tt.clone(),
                st.get_throttle_sleep_duration(),
                cancellation_token,
            ))
        } else {
            None
        }
    }

    /// Throttling completed, report or stop tracking task token
    fn handle_throttle_completed(&mut self, tt: TaskToken) -> Option<HeartbeatExecutorAction> {
        match self.tt_to_state.entry(tt.clone()) {
            Entry::Occupied(mut e) => {
                let state = e.get_mut();
                if let Some(details) = state.last_recorded_details.take() {
                    // Delete the recorded details before reporting
                    // Reset the cancellation token and schedule another report
                    state.throttled_cancellation_token = None;
                    state.last_send_requested = Instant::now();
                    state.is_record_in_flight = true;
                    Some(HeartbeatExecutorAction::Report {
                        task_token: tt,
                        details,
                    })
                } else {
                    // Nothing to report, forget this task token
                    e.remove();
                    None
                }
            }
            Entry::Vacant(_) => None,
        }
    }

    /// Activity should not be tracked anymore, cancel throttle timer if running.
    ///
    /// Will return a report action if there are recorded details present, to ensure we flush the
    /// latest details before we cease tracking this activity.
    fn evict(
        &mut self,
        tt: TaskToken,
        on_complete: Arc<Notify>,
        should_flush: bool,
    ) -> Option<HeartbeatExecutorAction> {
        if let Some(state) = self.tt_to_state.remove(&tt) {
            if let Some(cancel_tok) = state.throttled_cancellation_token {
                cancel_tok.cancel();
            }
            if let Some(last_deets) = state.last_recorded_details
                && should_flush
            {
                self.tt_needs_flush.insert(tt.clone(), on_complete);
                return Some(HeartbeatExecutorAction::Report {
                    task_token: tt,
                    details: last_deets,
                });
            } else if state.is_record_in_flight {
                self.tt_needs_flush.insert(tt, on_complete);
                return None;
            }
        }
        // Since there's nothing to flush immediately report back that eviction is finished
        on_complete.notify_one();
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::telemetry::MetricsCallBuffer;
    use crate::worker::client::mocks::{mock_manual_worker_client, mock_worker_client};
    use futures_util::FutureExt;
    use std::time::Duration;
    use temporalio_common::protos::temporal::api::{
        common::v1::Payload, workflowservice::v1::RecordActivityTaskHeartbeatResponse,
    };
    use temporalio_common::telemetry::{
        TelemetryOptions,
        metrics::{
            CoreMeter,
            core::{BufferInstrumentRef, MetricCallBufferer, MetricEvent, MetricUpdateVal},
        },
        telemetry_init,
    };
    use tokio::time::sleep;

    /// Ensure that heartbeats that are sent with a small `throttle_interval` are aggregated and sent roughly once
    /// every 1/2 of the heartbeat timeout.
    #[tokio::test]
    async fn process_heartbeats_and_shutdown() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(2);
        let (cancel_tx, _cancel_rx) = unbounded_channel();
        let hm = ActivityHeartbeatManager::new(
            Arc::new(mock_client),
            cancel_tx,
            MetricsContext::no_op(),
        );
        let fake_task_token = vec![1, 2, 3];
        // Send 2 heartbeat requests for 20ms apart.
        // The first heartbeat should be sent right away, and
        // the second should be throttled until 50ms have passed.
        for i in 0_u8..2 {
            record_heartbeat(&hm, fake_task_token.clone(), i, Duration::from_millis(50));
            sleep(Duration::from_millis(20)).await;
        }
        // sleep again to let heartbeats be flushed
        sleep(Duration::from_millis(20)).await;
        hm.shutdown().await;
    }

    #[tokio::test]
    async fn send_heartbeats_less_frequently_than_throttle_interval() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(3);
        let (cancel_tx, _cancel_rx) = unbounded_channel();
        let hm = ActivityHeartbeatManager::new(
            Arc::new(mock_client),
            cancel_tx,
            MetricsContext::no_op(),
        );
        let fake_task_token = vec![1, 2, 3];
        // Heartbeats always get sent if recorded less frequently than the throttle interval
        for i in 0_u8..3 {
            record_heartbeat(&hm, fake_task_token.clone(), i, Duration::from_millis(10));
            sleep(Duration::from_millis(20)).await;
        }
        hm.shutdown().await;
    }

    /// Ensure that heartbeat can be called from a tight loop and correctly throttle
    #[tokio::test]
    async fn process_tight_loop_and_shutdown() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(1);
        let (cancel_tx, _cancel_rx) = unbounded_channel();
        let hm = ActivityHeartbeatManager::new(
            Arc::new(mock_client),
            cancel_tx,
            MetricsContext::no_op(),
        );
        let fake_task_token = vec![1, 2, 3];
        // Send a whole bunch of heartbeats very fast. We should still only send one total.
        for i in 0_u8..50 {
            record_heartbeat(&hm, fake_task_token.clone(), i, Duration::from_millis(2000));
            // Let it propagate
            sleep(Duration::from_millis(10)).await;
        }
        hm.shutdown().await;
    }

    /// This test reports one heartbeat and waits for the throttle_interval to elapse before sending another
    #[tokio::test]
    async fn report_heartbeat_after_timeout() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(2);
        let (cancel_tx, _cancel_rx) = unbounded_channel();
        let hm = ActivityHeartbeatManager::new(
            Arc::new(mock_client),
            cancel_tx,
            MetricsContext::no_op(),
        );
        let fake_task_token = vec![1, 2, 3];
        record_heartbeat(&hm, fake_task_token.clone(), 0, Duration::from_millis(100));
        sleep(Duration::from_millis(500)).await;
        record_heartbeat(&hm, fake_task_token, 1, Duration::from_millis(100));
        // Let it propagate
        sleep(Duration::from_millis(50)).await;
        hm.shutdown().await;
    }

    #[tokio::test]
    async fn evict_works() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(2);
        let (cancel_tx, _cancel_rx) = unbounded_channel();
        let hm = ActivityHeartbeatManager::new(
            Arc::new(mock_client),
            cancel_tx,
            MetricsContext::no_op(),
        );
        let fake_task_token = vec![1, 2, 3];
        record_heartbeat(&hm, fake_task_token.clone(), 0, Duration::from_millis(100));
        // Let it propagate
        sleep(Duration::from_millis(10)).await;
        hm.evict(fake_task_token.clone().into(), true).await;
        record_heartbeat(&hm, fake_task_token, 0, Duration::from_millis(100));
        // Let it propagate
        sleep(Duration::from_millis(10)).await;
        // We know it works b/c otherwise we would have only called record 1 time w/o sleep
        hm.shutdown().await;
    }

    #[tokio::test]
    async fn evict_immediate_after_record() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(1);
        let (cancel_tx, _cancel_rx) = unbounded_channel();
        let hm = ActivityHeartbeatManager::new(
            Arc::new(mock_client),
            cancel_tx,
            MetricsContext::no_op(),
        );
        let fake_task_token = vec![1, 2, 3];
        record_heartbeat(&hm, fake_task_token.clone(), 0, Duration::from_millis(100));
        hm.evict(fake_task_token.clone().into(), true).await;
        hm.shutdown().await;
    }

    #[tokio::test]
    async fn no_flush_on_successful_completion() {
        let mut mock_client = mock_worker_client();
        // Should only expect 1 heartbeat call, not 2 (the second would be from evict flushing)
        mock_client
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(1);
        let (cancel_tx, _cancel_rx) = unbounded_channel();
        let hm = ActivityHeartbeatManager::new(
            Arc::new(mock_client),
            cancel_tx,
            MetricsContext::no_op(),
        );
        let fake_task_token = vec![1, 2, 3];

        // Record initial heartbeat - this should be sent immediately
        record_heartbeat(&hm, fake_task_token.clone(), 0, Duration::from_millis(100));

        // Wait a bit for initial heartbeat to process and enter throttling phase
        sleep(Duration::from_millis(50)).await;

        // Record another heartbeat while throttled - this should be stored in last_recorded_details
        record_heartbeat(&hm, fake_task_token.clone(), 1, Duration::from_millis(100));

        // Wait a bit to ensure the second heartbeat is recorded but not sent
        sleep(Duration::from_millis(10)).await;

        // Evict the activity with should_flush false
        // This should NOT send the stored heartbeat details since the activity completed successfully
        hm.evict(fake_task_token.into(), false).await;

        hm.shutdown().await;
    }

    /// Behavioral regression test: when `record_activity_heartbeat`
    /// hangs (silent socket-wedge simulation), the local 10s
    /// `tokio::time::timeout` wrap fires and the patched closure
    /// increments `MetricsContext::heartbeat_rpc_local_timeout()`.
    /// Exercises the actual `Err(_elapsed)` arm —
    /// `test_heartbeat_rpc_local_timeout_metric` (in metrics.rs)
    /// only covers the direct method call.
    ///
    /// Uses `MockManualWorkerClient` (the mock that supports
    /// returning arbitrary `impl Future`) with `future::pending`
    /// for the heartbeat RPC, plus `tokio::time::pause`/`advance`
    /// for virtual-clock control.
    #[tokio::test(start_paused = true)]
    async fn heartbeat_local_timeout_increments_metric() {
        // Local marker type for MetricsCallBuffer's I parameter.
        // We don't resolve instrument refs in this test (we count
        // Updates without filtering by instrument), so any
        // BufferInstrumentRef impl works.
        #[derive(Debug, Clone)]
        struct UnusedInstrRef;
        impl BufferInstrumentRef for UnusedInstrRef {}

        // Mock client where record_activity_heartbeat NEVER completes,
        // simulating a silently-wedged TCP socket (no FIN/RST/GOAWAY).
        let mut mock_client = mock_manual_worker_client();
        mock_client
            .expect_record_activity_heartbeat()
            .returning(|_, _| {
                futures_util::future::pending::<
                    std::result::Result<RecordActivityTaskHeartbeatResponse, tonic::Status>,
                >()
                .boxed()
            });

        // Buffer-backed MetricsContext so we can observe increments.
        let call_buffer: Arc<MetricsCallBuffer<UnusedInstrRef>> =
            Arc::new(MetricsCallBuffer::new(100));
        let telem_instance = telemetry_init(
            TelemetryOptions::builder()
                .metrics(call_buffer.clone() as Arc<dyn CoreMeter>)
                .build(),
        )
        .unwrap();
        let mc = MetricsContext::top_level("ns".to_string(), "tq".to_string(), &telem_instance);

        let (cancel_tx, _cancel_rx) = unbounded_channel();
        let hm = ActivityHeartbeatManager::new(Arc::new(mock_client), cancel_tx, mc);

        // Send a heartbeat. The first heartbeat for a token bypasses
        // throttling and immediately enters the Report path:
        // record_activity_heartbeat -> pending future ->
        // tokio::time::timeout(10s, ...).
        record_heartbeat(&hm, vec![1, 2, 3], 0, Duration::from_millis(50));

        // Yield aggressively so the spawned for_each_concurrent task
        // receives the message, dispatches through the stream, calls
        // the mock client, gets the pending future, and reaches the
        // inner await on tokio::time::timeout(10s, ...).
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }

        // Advance virtual time past the 10s local timeout. The
        // timeout fires Err(_elapsed); the patched closure calls
        // metrics.heartbeat_rpc_local_timeout(), then logs warn!,
        // then sends CompleteReport.
        tokio::time::advance(Duration::from_secs(11)).await;

        // Yield so the closure's post-timeout code runs.
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }

        // Drain buffered events. The Err(_elapsed) closure's only
        // metric call in the heartbeat path is our patched one, so
        // we expect exactly one Update with Delta(1).
        let events = call_buffer.retrieve();
        let updates: Vec<MetricUpdateVal> = events
            .iter()
            .filter_map(|e| match e {
                MetricEvent::Update { update, .. } => Some(*update),
                _ => None,
            })
            .collect();
        assert_eq!(
            updates.len(),
            1,
            "expected exactly one Update event from the Err(_elapsed) arm, got {}",
            updates.len()
        );
        assert!(
            matches!(updates[0], MetricUpdateVal::Delta(1)),
            "expected Delta(1), got {:?}",
            updates[0]
        );

        hm.shutdown().await;
    }

    fn record_heartbeat(
        hm: &ActivityHeartbeatManager,
        task_token: Vec<u8>,
        payload_data: u8,
        throttle_interval: Duration,
    ) {
        hm.record(
            ActivityHeartbeat {
                task_token,
                details: vec![Payload {
                    metadata: Default::default(),
                    data: vec![payload_data],
                    external_payloads: Default::default(),
                }],
            },
            // Mimic the same delay we would apply in activity task manager
            throttle_interval,
            None,
        )
        .expect("hearbeat recording should not fail");
    }
}
