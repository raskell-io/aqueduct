//! Workflow workers (ADR 0039, `workflow.proto`): a gRPC stream with credit over the engine's activation
//! queue. The session rules are the step workers': a session ends when either stream closes, and its
//! leases are released, including one a take in flight handed it.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use aqueduct_engine::activations::{Activation, ActivationQueue};
use aqueduct_engine::namespaces::Scope;
use aqueduct_protocol::v1;
use tokio::sync::{Notify, mpsc};

use crate::{Api, Failure, Kind};

const TAKE_WAIT: Duration = Duration::from_millis(500);

static SESSIONS: AtomicU64 = AtomicU64::new(0);

impl Api {
    fn activations(&self) -> Result<Arc<ActivationQueue>, Failure> {
        self.engine.activation_queue().cloned().ok_or(Failure {
            kind: Kind::Unavailable,
            message: "this server runs no remote workflows".into(),
        })
    }
}

fn activation(activation: Activation) -> v1::Activation {
    v1::Activation {
        activation_id: activation.id,
        run_id: activation.run,
        workflow: activation.workflow,
        at: activation.at as u64,
        history: activation.history,
        now_ms: activation.env.now_ms,
        seed: activation.env.seed,
        lease_ms: activation.lease_ms,
    }
}

type ServeStream = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<v1::WorkflowServerMessage, tonic::Status>> + Send>,
>;

#[tonic::async_trait]
impl v1::workflows_server::Workflows for Api {
    type ServeStream = ServeStream;

    async fn serve(
        &self,
        request: tonic::Request<tonic::Streaming<v1::WorkflowWorkerMessage>>,
    ) -> Result<tonic::Response<ServeStream>, tonic::Status> {
        let queue = self.activations()?;
        let scope = crate::worker_scope();
        let mut inbound = request.into_inner();
        let hello = match inbound.message().await? {
            Some(v1::WorkflowWorkerMessage {
                message: Some(v1::workflow_worker_message::Message::Hello(hello)),
            }) => hello,
            _ => {
                return Err(tonic::Status::invalid_argument(
                    "a workflow worker's first message is its Hello",
                ));
            }
        };
        let session = format!(
            "{}#{}",
            hello.worker,
            SESSIONS.fetch_add(1, Ordering::Relaxed)
        );
        let workflows = Arc::new(hello.workflows);
        let credit = Arc::new(AtomicU32::new(hello.capacity.max(1)));
        let closed = Arc::new(AtomicBool::new(false));
        let changed = Arc::new(Notify::new());
        let (out, stream) = mpsc::channel(64);

        // Decisions and credit from the worker.
        {
            let (engine, queue) = (Arc::clone(&self.engine), Arc::clone(&queue));
            let (credit, closed, changed) = (credit.clone(), closed.clone(), changed.clone());
            let (session, gone, scope) = (session.clone(), out.clone(), scope.clone());
            tokio::spawn(async move {
                loop {
                    let message = tokio::select! {
                        message = inbound.message() => message,
                        () = gone.closed() => break,
                    };
                    let Ok(Some(message)) = message else {
                        break;
                    };
                    match message.message {
                        Some(v1::workflow_worker_message::Message::Decision(decision)) => {
                            let commands: Result<Vec<_>, _> = decision
                                .commands
                                .into_iter()
                                .map(aqueduct_protocol::commands::from_proto)
                                .collect();
                            // A decision that does not decode is dropped; its lease runs out and the
                            // activation is offered again.
                            if let (Ok(commands), Some((run, at))) =
                                (commands, queue.complete_in(&scope, &decision.activation_id))
                            {
                                let _ = engine.decision(&run, at, commands);
                            }
                            credit.fetch_add(1, Ordering::SeqCst);
                        }
                        Some(v1::workflow_worker_message::Message::Credit(more)) => {
                            credit.fetch_add(more, Ordering::SeqCst);
                        }
                        _ => {}
                    }
                    changed.notify_one();
                }
                closed.store(true, Ordering::SeqCst);
                changed.notify_one();
                queue.release(&session);
            });
        }

        tokio::spawn(serve_activations(
            queue, scope, session, workflows, credit, closed, changed, out,
        ));
        Ok(tonic::Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(stream),
        )))
    }
}

#[allow(clippy::too_many_arguments)]
async fn serve_activations(
    queue: Arc<ActivationQueue>,
    scope: Scope,
    session: String,
    workflows: Arc<Vec<String>>,
    credit: Arc<AtomicU32>,
    closed: Arc<AtomicBool>,
    changed: Arc<Notify>,
    out: mpsc::Sender<Result<v1::WorkflowServerMessage, tonic::Status>>,
) {
    'session: while !closed.load(Ordering::SeqCst) {
        let available = credit.load(Ordering::SeqCst);
        if available == 0 {
            changed.notified().await;
            continue;
        }
        let (taking, names, name, scope) = (
            Arc::clone(&queue),
            Arc::clone(&workflows),
            session.clone(),
            scope.clone(),
        );
        let taken = tokio::task::spawn_blocking(move || {
            taking.wait_take_in(&scope, &name, &names, available as usize, TAKE_WAIT)
        })
        .await
        .unwrap_or_default();
        for taken in taken {
            credit.fetch_sub(1, Ordering::SeqCst);
            let message = v1::WorkflowServerMessage {
                message: Some(v1::workflow_server_message::Message::Activation(
                    activation(taken),
                )),
            };
            if out.send(Ok(message)).await.is_err() {
                break 'session;
            }
        }
    }
    queue.release(&session);
}
