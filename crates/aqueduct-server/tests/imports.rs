//! The import service (ADR 0047): an upload that is not complete, or not true, leaves nothing behind.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use aqueduct_codec::ChainWriter;
use aqueduct_core::{Entry, Payload};
use aqueduct_engine::embedded::Engine;
use aqueduct_engine::import::pending;
use aqueduct_procedure::Procedure;
use aqueduct_protocol::v1;
use aqueduct_protocol::v1::import_message::Message;
use aqueduct_protocol::v1::imports_client::ImportsClient;

const WAITS: &str = "def run(x):\n    return wait(signal(\"go\"))\n";

async fn serve(name: &str) -> (PathBuf, String) {
    let dir = std::env::temp_dir().join(format!("aqueduct-imports-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Arc::new(
        Engine::builder(&dir)
            .workflow(
                "waits",
                Arc::new(Procedure::compile("w.star", WAITS).unwrap()),
            )
            .remote_steps(Duration::from_secs(30))
            .open()
            .unwrap(),
    );
    engine.start("mine", "waits", Payload::new(*b"1")).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(aqueduct_server::serve(
        engine,
        listener,
        std::future::pending(),
    ));
    (dir, url)
}

fn history(key: &str, input: &[u8]) -> Vec<Vec<u8>> {
    let mut chain = ChainWriter::new(key);
    vec![chain.append(&Entry::Started {
        workflow: "waits".into(),
        input: Payload::new(input.to_vec()),
        budget_tokens: None,
        version: None,
    })]
}

fn run(key: &str, records: Vec<Vec<u8>>, last: bool) -> Message {
    Message::Run(v1::ImportRun {
        run_id: key.to_owned(),
        records,
        last,
    })
}

fn commit(runs: u64) -> Message {
    Message::Commit(v1::ImportCommit { runs, sources: 0 })
}

async fn upload(url: &str, messages: Vec<Message>) -> Result<v1::ImportReceipt, tonic::Status> {
    let mut client = ImportsClient::connect(url.to_owned()).await.unwrap();
    let messages: Vec<v1::ImportMessage> = messages
        .into_iter()
        .map(|message| v1::ImportMessage {
            message: Some(message),
        })
        .collect();
    client
        .upload(tokio_stream::iter(messages))
        .await
        .map(tonic::Response::into_inner)
}

fn nothing_waits(dir: &Path) {
    assert!(pending(dir).is_none(), "it left something to apply");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_incomplete_or_untrue_upload_leaves_nothing() {
    use tonic::Code;
    let (dir, url) = serve("bad").await;
    let good = || run("k", history("k", b"1"), true);

    let cases: Vec<(&str, Vec<Message>, Code)> = vec![
        ("no commit", vec![good()], Code::Aborted),
        ("too few runs", vec![commit(1)], Code::InvalidArgument),
        (
            "more runs than it says",
            vec![good(), commit(0)],
            Code::InvalidArgument,
        ),
        (
            "a run never finished",
            vec![run("k", history("k", b"1"), false), commit(0)],
            Code::InvalidArgument,
        ),
        (
            "another run's history",
            vec![run("k", history("other", b"1"), true), commit(1)],
            Code::InvalidArgument,
        ),
        (
            "a tenant's run",
            vec![
                run("$ns/acme/k", history("$ns/acme/k", b"1"), true),
                commit(1),
            ],
            Code::InvalidArgument,
        ),
        (
            "a key this server has another run under",
            vec![run("mine", history("mine", b"2"), true), commit(1)],
            Code::FailedPrecondition,
        ),
        (
            "a path for a version id",
            vec![
                Message::Source(v1::ImportSource {
                    version: "../../tokens".into(),
                    text: b"x".to_vec(),
                }),
                commit(0),
            ],
            Code::InvalidArgument,
        ),
    ];
    for (what, messages, code) in cases {
        let status = upload(&url, messages).await.unwrap_err();
        assert_eq!(status.code(), code, "{what}: {status}");
        nothing_waits(&dir);
    }

    // A run sent in parts is one run, and a run this server has is counted, not refused.
    let mut parts = history("long", b"1");
    let first = parts.remove(0);
    let receipt = upload(
        &url,
        vec![
            run("long", vec![first], true),
            run("mine", history("mine", b"1"), true),
            commit(2),
        ],
    )
    .await
    .unwrap();
    assert_eq!((receipt.runs, receipt.already_there), (2, 1));
    assert!(pending(&dir).is_some());

    // Only one waits at a time.
    let again = upload(&url, vec![good(), commit(1)]).await.unwrap_err();
    assert_eq!(again.code(), Code::FailedPrecondition);
    let _ = std::fs::remove_dir_all(&dir);
}
