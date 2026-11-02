//! Queries and updates in a Rust workflow (ADR 0036), through the embedded engine and a restart.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use aqueduct_core::{Payload, Status};
use aqueduct_engine::embedded::{Attempt, Engine, EngineError};
use aqueduct_sdk::{WorkflowContext, WorkflowError, workflow};
use serde::{Deserialize, Serialize};

const SECOND: Duration = Duration::from_secs(10);

#[derive(Serialize, Deserialize)]
struct Cart {
    items: Vec<String>,
}

/// A cart that takes items until it is checked out. Empty item names are refused on receipt; items
/// the `stock` step has none of are refused after asking it.
async fn cart(ctx: WorkflowContext, owner: String) -> Result<Cart, WorkflowError> {
    let mut cart = Cart { items: Vec::new() };
    ctx.publish("owner", &owner)?;
    loop {
        ctx.publish("cart", &cart)?;
        let Some(update) = ctx.wait_update::<String>("add", None).await? else {
            continue;
        };
        match update.value.as_str() {
            "checkout" => {
                ctx.respond(&update, &cart.items.len())?;
                break;
            }
            "" => ctx.reject(&update, "an item needs a name"),
            item => {
                let in_stock: bool = ctx.step("stock", item).await?;
                if in_stock {
                    cart.items.push(item.to_owned());
                    ctx.respond(&update, &cart.items)?;
                } else {
                    ctx.reject(&update, &format!("{item} is out of stock"));
                }
            }
        }
    }
    let _: () = ctx.step("charge", &cart.items).await?;
    Ok(cart)
}

fn engine(dir: &std::path::Path) -> Engine {
    Engine::builder(dir)
        .workflow("cart", Arc::new(workflow(cart)))
        .step("stock", |item: &Payload| {
            Attempt::Completed(Payload::new(if item.as_bytes() == b"\"unicorn\"" {
                b"false".to_vec()
            } else {
                b"true".to_vec()
            }))
        })
        .step("charge", |_: &Payload| {
            Attempt::Completed(Payload::new(*b"null"))
        })
        .open()
        .unwrap()
}

fn json(text: &str) -> Payload {
    Payload::new(text.as_bytes().to_vec())
}

fn add(engine: &Engine, id: &str, item: &str) -> Result<String, EngineError> {
    engine
        .update("c-1", "add", id, json(&format!("\"{item}\"")), SECOND)
        .map(|answer| String::from_utf8(answer.0).unwrap())
}

#[test]
fn a_workflow_answers_updates_and_queries_across_a_restart() {
    let dir = std::env::temp_dir().join(format!("aqueduct-sdk-updates-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    {
        let engine = engine(&dir);
        engine.start("c-1", "cart", json("\"ana\"")).unwrap();
        assert_eq!(add(&engine, "a-1", "tea").unwrap(), r#"["tea"]"#);
        assert_eq!(
            add(&engine, "a-2", ""),
            Err(EngineError::UpdateRejected("an item needs a name".into()))
        );
        assert_eq!(
            add(&engine, "a-3", "unicorn"),
            Err(EngineError::UpdateRejected(
                "unicorn is out of stock".into()
            ))
        );
        assert_eq!(
            engine.query("c-1", "cart").unwrap().0,
            br#"{"items":["tea"]}"#
        );
        assert_eq!(
            engine.query("c-1", "total"),
            Err(EngineError::UnknownQuery {
                key: "c-1".into(),
                name: "total".into()
            })
        );
    }
    // After a restart the cart is as it was, and a retried update gets its first answer.
    let engine = engine(&dir);
    assert_eq!(engine.query("c-1", "owner").unwrap().0, br#""ana""#);
    assert_eq!(add(&engine, "a-1", "tea").unwrap(), r#"["tea"]"#);
    assert_eq!(add(&engine, "a-4", "milk").unwrap(), r#"["tea","milk"]"#);
    assert_eq!(add(&engine, "a-5", "checkout").unwrap(), "2");
    let result = engine.wait("c-1", SECOND).unwrap();
    assert_eq!(result.status, Status::Completed, "{:?}", result.error);
    assert_eq!(result.output.unwrap().0, br#"{"items":["tea","milk"]}"#);
    assert_eq!(
        add(&engine, "a-6", "jam"),
        Err(EngineError::UpdateRejected("run `c-1` has ended".into()))
    );
    drop(engine);
    std::fs::remove_dir_all(&dir).unwrap();
}
