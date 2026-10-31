//! An order saga. Run it with `cargo run -p aqueduct --example order_saga`.
//!
//! An order reserves stock, charges the card and books a shipment. If a later step fails, the earlier
//! ones are undone in reverse order: refund the card, then release the stock. Compensations are steps
//! like any other, so they are journaled and survive a crash halfway through undoing.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use aqueduct::embedded::{Attempt, Engine};
use aqueduct::{Payload, WorkflowContext, WorkflowError, workflow};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Order {
    id: String,
    item: String,
    destination: String,
}

async fn place_order(ctx: WorkflowContext, order: Order) -> Result<String, WorkflowError> {
    let mut saga = ctx.saga();
    let result = async {
        saga.step::<(), _, _>("reserve_stock", &order.item, "release_stock", &order.item)
            .await?;
        let payment: String = saga
            .step("charge_card", &order.id, "refund_card", &order.id)
            .await?;
        let tracking: String = ctx.step("book_shipment", &order.destination).await?;
        Ok(format!("{payment}, {tracking}"))
    }
    .await;
    match result {
        Ok(done) => Ok(done),
        Err(error) => Err(saga.abort(error).await),
    }
}

fn json<T: Serialize>(value: &T) -> Payload {
    Payload::new(serde_json::to_vec(value).unwrap_or_default())
}

fn text(payload: &Payload) -> String {
    serde_json::from_slice(payload.as_bytes()).unwrap_or_default()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::fs::remove_dir_all("./order-saga-data");
    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let note = |log: &Arc<Mutex<Vec<String>>>, line: String| {
        if let Ok(mut log) = log.lock() {
            log.push(line);
        }
    };
    let (l1, l2, l3, l4, l5) = (
        Arc::clone(&log),
        Arc::clone(&log),
        Arc::clone(&log),
        Arc::clone(&log),
        Arc::clone(&log),
    );
    let engine = Engine::builder("./order-saga-data")
        .workflow("place_order", Arc::new(workflow(place_order)))
        .step("reserve_stock", move |item: &Payload| {
            note(&l1, format!("reserved {}", text(item)));
            Attempt::Completed(json(&()))
        })
        .step("release_stock", move |item: &Payload| {
            note(&l2, format!("released {}", text(item)));
            Attempt::Completed(json(&()))
        })
        .step("charge_card", move |id: &Payload| {
            note(&l3, format!("charged {}", text(id)));
            Attempt::Completed(json(&format!("paid {}", text(id))))
        })
        .step("refund_card", move |id: &Payload| {
            note(&l4, format!("refunded {}", text(id)));
            Attempt::Completed(json(&()))
        })
        .step("book_shipment", move |destination: &Payload| {
            let destination = text(destination);
            if destination == "Atlantis" {
                // Not retryable: no carrier will ever go there.
                return Attempt::Fail(format!("no carrier ships to {destination}"));
            }
            note(&l5, format!("shipped to {destination}"));
            Attempt::Completed(json(&format!("tracking {}", destination.len() * 1_000 + 7)))
        })
        .open()?;

    for (id, destination) in [("o-1", "Lisbon"), ("o-2", "Atlantis")] {
        let order = Order {
            id: id.into(),
            item: "lamp".into(),
            destination: destination.into(),
        };
        engine.start(id, "place_order", json(&order))?;
        let result = engine.wait(id, Duration::from_secs(30))?;
        let output = result.output.as_ref().map(text);
        println!(
            "{id}: {:?} {}",
            result.status,
            output.or(result.error).unwrap_or_default()
        );
        for line in log
            .lock()
            .map(|mut l| std::mem::take(&mut *l))
            .unwrap_or_default()
        {
            println!("    {line}");
        }
    }
    Ok(())
}
