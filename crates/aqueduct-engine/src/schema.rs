//! Checking agent answers against their output schema (JSON Schema, draft 2020-12; ADR 0025).
//!
//! Hermetic, like everything that decides a run's course: schemas are compiled from the request alone,
//! with no loader, so a `$ref` to anything outside the schema is an error, never a network or file read.

use aqueduct_core::Payload;
use serde_json::Value;

/// Why an answer was not accepted.
#[derive(Debug)]
pub(crate) enum SchemaError {
    /// The schema itself does not compile: a mistake in the workflow, not in the answer.
    Schema(String),
    /// The answer does not match the schema (or is not JSON).
    Mismatch(String),
}

/// The base location the schema is compiled at; nothing is ever loaded from it.
const LOCATION: &str = "https://aqueduct.invalid/output-schema.json";

/// The output schema in an agent call's request, if it has one.
pub(crate) fn output_schema(request: &Payload) -> Option<Value> {
    let request: Value = serde_json::from_slice(request.as_bytes()).ok()?;
    match request.get("output_schema") {
        None | Some(Value::Null) => None,
        Some(schema) => Some(schema.clone()),
    }
}

/// Checks `answer` against `schema`.
pub(crate) fn check(schema: &Value, answer: &Payload) -> Result<(), SchemaError> {
    let answer: Value = serde_json::from_slice(answer.as_bytes())
        .map_err(|e| SchemaError::Mismatch(format!("the answer is not JSON: {e}")))?;
    let mut compiler = boon::Compiler::new();
    compiler.set_default_draft(boon::Draft::V2020_12);
    // No loaders: references outside the schema fail instead of reaching the network or the disk.
    compiler.use_loader(Box::new(boon::SchemeUrlLoader::new()));
    compiler
        .add_resource(LOCATION, schema.clone())
        .map_err(|e| SchemaError::Schema(format!("{e:#}")))?;
    let mut schemas = boon::Schemas::new();
    let index = compiler
        .compile(LOCATION, &mut schemas)
        .map_err(|e| SchemaError::Schema(format!("{e:#}")))?;
    schemas
        .validate(&answer, index)
        .map_err(|e| SchemaError::Mismatch(e.to_string().replace(LOCATION, "the output schema")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(text: &str) -> Payload {
        Payload::new(text.as_bytes().to_vec())
    }

    #[test]
    fn answers_are_checked_and_schemas_stay_hermetic() {
        let schema: Value = serde_json::json!({
            "type": "object",
            "required": ["label"],
            "properties": {"label": {"enum": ["invoice", "receipt"]}}
        });
        assert!(check(&schema, &payload(r#"{"label":"invoice"}"#)).is_ok());
        assert!(matches!(
            check(&schema, &payload(r#"{"label":"poem"}"#)),
            Err(SchemaError::Mismatch(_))
        ));
        assert!(matches!(
            check(&schema, &payload("not json")),
            Err(SchemaError::Mismatch(_))
        ));
        let remote = serde_json::json!({"$ref": "https://example.com/schema.json"});
        assert!(matches!(
            check(&remote, &payload("1")),
            Err(SchemaError::Schema(_))
        ));
        let file = serde_json::json!({"$ref": "schemas/Classification.json"});
        assert!(matches!(
            check(&file, &payload("1")),
            Err(SchemaError::Schema(_))
        ));
    }

    #[test]
    fn the_schema_is_read_from_the_request() {
        assert!(output_schema(&payload(r#"{"input":1,"output_schema":null}"#)).is_none());
        assert!(output_schema(&payload(r#"{"input":1}"#)).is_none());
        assert_eq!(
            output_schema(&payload(r#"{"output_schema":{"type":"string"}}"#)),
            Some(serde_json::json!({"type": "string"}))
        );
    }
}
