//! Config schemas: validation, defaults and layered merging.
//!
//! Official Cordis plugins declare a `Plugin.Config` (a Standard Schema) and
//! `fiber.update()` validates new config before restarting. Rust has no
//! Standard Schema, so this module defines the equivalent contract:
//!
//! * [`ConfigSchema::validate`] normalizes raw config and reports issues;
//! * [`ConfigSchema::merge`] combines intercept layers (root → child → head);
//! * [`JsonConfigSchema`] is the default schema for JSON-object configs.
//!
//! The wire format stays `serde_json::Value`; schemas are optional, and a
//! plugin/service without one keeps the historical shallow-merge behaviour.

use std::collections::BTreeMap;
use std::fmt;

use serde_json::{Map, Value};

/// One or more config validation issues, aggregated like Cordis
/// `ValidationError` (`issue.message (at issue.path)` lines).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ValidationError {
    /// Human-readable issues, one per line.
    pub issues: Vec<String>,
}

impl ValidationError {
    /// Build an error from raw issue strings.
    pub fn new(issues: Vec<String>) -> Self {
        Self { issues }
    }

    /// The aggregated message (`invalid config:` followed by one `- issue` line
    /// each), matching Cordis' `ValidationError.message`.
    pub fn message(&self) -> String {
        let mut message = String::from("invalid config:");
        for issue in &self.issues {
            message.push_str("\n  - ");
            message.push_str(issue);
        }
        message
    }

    /// Whether no issue was recorded.
    pub fn is_empty(&self) -> bool {
        self.issues.is_empty()
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message())
    }
}

impl std::error::Error for ValidationError {}

/// JSON value kind accepted by [`JsonConfigSchema::property`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonType {
    String,
    Number,
    Bool,
    Object,
    Array,
}

impl JsonType {
    /// Human-readable type name used in validation issues.
    pub fn name(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Number => "number",
            Self::Bool => "boolean",
            Self::Object => "object",
            Self::Array => "array",
        }
    }

    /// Whether `value` matches this JSON kind.
    pub fn matches(self, value: &Value) -> bool {
        matches!(
            (self, value),
            (Self::String, Value::String(_))
                | (Self::Number, Value::Number(_))
                | (Self::Bool, Value::Bool(_))
                | (Self::Object, Value::Object(_))
                | (Self::Array, Value::Array(_))
        )
    }
}

/// A config schema: validation plus layered merging.
///
/// Mirrors Cordis `Plugin.Config` for plugin configs and the service
/// `Config.merge` hook used by `resolveConfig`.
pub trait ConfigSchema: Send + Sync {
    /// Validate and normalize a raw config value.
    ///
    /// `Err` carries human-readable issues; callers surface them through
    /// [`crate::CordisError::Validation`] (fiber update) or mark the fiber
    /// failed (initial activation).
    fn validate(&self, value: Value) -> std::result::Result<Value, Vec<String>>;

    /// Merge config layers from lowest to highest precedence.
    ///
    /// `base` is the lowest-precedence value (may be `Value::Null`), followed
    /// by `overrides` in increasing precedence. Used by
    /// [`crate::Context::resolve_config`] when a schema is registered for the
    /// service name.
    fn merge(&self, base: Value, overrides: &[Value]) -> Value;
}

/// Validate `value` with an embedder schema, containing a panic.
///
/// `ConfigSchema` is implemented by plugins/embedders; a panicking `validate`
/// must surface as an ordinary validation failure instead of unwinding through
/// `ctx.plugin` / `Fiber::update`. Call sites (`fiber::validate_config`) should
/// route through this helper.
pub(crate) fn contained_validate(
    schema: &dyn ConfigSchema,
    value: Value,
) -> std::result::Result<Value, Vec<String>> {
    crate::internal::contained(
        "zeokeel_cordis::config",
        "ConfigSchema::validate",
        None,
        || Some(schema.validate(value)),
    )
    .unwrap_or_else(|| Err(vec!["config schema panicked during validation".to_string()]))
}

/// Merge config layers with an embedder schema, containing a panic.
///
/// A panicking `merge` degrades to the lowest-precedence layer (`base`), which
/// keeps the call shape and never unwinds out of `Context::resolve_config`.
pub(crate) fn contained_merge(
    schema: &dyn ConfigSchema,
    base: Value,
    overrides: &[Value],
) -> Value {
    let fallback = base.clone();
    crate::internal::contained(
        "zeokeel_cordis::config",
        "ConfigSchema::merge",
        None,
        || Some(schema.merge(base, overrides)),
    )
    .unwrap_or(fallback)
}

/// The default JSON-object schema.
///
/// Validation accepts an object (or `null`, treated as an empty object),
/// injects defaults for missing keys, reports missing required keys, checks
/// declared property types and — when [`JsonConfigSchema::deny_unknown`] was
/// called — rejects undeclared keys.
#[derive(Debug, Clone)]
pub struct JsonConfigSchema {
    required: Vec<String>,
    defaults: Map<String, Value>,
    additional_properties: bool,
    properties: BTreeMap<String, JsonType>,
}

impl Default for JsonConfigSchema {
    fn default() -> Self {
        Self {
            required: Vec::new(),
            defaults: Map::new(),
            additional_properties: true,
            properties: BTreeMap::new(),
        }
    }
}

impl JsonConfigSchema {
    /// An object schema that allows unknown keys.
    pub fn new() -> Self {
        Self::default()
    }

    /// Require `name` to be present (after defaults are injected).
    pub fn required(mut self, name: impl Into<String>) -> Self {
        self.required.push(name.into());
        self
    }

    /// Inject `value` when `name` is missing.
    pub fn default_value(mut self, name: impl Into<String>, value: Value) -> Self {
        let name = name.into();
        self.properties.entry(name.clone()).or_insert(match value {
            Value::String(_) => JsonType::String,
            Value::Number(_) => JsonType::Number,
            Value::Bool(_) => JsonType::Bool,
            Value::Object(_) => JsonType::Object,
            Value::Array(_) => JsonType::Array,
            Value::Null => JsonType::Object,
        });
        self.defaults.insert(name, value);
        self
    }

    /// Reject keys that are neither declared as a property nor given a default.
    pub fn deny_unknown(mut self) -> Self {
        self.additional_properties = false;
        self
    }

    /// Declare the expected JSON kind of `name`.
    pub fn property(mut self, name: impl Into<String>, kind: JsonType) -> Self {
        self.properties.insert(name.into(), kind);
        self
    }

    /// The declared defaults.
    pub fn defaults(&self) -> &Map<String, Value> {
        &self.defaults
    }

    /// The required keys.
    pub fn required_keys(&self) -> &[String] {
        &self.required
    }

    fn validate_object(
        &self,
        object: Map<String, Value>,
    ) -> std::result::Result<Value, Vec<String>> {
        let mut issues = Vec::new();
        let mut normalized = object;

        for (name, default) in &self.defaults {
            if !normalized.contains_key(name) {
                normalized.insert(name.clone(), default.clone());
            }
        }
        for name in &self.required {
            if !normalized.contains_key(name) {
                issues.push(format!("missing required field `{name}`"));
            }
        }
        for (name, value) in &normalized {
            match self.properties.get(name) {
                Some(expected) if !expected.matches(value) => issues.push(format!(
                    "field `{name}` must be {}, got {}",
                    expected.name(),
                    json_kind(value)
                )),
                None if !self.additional_properties => {
                    issues.push(format!("unknown field `{name}`"));
                }
                _ => {}
            }
        }

        if issues.is_empty() {
            Ok(Value::Object(normalized))
        } else {
            Err(issues)
        }
    }
}

fn json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

impl ConfigSchema for JsonConfigSchema {
    fn validate(&self, value: Value) -> std::result::Result<Value, Vec<String>> {
        match value {
            Value::Null => self.validate_object(Map::new()),
            Value::Object(object) => self.validate_object(object),
            other => Err(vec![format!(
                "expected an object, got {}",
                json_kind(&other)
            )]),
        }
    }

    fn merge(&self, base: Value, overrides: &[Value]) -> Value {
        let mut merged = Map::new();
        let mut layers = Vec::with_capacity(overrides.len() + 1);
        layers.push(base);
        layers.extend(overrides.iter().cloned());
        for layer in layers {
            if let Value::Object(object) = layer {
                for (key, value) in object {
                    merged.insert(key, value);
                }
            }
        }
        // Defaults never override an explicitly provided value.
        for (name, default) in &self.defaults {
            merged
                .entry(name.clone())
                .or_insert_with(|| default.clone());
        }
        Value::Object(merged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema() -> JsonConfigSchema {
        JsonConfigSchema::new()
            .required("model")
            .default_value("temperature", json!(0.2))
            .property("model", JsonType::String)
            .deny_unknown()
    }

    #[test]
    fn validate_injects_defaults_and_reports_missing() {
        let issues = schema().validate(json!({})).unwrap_err();
        assert_eq!(issues.len(), 1);
        assert!(issues[0].contains("model"));

        let value = schema().validate(json!({"model": "deepseek-v4"})).unwrap();
        assert_eq!(value["model"], json!("deepseek-v4"));
        assert_eq!(value["temperature"], json!(0.2));
    }

    #[test]
    fn validate_reports_types_and_unknown_keys() {
        let issues = schema()
            .validate(json!({"model": 1, "extra": true}))
            .unwrap_err();
        assert_eq!(issues.len(), 2, "issues: {issues:?}");
        assert!(issues.iter().any(|issue| issue.contains("model")));
        assert!(issues.iter().any(|issue| issue.contains("extra")));
    }

    #[test]
    fn merge_is_shallow_with_defaults_and_precedence() {
        let merged = schema().merge(
            json!({"temperature": 1.0, "model": "base"}),
            &[json!({"temperature": 0.5}), json!({"top_p": 0.9})],
        );
        assert_eq!(merged["temperature"], json!(0.5));
        assert_eq!(merged["model"], json!("base"));
        assert_eq!(merged["top_p"], json!(0.9));
    }
}
