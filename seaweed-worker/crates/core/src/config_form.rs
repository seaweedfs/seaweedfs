//! Builders for the config forms a worker returns in its JobTypeDescriptor.
//!
//! Admin renders these into the job's settings page, so a worker written in any
//! language gets a UI without touching Go or templ. That only holds if the
//! field types and defaults are right, which is why they are built here rather
//! than spelled out at each call site.

use std::collections::HashMap;

use crate::pb::{config_value::Kind, ConfigField, ConfigFieldType, ConfigForm, ConfigSection, ConfigValue};

pub fn int_value(value: i64) -> ConfigValue {
    ConfigValue {
        kind: Some(Kind::Int64Value(value)),
    }
}

pub fn bool_value(value: bool) -> ConfigValue {
    ConfigValue {
        kind: Some(Kind::BoolValue(value)),
    }
}

pub fn string_value(value: impl Into<String>) -> ConfigValue {
    ConfigValue {
        kind: Some(Kind::StringValue(value.into())),
    }
}

/// Reads an integer a request carried, falling back when admin sent nothing.
pub fn int_or(values: &HashMap<String, ConfigValue>, name: &str, fallback: i64) -> i64 {
    match values.get(name).and_then(|v| v.kind.as_ref()) {
        Some(Kind::Int64Value(value)) => *value,
        Some(Kind::DoubleValue(value)) => *value as i64,
        _ => fallback,
    }
}

pub fn bool_or(values: &HashMap<String, ConfigValue>, name: &str, fallback: bool) -> bool {
    match values.get(name).and_then(|v| v.kind.as_ref()) {
        Some(Kind::BoolValue(value)) => *value,
        _ => fallback,
    }
}

pub fn string_or(values: &HashMap<String, ConfigValue>, name: &str, fallback: &str) -> String {
    match values.get(name).and_then(|v| v.kind.as_ref()) {
        Some(Kind::StringValue(value)) if !value.is_empty() => value.clone(),
        _ => fallback.to_string(),
    }
}

pub fn number_field(
    name: &str,
    label: &str,
    description: &str,
    min: i64,
    max: i64,
) -> ConfigField {
    ConfigField {
        name: name.to_string(),
        label: label.to_string(),
        description: description.to_string(),
        field_type: ConfigFieldType::Int64 as i32,
        min_value: Some(int_value(min)),
        max_value: Some(int_value(max)),
        ..Default::default()
    }
}

pub fn bool_field(name: &str, label: &str, description: &str) -> ConfigField {
    ConfigField {
        name: name.to_string(),
        label: label.to_string(),
        description: description.to_string(),
        field_type: ConfigFieldType::Bool as i32,
        ..Default::default()
    }
}

pub fn form(
    form_id: &str,
    title: &str,
    fields: Vec<ConfigField>,
    defaults: HashMap<String, ConfigValue>,
) -> ConfigForm {
    ConfigForm {
        form_id: form_id.to_string(),
        title: title.to_string(),
        description: String::new(),
        sections: vec![ConfigSection {
            section_id: format!("{form_id}-main"),
            title: title.to_string(),
            description: String::new(),
            fields,
        }],
        default_values: defaults,
    }
}
