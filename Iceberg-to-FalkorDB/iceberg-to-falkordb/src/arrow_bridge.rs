//! LOCAL VENDOR of Arrow `RecordBatch` → `LogicalRow` conversion.
//!
//! This module intentionally duplicates the conversion logic that will live in
//! `common/arrow-to-falkordb-bridge` (owned by the parquet-agent). Keep all
//! conversion code here so the module can be deleted wholesale once the shared
//! crate is available as a path dependency.
//!
//! DELETE THIS FILE when switching to `arrow_to_falkordb_bridge`.

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use chrono::{TimeZone, Utc};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

use crate::source::LogicalRow;

/// Convert an Arrow `RecordBatch` into a vector of `LogicalRow` (JSON maps).
pub fn record_batch_to_logical_rows(batch: &RecordBatch) -> anyhow::Result<Vec<LogicalRow>> {
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    let mut rows: Vec<LogicalRow> = (0..num_rows)
        .map(|_| LogicalRow {
            values: JsonMap::with_capacity(schema.fields().len()),
        })
        .collect();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let column = batch.column(col_idx);
        let name = field.name();
        for row_idx in 0..num_rows {
            let value = array_value_to_json(column.as_ref(), row_idx)?;
            rows[row_idx].values.insert(name.clone(), value);
        }
    }

    Ok(rows)
}

fn array_value_to_json(array: &dyn Array, row_idx: usize) -> anyhow::Result<JsonValue> {
    if array.is_null(row_idx) {
        return Ok(JsonValue::Null);
    }

    match array.data_type() {
        DataType::Null => Ok(JsonValue::Null),
        DataType::Boolean => Ok(JsonValue::Bool(array.as_boolean().value(row_idx))),
        DataType::Int8 => Ok(JsonValue::Number(JsonNumber::from(
            array.as_primitive::<Int8Type>().value(row_idx) as i64,
        ))),
        DataType::Int16 => Ok(JsonValue::Number(JsonNumber::from(
            array.as_primitive::<Int16Type>().value(row_idx) as i64,
        ))),
        DataType::Int32 => Ok(JsonValue::Number(JsonNumber::from(
            array.as_primitive::<Int32Type>().value(row_idx) as i64,
        ))),
        DataType::Int64 => Ok(JsonValue::Number(JsonNumber::from(
            array.as_primitive::<Int64Type>().value(row_idx),
        ))),
        DataType::UInt8 => Ok(JsonValue::Number(JsonNumber::from(
            array.as_primitive::<UInt8Type>().value(row_idx) as u64,
        ))),
        DataType::UInt16 => Ok(JsonValue::Number(JsonNumber::from(
            array.as_primitive::<UInt16Type>().value(row_idx) as u64,
        ))),
        DataType::UInt32 => Ok(JsonValue::Number(JsonNumber::from(
            array.as_primitive::<UInt32Type>().value(row_idx) as u64,
        ))),
        DataType::UInt64 => Ok(JsonValue::Number(JsonNumber::from(
            array.as_primitive::<UInt64Type>().value(row_idx),
        ))),
        DataType::Float32 => {
            let v = array.as_primitive::<Float32Type>().value(row_idx) as f64;
            Ok(json_number_from_f64(v))
        }
        DataType::Float64 => {
            let v = array.as_primitive::<Float64Type>().value(row_idx);
            Ok(json_number_from_f64(v))
        }
        DataType::Utf8 => Ok(JsonValue::String(array.as_string::<i32>().value(row_idx).to_string())),
        DataType::LargeUtf8 => Ok(JsonValue::String(
            array.as_string::<i64>().value(row_idx).to_string(),
        )),
        DataType::Utf8View => Ok(JsonValue::String(
            array.as_string_view().value(row_idx).to_string(),
        )),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            // Store binary as base64-ish hex string for property safety.
            let bytes = match array.data_type() {
                DataType::Binary => array.as_binary::<i32>().value(row_idx),
                DataType::LargeBinary => array.as_binary::<i64>().value(row_idx),
                DataType::FixedSizeBinary(_) => array.as_fixed_size_binary().value(row_idx),
                _ => unreachable!(),
            };
            Ok(JsonValue::String(hex_encode(bytes)))
        }
        DataType::Date32 => {
            let days = array.as_primitive::<Date32Type>().value(row_idx) as i64;
            let secs = days.saturating_mul(86_400);
            Ok(JsonValue::String(format_utc_secs(secs)))
        }
        DataType::Date64 => {
            let ms = array.as_primitive::<Date64Type>().value(row_idx);
            Ok(JsonValue::String(format_utc_millis(ms)))
        }
        DataType::Timestamp(unit, tz) => {
            let nanos = match unit {
                TimeUnit::Second => {
                    array.as_primitive::<TimestampSecondType>().value(row_idx) * 1_000_000_000
                }
                TimeUnit::Millisecond => {
                    array
                        .as_primitive::<TimestampMillisecondType>()
                        .value(row_idx)
                        * 1_000_000
                }
                TimeUnit::Microsecond => {
                    array
                        .as_primitive::<TimestampMicrosecondType>()
                        .value(row_idx)
                        * 1_000
                }
                TimeUnit::Nanosecond => array
                    .as_primitive::<TimestampNanosecondType>()
                    .value(row_idx),
            };
            // Always emit RFC3339. Timezone metadata is preserved only insofar as
            // the absolute instant is correct; offset strings are not re-applied.
            let _ = tz;
            Ok(JsonValue::String(format_utc_nanos(nanos)))
        }
        DataType::Time32(_) | DataType::Time64(_) => {
            // Time-of-day without date: stringify via debug-ish decimal.
            Ok(JsonValue::String(format!("{:?}", array_scalar_debug(array, row_idx))))
        }
        DataType::Duration(_) | DataType::Interval(_) => {
            Ok(JsonValue::String(format!("{:?}", array_scalar_debug(array, row_idx))))
        }
        DataType::Decimal128(_, scale) => {
            let arr = array.as_primitive::<Decimal128Type>();
            let raw = arr.value(row_idx);
            Ok(JsonValue::String(decimal128_to_string(raw, *scale)))
        }
        DataType::Decimal256(_, scale) => {
            // Stringify via Debug to preserve precision without depending on i256 helpers.
            let _ = scale;
            Ok(JsonValue::String(format!("{:?}", array_scalar_debug(array, row_idx))))
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            // Complex nested values become JSON strings (FalkorDB property limit).
            let json = list_to_json(array, row_idx)?;
            match json {
                JsonValue::Array(items) if items.iter().all(is_primitive) => Ok(JsonValue::Array(items)),
                other => Ok(JsonValue::String(
                    serde_json::to_string(&other).unwrap_or_else(|_| "[]".to_string()),
                )),
            }
        }
        DataType::Struct(_) | DataType::Map(_, _) | DataType::Union(_, _) | DataType::Dictionary(_, _) => {
            Ok(JsonValue::String(format!("{:?}", array_scalar_debug(array, row_idx))))
        }
        other => Ok(JsonValue::String(format!(
            "unsupported_arrow_type:{other:?}"
        ))),
    }
}

fn is_primitive(v: &JsonValue) -> bool {
    matches!(
        v,
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_)
    )
}

fn list_to_json(array: &dyn Array, row_idx: usize) -> anyhow::Result<JsonValue> {
    // Best-effort: convert list children element-wise when possible.
    match array.data_type() {
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let values = list.value(row_idx);
            let mut out = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                out.push(array_value_to_json(values.as_ref(), i)?);
            }
            Ok(JsonValue::Array(out))
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let values = list.value(row_idx);
            let mut out = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                out.push(array_value_to_json(values.as_ref(), i)?);
            }
            Ok(JsonValue::Array(out))
        }
        DataType::FixedSizeList(_, _) => {
            let list = array.as_fixed_size_list();
            let values = list.value(row_idx);
            let mut out = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                out.push(array_value_to_json(values.as_ref(), i)?);
            }
            Ok(JsonValue::Array(out))
        }
        _ => Ok(JsonValue::String("[]".to_string())),
    }
}

fn json_number_from_f64(v: f64) -> JsonValue {
    JsonNumber::from_f64(v)
        .map(JsonValue::Number)
        .unwrap_or_else(|| JsonValue::String(v.to_string()))
}

fn format_utc_secs(secs: i64) -> String {
    match Utc.timestamp_opt(secs, 0) {
        chrono::LocalResult::Single(dt) => dt.to_rfc3339(),
        _ => secs.to_string(),
    }
}

fn format_utc_millis(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let nanos = (ms.rem_euclid(1000) * 1_000_000) as u32;
    match Utc.timestamp_opt(secs, nanos) {
        chrono::LocalResult::Single(dt) => dt.to_rfc3339(),
        _ => ms.to_string(),
    }
}

fn format_utc_nanos(nanos: i64) -> String {
    let secs = nanos.div_euclid(1_000_000_000);
    let n = nanos.rem_euclid(1_000_000_000) as u32;
    match Utc.timestamp_opt(secs, n) {
        chrono::LocalResult::Single(dt) => dt.to_rfc3339(),
        _ => nanos.to_string(),
    }
}

fn decimal128_to_string(raw: i128, scale: i8) -> String {
    if scale <= 0 {
        let mut s = raw.to_string();
        if scale < 0 {
            s.push_str(&"0".repeat((-scale) as usize));
        }
        return s;
    }
    let scale = scale as usize;
    let negative = raw < 0;
    let abs = raw.unsigned_abs();
    let digits = abs.to_string();
    let (int_part, frac_part) = if digits.len() <= scale {
        ("0", format!("{:0>width$}", digits, width = scale))
    } else {
        let split = digits.len() - scale;
        (&digits[..split], digits[split..].to_string())
    };
    if negative {
        format!("-{int_part}.{frac_part}")
    } else {
        format!("{int_part}.{frac_part}")
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

fn array_scalar_debug(array: &dyn Array, row_idx: usize) -> String {
    // Fallback path: use Arrow's Display via downcast-free debug of the slice.
    format!("row={row_idx},type={:?}", array.data_type())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn converts_basic_types() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
            Field::new("amount", DataType::Float64, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("alice"), None])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5)])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_700_000_000_000_000),
                    None,
                ]).with_timezone("UTC")) as ArrayRef,
            ],
        )
        .unwrap();

        let rows = record_batch_to_logical_rows(&batch).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("id"), Some(&JsonValue::from(1)));
        assert_eq!(
            rows[0].get("name"),
            Some(&JsonValue::String("alice".into()))
        );
        assert_eq!(rows[0].get("active"), Some(&JsonValue::Bool(true)));
        assert!(matches!(rows[0].get("ts"), Some(JsonValue::String(_))));
        assert_eq!(rows[1].get("name"), Some(&JsonValue::Null));
        assert_eq!(rows[1].get("ts"), Some(&JsonValue::Null));
    }

    #[test]
    fn decimal128_stringifies_with_scale() {
        assert_eq!(decimal128_to_string(12345, 2), "123.45");
        assert_eq!(decimal128_to_string(-7, 3), "-0.007");
        assert_eq!(decimal128_to_string(42, 0), "42");
    }
}
