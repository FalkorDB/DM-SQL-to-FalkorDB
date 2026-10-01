//! Arrow `RecordBatch` → FalkorDB-safe JSON `LogicalRow` conversion.

use anyhow::{anyhow, Result};
use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, ListArray, MapArray, StringArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

/// Logical row abstraction used by mapping/sink layers across connectors.
///
/// Identical shape to the `LogicalRow` used by existing SQL connectors
/// (`serde_json::Map<String, Value>`).
pub type LogicalRow = JsonMap<String, JsonValue>;

/// Convert an Arrow `RecordBatch` into a vector of logical JSON rows.
///
/// Column names become map keys. Null cells become `JsonValue::Null`.
/// Complex nested values (structs, maps, nested lists) are JSON-stringified
/// so FalkorDB property values remain primitives or arrays of primitives.
pub fn record_batch_to_logical_rows(batch: &RecordBatch) -> Result<Vec<LogicalRow>> {
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut map = JsonMap::with_capacity(batch.num_columns());
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let array = batch.column(col_idx);
            let value = array_value_to_json(array.as_ref(), row_idx)?;
            map.insert(field.name().clone(), value);
        }
        rows.push(map);
    }
    Ok(rows)
}

/// Convert a single Arrow array cell at `row_idx` into a JSON value.
pub fn array_value_to_json(array: &dyn Array, row_idx: usize) -> Result<JsonValue> {
    if array.is_null(row_idx) {
        return Ok(JsonValue::Null);
    }

    match array.data_type() {
        DataType::Null => Ok(JsonValue::Null),
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("expected BooleanArray"))?;
            Ok(JsonValue::Bool(a.value(row_idx)))
        }
        DataType::Int8 => {
            let a = downcast::<Int8Array>(array)?;
            Ok(JsonValue::Number(JsonNumber::from(a.value(row_idx) as i64)))
        }
        DataType::Int16 => {
            let a = downcast::<Int16Array>(array)?;
            Ok(JsonValue::Number(JsonNumber::from(a.value(row_idx) as i64)))
        }
        DataType::Int32 => {
            let a = downcast::<Int32Array>(array)?;
            Ok(JsonValue::Number(JsonNumber::from(a.value(row_idx) as i64)))
        }
        DataType::Int64 => {
            let a = downcast::<Int64Array>(array)?;
            Ok(JsonValue::Number(JsonNumber::from(a.value(row_idx))))
        }
        DataType::UInt8 => {
            let a = downcast::<UInt8Array>(array)?;
            Ok(JsonValue::Number(JsonNumber::from(a.value(row_idx) as u64)))
        }
        DataType::UInt16 => {
            let a = downcast::<UInt16Array>(array)?;
            Ok(JsonValue::Number(JsonNumber::from(a.value(row_idx) as u64)))
        }
        DataType::UInt32 => {
            let a = downcast::<UInt32Array>(array)?;
            Ok(JsonValue::Number(JsonNumber::from(a.value(row_idx) as u64)))
        }
        DataType::UInt64 => {
            let a = downcast::<UInt64Array>(array)?;
            Ok(JsonValue::Number(JsonNumber::from(a.value(row_idx))))
        }
        DataType::Float16 => {
            let a = downcast::<Float16Array>(array)?;
            let f = f32::from(a.value(row_idx));
            Ok(float_to_json(f as f64))
        }
        DataType::Float32 => {
            let a = downcast::<Float32Array>(array)?;
            Ok(float_to_json(a.value(row_idx) as f64))
        }
        DataType::Float64 => {
            let a = downcast::<Float64Array>(array)?;
            Ok(float_to_json(a.value(row_idx)))
        }
        DataType::Utf8 => {
            let a = downcast::<StringArray>(array)?;
            Ok(JsonValue::String(a.value(row_idx).to_string()))
        }
        DataType::LargeUtf8 => {
            let a = downcast::<LargeStringArray>(array)?;
            Ok(JsonValue::String(a.value(row_idx).to_string()))
        }
        DataType::Binary => {
            let a = downcast::<BinaryArray>(array)?;
            Ok(JsonValue::String(hex::encode_bytes(a.value(row_idx))))
        }
        DataType::LargeBinary => {
            let a = downcast::<LargeBinaryArray>(array)?;
            Ok(JsonValue::String(hex::encode_bytes(a.value(row_idx))))
        }
        DataType::FixedSizeBinary(_) => {
            let a = downcast::<FixedSizeBinaryArray>(array)?;
            Ok(JsonValue::String(hex::encode_bytes(a.value(row_idx))))
        }
        DataType::Date32 => {
            let a = downcast::<Date32Array>(array)?;
            let days = a.value(row_idx);
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|d| d.checked_add_signed(chrono::Duration::days(days as i64)))
                .ok_or_else(|| anyhow!("invalid Date32 value {days}"))?;
            Ok(JsonValue::String(date.format("%Y-%m-%d").to_string()))
        }
        DataType::Date64 => {
            let a = downcast::<Date64Array>(array)?;
            let ms = a.value(row_idx);
            let dt = DateTime::<Utc>::from_timestamp_millis(ms)
                .ok_or_else(|| anyhow!("invalid Date64 value {ms}"))?;
            Ok(JsonValue::String(dt.date_naive().format("%Y-%m-%d").to_string()))
        }
        DataType::Timestamp(unit, tz) => timestamp_to_json(array, row_idx, *unit, tz.as_deref()),
        DataType::Decimal128(precision, scale) => {
            let a = downcast::<Decimal128Array>(array)?;
            let raw = a.value(row_idx);
            Ok(JsonValue::String(decimal128_to_string(raw, *precision, *scale)))
        }
        DataType::Decimal256(_, scale) => {
            let a = downcast::<Decimal256Array>(array)?;
            let raw = a.value(row_idx);
            // i256 Display is integer; apply scale manually via string.
            Ok(JsonValue::String(decimal256_to_string(raw, *scale)))
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            list_to_json(array, row_idx)
        }
        DataType::Struct(_) => {
            let a = downcast::<StructArray>(array)?;
            struct_row_to_json(a, row_idx)
        }
        DataType::Map(_, _) => {
            let a = downcast::<MapArray>(array)?;
            map_entry_to_json(a, row_idx)
        }
        DataType::Dictionary(_, _) => {
            // Resolve dictionary values via arrow's string representation fallback.
            Ok(JsonValue::String(format!("{:?}", array.slice(row_idx, 1))))
        }
        other => {
            // Fallback: string-debug so we never drop data silently.
            Ok(JsonValue::String(format!("unsupported:{other:?}")))
        }
    }
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| anyhow!("failed to downcast Arrow array"))
}

fn float_to_json(f: f64) -> JsonValue {
    JsonNumber::from_f64(f)
        .map(JsonValue::Number)
        .unwrap_or_else(|| JsonValue::String(f.to_string()))
}

fn timestamp_to_json(
    array: &dyn Array,
    row_idx: usize,
    unit: TimeUnit,
    tz: Option<&str>,
) -> Result<JsonValue> {
    let (secs, nsecs) = match unit {
        TimeUnit::Second => {
            let a = downcast::<TimestampSecondArray>(array)?;
            (a.value(row_idx), 0i64)
        }
        TimeUnit::Millisecond => {
            let a = downcast::<TimestampMillisecondArray>(array)?;
            let ms = a.value(row_idx);
            (ms / 1_000, (ms % 1_000) * 1_000_000)
        }
        TimeUnit::Microsecond => {
            let a = downcast::<TimestampMicrosecondArray>(array)?;
            let us = a.value(row_idx);
            (us / 1_000_000, (us % 1_000_000) * 1_000)
        }
        TimeUnit::Nanosecond => {
            let a = downcast::<TimestampNanosecondArray>(array)?;
            let ns = a.value(row_idx);
            (ns / 1_000_000_000, ns % 1_000_000_000)
        }
    };

    let nsecs_u32 = if nsecs < 0 {
        // Negative remainder: borrow one second.
        return Err(anyhow!("negative nanosecond remainder in timestamp"));
    } else {
        nsecs as u32
    };

    let dt = DateTime::<Utc>::from_timestamp(secs, nsecs_u32)
        .ok_or_else(|| anyhow!("invalid timestamp secs={secs} nsecs={nsecs_u32}"))?;

    // With timezone → RFC3339; without → RFC3339 in UTC (still valid ISO-8601).
    // Callers that need naive local can strip the Z; we keep a stable string form.
    let _ = tz; // tz is already applied by Arrow physical values when present
    Ok(JsonValue::String(dt.to_rfc3339()))
}

fn decimal128_to_string(raw: i128, _precision: u8, scale: i8) -> String {
    if scale == 0 {
        return raw.to_string();
    }
    let scale = scale as u32;
    let neg = raw < 0;
    let abs = raw.unsigned_abs();
    let factor = 10u128.pow(scale);
    let int_part = abs / factor;
    let frac_part = abs % factor;
    let frac = format!("{:0width$}", frac_part, width = scale as usize);
    if neg {
        format!("-{int_part}.{frac}")
    } else {
        format!("{int_part}.{frac}")
    }
}

fn decimal256_to_string(raw: arrow::datatypes::i256, scale: i8) -> String {
    // Use Display of i256 then insert decimal point.
    let s = raw.to_string();
    if scale == 0 {
        return s;
    }
    let scale = scale as usize;
    let neg = s.starts_with('-');
    let digits = if neg { &s[1..] } else { &s[..] };
    let padded = if digits.len() <= scale {
        format!("{:0>width$}", digits, width = scale + 1)
    } else {
        digits.to_string()
    };
    let split = padded.len() - scale;
    let (int_part, frac_part) = padded.split_at(split);
    if neg {
        format!("-{int_part}.{frac_part}")
    } else {
        format!("{int_part}.{frac_part}")
    }
}

fn list_to_json(array: &dyn Array, row_idx: usize) -> Result<JsonValue> {
    // Handle List / LargeList / FixedSizeList via typed downcasts.
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        return list_values_to_json(list.value(row_idx).as_ref());
    }
    if let Some(list) = array.as_any().downcast_ref::<arrow::array::LargeListArray>() {
        return list_values_to_json(list.value(row_idx).as_ref());
    }
    if let Some(list) = array.as_any().downcast_ref::<arrow::array::FixedSizeListArray>() {
        return list_values_to_json(list.value(row_idx).as_ref());
    }
    Err(anyhow!("expected list array"))
}

fn list_values_to_json(values: &dyn Array) -> Result<JsonValue> {
    let mut items = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        items.push(array_value_to_json(values, i)?);
    }
    Ok(normalise_property_value(JsonValue::Array(items)))
}

fn struct_row_to_json(array: &StructArray, row_idx: usize) -> Result<JsonValue> {
    let mut map = JsonMap::new();
    for (field, child) in array.fields().iter().zip(array.columns().iter()) {
        let v = array_value_to_json(child.as_ref(), row_idx)?;
        map.insert(field.name().clone(), v);
    }
    // Structs become JSON-string properties for FalkorDB.
    Ok(normalise_property_value(JsonValue::Object(map)))
}

fn map_entry_to_json(array: &MapArray, row_idx: usize) -> Result<JsonValue> {
    let entries = array.value(row_idx);
    // MapArray value is a StructArray of (key, value).
    let mut map = JsonMap::new();
    if let Some(struct_arr) = entries.as_any().downcast_ref::<StructArray>() {
        if struct_arr.num_columns() >= 2 {
            let keys = struct_arr.column(0);
            let vals = struct_arr.column(1);
            for i in 0..struct_arr.len() {
                let key = match array_value_to_json(keys.as_ref(), i)? {
                    JsonValue::String(s) => s,
                    other => other.to_string(),
                };
                let val = array_value_to_json(vals.as_ref(), i)?;
                map.insert(key, val);
            }
        }
    }
    Ok(normalise_property_value(JsonValue::Object(map)))
}

/// Neo4j/FalkorDB only allow property values that are primitives or arrays of primitives.
/// Complex values (objects, nested arrays) are stringified.
pub fn normalise_property_value(value: JsonValue) -> JsonValue {
    fn is_primitive(v: &JsonValue) -> bool {
        matches!(
            v,
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_)
        )
    }

    match value {
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => value,
        JsonValue::Array(arr) => {
            if arr.iter().all(is_primitive) {
                JsonValue::Array(arr)
            } else {
                let json = serde_json::to_string(&JsonValue::Array(arr)).unwrap_or_else(|_| "[]".into());
                JsonValue::String(json)
            }
        }
        JsonValue::Object(_) => {
            let json = serde_json::to_string(&value).unwrap_or_else(|_| "{}".into());
            JsonValue::String(json)
        }
    }
}

/// Tiny hex encoder so we don't need an extra dependency for binary columns.
mod hex {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    pub fn encode_bytes(bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0xf) as usize] as char);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, ListBuilder, StructArray, TimestampMicrosecondArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn batch_from(cols: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<ArrayRef> = cols.into_iter().map(|(_, a)| a).collect();
        RecordBatch::try_new(schema, arrays).expect("batch")
    }

    #[test]
    fn converts_primitives_and_nulls() {
        let batch = batch_from(vec![
            ("id", Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef),
            (
                "name",
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("alice"),
                    Some("bob"),
                    None,
                ])) as ArrayRef,
            ),
            (
                "score",
                Arc::new(Float64Array::from(vec![Some(1.5), Some(2.0), None])) as ArrayRef,
            ),
            (
                "active",
                Arc::new(arrow::array::BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    None,
                ])) as ArrayRef,
            ),
        ]);

        let rows = record_batch_to_logical_rows(&batch).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("id"), Some(&JsonValue::from(1)));
        assert_eq!(rows[0].get("name"), Some(&JsonValue::String("alice".into())));
        assert_eq!(rows[0].get("active"), Some(&JsonValue::Bool(true)));
        assert_eq!(rows[1].get("id"), Some(&JsonValue::Null));
        assert_eq!(rows[2].get("name"), Some(&JsonValue::Null));
    }

    #[test]
    fn converts_timestamp_to_rfc3339() {
        // 2024-01-01T00:00:00Z in microseconds
        let us = 1_704_067_200_000_000i64;
        let arr = TimestampMicrosecondArray::from(vec![Some(us), None]);
        let batch = batch_from(vec![("ts", Arc::new(arr) as ArrayRef)]);
        let rows = record_batch_to_logical_rows(&batch).unwrap();
        let s = rows[0]["ts"].as_str().unwrap();
        assert!(s.starts_with("2024-01-01T00:00:00"), "got {s}");
        assert_eq!(rows[1]["ts"], JsonValue::Null);
    }

    #[test]
    fn converts_decimal128_as_string() {
        // 123.45 with precision 5 scale 2 → raw = 12345
        let arr = Decimal128Array::from(vec![Some(12345i128), Some(-100i128)])
            .with_precision_and_scale(5, 2)
            .unwrap();
        let batch = batch_from(vec![("amount", Arc::new(arr) as ArrayRef)]);
        let rows = record_batch_to_logical_rows(&batch).unwrap();
        assert_eq!(rows[0]["amount"], JsonValue::String("123.45".into()));
        assert_eq!(rows[1]["amount"], JsonValue::String("-1.00".into()));
    }

    #[test]
    fn converts_date32() {
        // days since epoch for 2020-01-02 = 18263
        let arr = Date32Array::from(vec![Some(18263)]);
        let batch = batch_from(vec![("d", Arc::new(arr) as ArrayRef)]);
        let rows = record_batch_to_logical_rows(&batch).unwrap();
        assert_eq!(rows[0]["d"], JsonValue::String("2020-01-02".into()));
    }

    #[test]
    fn list_of_primitives_stays_array() {
        let mut builder = ListBuilder::new(Int32Array::builder(4));
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.append(true);
        builder.values().append_value(3);
        builder.append(true);
        let arr = builder.finish();
        let batch = batch_from(vec![("tags", Arc::new(arr) as ArrayRef)]);
        let rows = record_batch_to_logical_rows(&batch).unwrap();
        assert_eq!(
            rows[0]["tags"],
            JsonValue::Array(vec![JsonValue::from(1), JsonValue::from(2)])
        );
        assert_eq!(rows[1]["tags"], JsonValue::Array(vec![JsonValue::from(3)]));
    }

    #[test]
    fn struct_becomes_json_string() {
        let ids = Int32Array::from(vec![Some(10)]);
        let names = arrow::array::StringArray::from(vec![Some("x")]);
        let struct_arr = StructArray::from(vec![
            (
                Arc::new(Field::new("id", DataType::Int32, true)),
                Arc::new(ids) as ArrayRef,
            ),
            (
                Arc::new(Field::new("name", DataType::Utf8, true)),
                Arc::new(names) as ArrayRef,
            ),
        ]);
        let batch = batch_from(vec![("meta", Arc::new(struct_arr) as ArrayRef)]);
        let rows = record_batch_to_logical_rows(&batch).unwrap();
        let s = rows[0]["meta"].as_str().expect("struct should be stringified");
        assert!(s.contains("\"id\""));
        assert!(s.contains("10"));
    }

    #[test]
    fn normalise_nested_array_stringifies() {
        let nested = JsonValue::Array(vec![JsonValue::Array(vec![JsonValue::from(1)])]);
        let out = normalise_property_value(nested);
        assert!(out.is_string());
    }

    #[test]
    fn binary_becomes_hex() {
        let arr = BinaryArray::from(vec![Some(b"hi".as_slice())]);
        let batch = batch_from(vec![("b", Arc::new(arr) as ArrayRef)]);
        let rows = record_batch_to_logical_rows(&batch).unwrap();
        assert_eq!(rows[0]["b"], JsonValue::String("6869".into()));
    }
}
