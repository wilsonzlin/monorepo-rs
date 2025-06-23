use arrow::array::Array;
use arrow::array::ArrowPrimitiveType;
use arrow::array::AsArray;
use arrow::array::RecordBatch;
use arrow::datatypes::ArrowDictionaryKeyType;
use arrow::datatypes::DataType;
use arrow::datatypes::Date32Type;
use arrow::datatypes::Date64Type;
use arrow::datatypes::Float16Type;
use arrow::datatypes::Float32Type;
use arrow::datatypes::Float64Type;
use arrow::datatypes::Int8Type;
use arrow::datatypes::Int16Type;
use arrow::datatypes::Int32Type;
use arrow::datatypes::Int64Type;
use arrow::datatypes::Time32MillisecondType;
use arrow::datatypes::Time32SecondType;
use arrow::datatypes::Time64MicrosecondType;
use arrow::datatypes::Time64NanosecondType;
use arrow::datatypes::TimeUnit;
use arrow::datatypes::TimestampMicrosecondType;
use arrow::datatypes::TimestampMillisecondType;
use arrow::datatypes::TimestampNanosecondType;
use arrow::datatypes::TimestampSecondType;
use arrow::datatypes::UInt8Type;
use arrow::datatypes::UInt16Type;
use arrow::datatypes::UInt32Type;
use arrow::datatypes::UInt64Type;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use serde_json::Map;
use serde_json::Number;
use serde_json::Value;
use std::iter::zip;

/// Converts an Arrow `RecordBatch` into a `Vec<serde_json::Value>`, where each Value is a JSON object representing a row.
///
/// # Arguments
/// * `batch`: The `RecordBatch` to convert.
///
/// # Errors
/// Returns `ArrowToJsonError` if any value cannot be converted to its JSON representation
/// or if the Arrow data types are unsupported.
pub fn record_batch_to_json_array(batch: &RecordBatch) -> Vec<Value> {
  let num_rows = batch.num_rows();
  let mut objs = Vec::with_capacity(num_rows);
  let schema = batch.schema();
  let columns = batch.columns();

  for row_idx in 0..num_rows {
    let mut row_map = Map::with_capacity(batch.num_columns());

    for (field, array) in zip(schema.fields(), columns) {
      let col_name = field.name().clone();
      let value = array_value_to_json(array, row_idx);
      row_map.insert(col_name, value);
    }
    objs.push(Value::Object(row_map));
  }

  objs
}

fn array_to_json(array: &dyn Array) -> Vec<Value> {
  let mut objs = Vec::with_capacity(array.len());
  for i in 0..array.len() {
    let value = array_value_to_json(array, i);
    objs.push(value);
  }
  objs
}

// Helper function to convert a single Arrow array value at a given index to a JSON Value.
// Path should include this row (i.e. `index`).
fn array_value_to_json(
  array: &dyn Array,
  index: usize,
) -> Value {
  if array.is_null(index) {
    return Value::Null;
  }

  fn handle_int<A>(array: &dyn Array, index: usize) -> Value
  where
    A: ArrowPrimitiveType,
    A::Native: Into<Number>,
  {
    let array = array.as_primitive::<A>();
    let raw = array.value(index);
    Value::Number(raw.into())
  }

  fn handle_float<A>(
    array: &dyn Array,
    index: usize,
  ) -> Value
  where
    A: ArrowPrimitiveType,
    A::Native: Into<f64>,
  {
    let array = array.as_primitive::<A>();
    let raw = array.value(index);
    match Number::from_f64(raw.into()) {
      Some(n) => Value::Number(n),
      None => Value::Null,
    }
  }

  fn as_dt<A>(array: &dyn Array, index: usize, mul_for_nanos: i64) -> DateTime<Utc>
  where
    A: ArrowPrimitiveType,
    A::Native: Into<i64>,
  {
    let array = array.as_primitive::<A>();
    let raw = array.value(index).into();
    let nanos = raw * mul_for_nanos;
    Utc.timestamp_nanos(nanos)
  }

  #[rustfmt::skip]
  let value = match array.data_type() {
    DataType::Null => Value::Null,
    DataType::Boolean => Value::Bool(array.as_boolean().value(index)),
    DataType::Int8 => handle_int::<Int8Type>(array, index),
    DataType::Int16 => handle_int::<Int16Type>(array, index),
    DataType::Int32 => handle_int::<Int32Type>(array, index),
    DataType::Int64 => handle_int::<Int64Type>(array, index),
    DataType::UInt8 => handle_int::<UInt8Type>(array, index),
    DataType::UInt16 => handle_int::<UInt16Type>(array, index),
    DataType::UInt32 => handle_int::<UInt32Type>(array, index),
    DataType::UInt64 => handle_int::<UInt64Type>(array, index),
    DataType::Float16 => handle_float::<Float16Type>(array, index),
    DataType::Float32 => handle_float::<Float32Type>(array, index),
    DataType::Float64 => handle_float::<Float64Type>(array, index),

    // Temporal types to ISO strings
    DataType::Timestamp(TimeUnit::Microsecond, _) => {
      let ts = as_dt::<TimestampMicrosecondType>(array, index, 1_000_000);
      Value::String(ts.to_rfc3339())
    },
    DataType::Timestamp(TimeUnit::Millisecond, _) => {
      let ts = as_dt::<TimestampMillisecondType>(array, index, 1_000);
      Value::String(ts.to_rfc3339())
    },
    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
      let ts = as_dt::<TimestampNanosecondType>(array, index, 1);
      Value::String(ts.to_rfc3339())
    },
    DataType::Timestamp(TimeUnit::Second, _) => {
      let ts = as_dt::<TimestampSecondType>(array, index, 1_000_000_000);
      Value::String(ts.to_rfc3339())
    },
    DataType::Date32 => {
      let ts = as_dt::<Date32Type>(array, index, 86_400_000_000_000);
      Value::String(ts.date_naive().to_string())
    }
    DataType::Date64 => {
      let ts = as_dt::<Date64Type>(array, index, 1_000);
      Value::String(ts.date_naive().to_string())
    }
    DataType::Time32(TimeUnit::Second) => {
      let ts = as_dt::<Time32SecondType>(array, index, 1);
      Value::String(ts.time().to_string())
    }
    DataType::Time32(TimeUnit::Millisecond) => {
      let ts = as_dt::<Time32MillisecondType>(array, index, 1_000);
      Value::String(ts.time().to_string())
    }
    DataType::Time64(TimeUnit::Microsecond) => {
      let ts = as_dt::<Time64MicrosecondType>(array, index, 1_000_000);
      Value::String(ts.time().to_string())
    }
    DataType::Time64(TimeUnit::Nanosecond) => {
      let ts = as_dt::<Time64NanosecondType>(array, index, 1_000_000_000);
      Value::String(ts.time().to_string())
    }

    // String/Binary types
    DataType::Utf8 => {
      let array = array.as_string::<i32>();
      Value::String(array.value(index).to_string())
    }
    DataType::LargeUtf8 => {
      let array = array.as_string::<i64>();
      Value::String(array.value(index).to_string())
    }
    DataType::Binary => {
      let arr = array.as_binary::<i32>();
      Value::String(BASE64_STANDARD.encode(arr.value(index)))
    }
     DataType::LargeBinary => {
      let arr = array.as_binary::<i64>();
      Value::String(BASE64_STANDARD.encode(arr.value(index)))
    }
    DataType::FixedSizeBinary(_) => {
       let arr = array.as_fixed_size_binary();
       Value::String(BASE64_STANDARD.encode(arr.value(index)))
    }

    // Nested types
    DataType::List(_field) => {
      let list_arr = array.as_list::<i32>();
      let nested_array = list_arr.value(index);
      Value::Array(array_to_json(&nested_array))
    }
     DataType::LargeList(_field) => {
      let list_arr = array.as_list::<i64>();
      let nested_array = list_arr.value(index);
      Value::Array(array_to_json(&nested_array))
    }
    DataType::FixedSizeList(_field, _size) => {
       let list_arr = array.as_fixed_size_list();
       let nested_array = list_arr.value(index);
       Value::Array(array_to_json(&nested_array))
    }
    DataType::Struct(fields) => {
      let struct_arr = array.as_struct();
      let mut struct_map = Map::with_capacity(fields.len());
      for (field_idx, field) in fields.iter().enumerate() {
        let field_name = field.name().clone();
        let field_array = struct_arr.column(field_idx);
        let val = array_value_to_json(field_array, index);
        struct_map.insert(field_name, val);
      }
      Value::Object(struct_map)
    }
    DataType::Map(_field, _ordered) => {
       let map_arr = array.as_map();
       let keys_arr = map_arr.keys();
       let values_arr = map_arr.values();
       let offsets = map_arr.offsets();
       let start_offset: usize = offsets[index].try_into().unwrap();
       let end_offset: usize = offsets[index + 1].try_into().unwrap();
       let n = end_offset - start_offset;

       let mut map_obj = Map::new();

       for entry_no in 0..n {
        let j = start_offset + entry_no;
         // Convert key to string. This might fail for non-string-compatible key types.
         // JSON object keys MUST be strings.
         let key_val = array_value_to_json(keys_arr, j);
         let key_str = match key_val {
           Value::String(s) => s,
           Value::Number(n) => n.to_string(),
           Value::Bool(b) => b.to_string(),
           // Other types are generally not suitable as JSON keys
           _ => continue,
         };

         let value = array_value_to_json(values_arr, j);
         map_obj.insert(key_str, value);
       }
       Value::Object(map_obj)
    }
    DataType::Dictionary(key_type, _value_type) => {
      fn handle_dict<I>(array: &dyn Array, index: usize) -> Value
        where I: ArrowDictionaryKeyType,
              I::Native: TryInto<usize>,
      {
        let dict_arr = array.as_dictionary::<I>();
        let key_index = dict_arr.keys().value(index);
        let values = dict_arr.values();
        array_value_to_json(values, key_index.try_into().ok().unwrap())
      }

      match **key_type {
        DataType::Int8 => handle_dict::<Int8Type>(array, index),
        DataType::Int16 => handle_dict::<Int16Type>(array, index),
        DataType::Int32 => handle_dict::<Int32Type>(array, index),
        DataType::Int64 => handle_dict::<Int64Type>(array, index),
        DataType::UInt8 => handle_dict::<UInt8Type>(array, index),
        DataType::UInt16 => handle_dict::<UInt16Type>(array, index),
        DataType::UInt32 => handle_dict::<UInt32Type>(array, index),
        DataType::UInt64 => handle_dict::<UInt64Type>(array, index),
        // Unsupported key types
        _ => Value::Null,
      }
    }

    // Unsupported types
    _ => Value::Null,
  };

  value
}
