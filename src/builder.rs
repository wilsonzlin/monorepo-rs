use arrow::array::ArrowPrimitiveType;
use arrow::array::ListBuilder;
use arrow::array::PrimitiveBuilder;
use arrow::array::StringBuilder;
use arrow::array::StructBuilder;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use std::sync::Arc;

pub fn list_of(elem_datatype: DataType) -> DataType {
  // "item" must always be nullable apparently, as list builders seem to always define its type as nullable.
  DataType::List(Arc::new(Field::new("item", elem_datatype, true)))
}

pub fn append_list_of_structs<T>(
  list_builder: &mut ListBuilder<StructBuilder>,
  struct_appender: fn(&mut StructBuilder, T),
  items: Vec<T>,
) {
  let struct_builder = list_builder.values();
  for item in items {
    struct_appender(struct_builder, item);
  }
  list_builder.append(true);
}

pub fn append_optional_struct<T>(
  struct_builder: &mut StructBuilder,
  some_struct_appender: fn(&mut StructBuilder, T),
  none_struct_appender: fn(&mut StructBuilder),
  item: Option<T>,
) {
  match item {
    Some(item) => some_struct_appender(struct_builder, item),
    None => none_struct_appender(struct_builder),
  }
}

pub fn append_list_of_strings<S: AsRef<str>>(
  list_builder: &mut ListBuilder<StringBuilder>,
  items: impl IntoIterator<Item = S>,
) {
  let string_builder = list_builder.values();
  for item in items {
    string_builder.append_value(item);
  }
  list_builder.append(true);
}

pub fn append_list_of_primitives<T: ArrowPrimitiveType>(
  list_builder: &mut ListBuilder<PrimitiveBuilder<T>>,
  items: impl IntoIterator<Item = T::Native>,
) {
  let primitive_builder = list_builder.values();
  for item in items {
    primitive_builder.append_value(item);
  }
  list_builder.append(true);
}
