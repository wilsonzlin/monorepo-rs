use struct_name::StructName;
use struct_name_macro::StructName;

#[derive(StructName)]
struct MyStruct {}

#[test]
fn test_derive() {
    assert_eq!(MyStruct::struct_name(), "MyStruct");
}
