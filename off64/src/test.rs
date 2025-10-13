use crate::int::create_i8;
use crate::int::create_u128_be;
use crate::int::create_u128_le;
use crate::int::create_u24_le;
use crate::int::create_u8;
use crate::int::Off64ReadInt;
use crate::int::Off64WriteMutInt;

#[test]
fn test_int_functions() {
  let mut raw = create_u24_le(987654).to_vec();
  assert_eq!(raw, [0x06, 0x12, 0x0f]);
  assert_eq!(raw.read_u24_le_at(0), 987654);
  assert_eq!(raw.read_u24_be_at(0), 397839);
  raw.write_i16_be_at(1, 1000);
  assert_eq!(raw, [0x06, 0x03, 0xe8]);

  raw.push(0);
  raw.push(0);
  raw.push(0);
  raw.push(0);
  raw.write_u48_be_at(1, 115922130);
}

#[test]
fn test_u8_i8_functions() {
  // Test u8
  let raw = create_u8(255);
  assert_eq!(raw, [255]);
  assert_eq!(raw.read_u8_at(0), 255);

  let mut raw = vec![0u8];
  raw.write_u8_at(0, 123);
  assert_eq!(raw[0], 123);

  // Test i8
  let raw = create_i8(-100);
  assert_eq!(raw, [156]);
  assert_eq!(raw.read_i8_at(0), -100);

  let mut raw = vec![0u8];
  raw.write_i8_at(0, -50);
  assert_eq!(raw.read_i8_at(0), -50);
}

#[test]
fn test_u128_i128_functions() {
  // Test u128 big-endian
  let value: u128 = 0x0102030405060708090a0b0c0d0e0f10;
  let raw = create_u128_be(value);
  assert_eq!(raw, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
  assert_eq!(raw.read_u128_be_at(0), value);

  // Test u128 little-endian
  let raw = create_u128_le(value);
  assert_eq!(raw, [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
  assert_eq!(raw.read_u128_le_at(0), value);

  // Test i128
  let value: i128 = -1234567890123456789;
  let mut raw = vec![0u8; 16];
  raw.write_i128_le_at(0, value);
  assert_eq!(raw.read_i128_le_at(0), value);

  raw.write_i128_be_at(0, value);
  assert_eq!(raw.read_i128_be_at(0), value);
}
