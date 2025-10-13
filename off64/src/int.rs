use crate::*;

// Short convenient macros for converting between int types without using the unsafe `as` operator.
#[macro_export]
macro_rules! isz {
  ($v:expr) => {
    isize::try_from($v).unwrap()
  };
}
#[macro_export]
macro_rules! i8 {
  ($v:expr) => {
    i8::try_from($v).unwrap()
  };
}
#[macro_export]
macro_rules! i16 {
  ($v:expr) => {
    i16::try_from($v).unwrap()
  };
}
#[macro_export]
macro_rules! i32 {
  ($v:expr) => {
    i32::try_from($v).unwrap()
  };
}
#[macro_export]
macro_rules! i64 {
  ($v:expr) => {
    i64::try_from($v).unwrap()
  };
}
#[macro_export]
macro_rules! usz {
  ($v:expr) => {
    usize::try_from($v).unwrap()
  };
}
#[macro_export]
macro_rules! u8 {
  ($v:expr) => {
    u8::try_from($v).unwrap()
  };
}
#[macro_export]
macro_rules! u16 {
  ($v:expr) => {
    u16::try_from($v).unwrap()
  };
}
#[macro_export]
macro_rules! u32 {
  ($v:expr) => {
    u32::try_from($v).unwrap()
  };
}
#[macro_export]
macro_rules! u64 {
  ($v:expr) => {
    u64::try_from($v).unwrap()
  };
}
#[macro_export]
macro_rules! i128 {
  ($v:expr) => {
    i128::try_from($v).unwrap()
  };
}
#[macro_export]
macro_rules! u128 {
  ($v:expr) => {
    u128::try_from($v).unwrap()
  };
}
pub trait Off64ReadInt<'a, T: 'a + AsRef<[u8]>>: Off64Read<'a, T> {
  fn read_i8_at(&'a self, offset: u64) -> i8 {
    self.read_at(offset, 1).as_ref()[0] as i8
  }

  fn read_u8_at(&'a self, offset: u64) -> u8 {
    self.read_at(offset, 1).as_ref()[0] as u8
  }

  fn read_i16_be_at(&'a self, offset: u64) -> i16 {
    let mut buf = [0u8; 2];
    buf[0..2].copy_from_slice(self.read_at(offset, 2).as_ref());
    i16::from_be_bytes(buf)
  }

  fn read_i16_le_at(&'a self, offset: u64) -> i16 {
    let mut buf = [0u8; 2];
    buf[0..2].copy_from_slice(self.read_at(offset, 2).as_ref());
    i16::from_le_bytes(buf)
  }

  fn read_u16_be_at(&'a self, offset: u64) -> u16 {
    let mut buf = [0u8; 2];
    buf[0..2].copy_from_slice(self.read_at(offset, 2).as_ref());
    u16::from_be_bytes(buf)
  }

  fn read_u16_le_at(&'a self, offset: u64) -> u16 {
    let mut buf = [0u8; 2];
    buf[0..2].copy_from_slice(self.read_at(offset, 2).as_ref());
    u16::from_le_bytes(buf)
  }

  fn read_i24_be_at(&'a self, offset: u64) -> i32 {
    let mut buf = [0u8; 4];
    buf[1..4].copy_from_slice(self.read_at(offset, 3).as_ref());
    i32::from_be_bytes(buf)
  }

  fn read_i24_le_at(&'a self, offset: u64) -> i32 {
    let mut buf = [0u8; 4];
    buf[0..3].copy_from_slice(self.read_at(offset, 3).as_ref());
    i32::from_le_bytes(buf)
  }

  fn read_u24_be_at(&'a self, offset: u64) -> u32 {
    let mut buf = [0u8; 4];
    buf[1..4].copy_from_slice(self.read_at(offset, 3).as_ref());
    u32::from_be_bytes(buf)
  }

  fn read_u24_le_at(&'a self, offset: u64) -> u32 {
    let mut buf = [0u8; 4];
    buf[0..3].copy_from_slice(self.read_at(offset, 3).as_ref());
    u32::from_le_bytes(buf)
  }

  fn read_i32_be_at(&'a self, offset: u64) -> i32 {
    let mut buf = [0u8; 4];
    buf[0..4].copy_from_slice(self.read_at(offset, 4).as_ref());
    i32::from_be_bytes(buf)
  }

  fn read_i32_le_at(&'a self, offset: u64) -> i32 {
    let mut buf = [0u8; 4];
    buf[0..4].copy_from_slice(self.read_at(offset, 4).as_ref());
    i32::from_le_bytes(buf)
  }

  fn read_u32_be_at(&'a self, offset: u64) -> u32 {
    let mut buf = [0u8; 4];
    buf[0..4].copy_from_slice(self.read_at(offset, 4).as_ref());
    u32::from_be_bytes(buf)
  }

  fn read_u32_le_at(&'a self, offset: u64) -> u32 {
    let mut buf = [0u8; 4];
    buf[0..4].copy_from_slice(self.read_at(offset, 4).as_ref());
    u32::from_le_bytes(buf)
  }

  fn read_i40_be_at(&'a self, offset: u64) -> i64 {
    let mut buf = [0u8; 8];
    buf[3..8].copy_from_slice(self.read_at(offset, 5).as_ref());
    i64::from_be_bytes(buf)
  }

  fn read_i40_le_at(&'a self, offset: u64) -> i64 {
    let mut buf = [0u8; 8];
    buf[0..5].copy_from_slice(self.read_at(offset, 5).as_ref());
    i64::from_le_bytes(buf)
  }

  fn read_u40_be_at(&'a self, offset: u64) -> u64 {
    let mut buf = [0u8; 8];
    buf[3..8].copy_from_slice(self.read_at(offset, 5).as_ref());
    u64::from_be_bytes(buf)
  }

  fn read_u40_le_at(&'a self, offset: u64) -> u64 {
    let mut buf = [0u8; 8];
    buf[0..5].copy_from_slice(self.read_at(offset, 5).as_ref());
    u64::from_le_bytes(buf)
  }

  fn read_i48_be_at(&'a self, offset: u64) -> i64 {
    let mut buf = [0u8; 8];
    buf[2..8].copy_from_slice(self.read_at(offset, 6).as_ref());
    i64::from_be_bytes(buf)
  }

  fn read_i48_le_at(&'a self, offset: u64) -> i64 {
    let mut buf = [0u8; 8];
    buf[0..6].copy_from_slice(self.read_at(offset, 6).as_ref());
    i64::from_le_bytes(buf)
  }

  fn read_u48_be_at(&'a self, offset: u64) -> u64 {
    let mut buf = [0u8; 8];
    buf[2..8].copy_from_slice(self.read_at(offset, 6).as_ref());
    u64::from_be_bytes(buf)
  }

  fn read_u48_le_at(&'a self, offset: u64) -> u64 {
    let mut buf = [0u8; 8];
    buf[0..6].copy_from_slice(self.read_at(offset, 6).as_ref());
    u64::from_le_bytes(buf)
  }

  fn read_i56_be_at(&'a self, offset: u64) -> i64 {
    let mut buf = [0u8; 8];
    buf[1..8].copy_from_slice(self.read_at(offset, 7).as_ref());
    i64::from_be_bytes(buf)
  }

  fn read_i56_le_at(&'a self, offset: u64) -> i64 {
    let mut buf = [0u8; 8];
    buf[0..7].copy_from_slice(self.read_at(offset, 7).as_ref());
    i64::from_le_bytes(buf)
  }

  fn read_u56_be_at(&'a self, offset: u64) -> u64 {
    let mut buf = [0u8; 8];
    buf[1..8].copy_from_slice(self.read_at(offset, 7).as_ref());
    u64::from_be_bytes(buf)
  }

  fn read_u56_le_at(&'a self, offset: u64) -> u64 {
    let mut buf = [0u8; 8];
    buf[0..7].copy_from_slice(self.read_at(offset, 7).as_ref());
    u64::from_le_bytes(buf)
  }

  fn read_i64_be_at(&'a self, offset: u64) -> i64 {
    let mut buf = [0u8; 8];
    buf[0..8].copy_from_slice(self.read_at(offset, 8).as_ref());
    i64::from_be_bytes(buf)
  }

  fn read_i64_le_at(&'a self, offset: u64) -> i64 {
    let mut buf = [0u8; 8];
    buf[0..8].copy_from_slice(self.read_at(offset, 8).as_ref());
    i64::from_le_bytes(buf)
  }

  fn read_u64_be_at(&'a self, offset: u64) -> u64 {
    let mut buf = [0u8; 8];
    buf[0..8].copy_from_slice(self.read_at(offset, 8).as_ref());
    u64::from_be_bytes(buf)
  }

  fn read_u64_le_at(&'a self, offset: u64) -> u64 {
    let mut buf = [0u8; 8];
    buf[0..8].copy_from_slice(self.read_at(offset, 8).as_ref());
    u64::from_le_bytes(buf)
  }

  fn read_i72_be_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[7..16].copy_from_slice(self.read_at(offset, 9).as_ref());
    i128::from_be_bytes(buf)
  }

  fn read_i72_le_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..9].copy_from_slice(self.read_at(offset, 9).as_ref());
    i128::from_le_bytes(buf)
  }

  fn read_u72_be_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[7..16].copy_from_slice(self.read_at(offset, 9).as_ref());
    u128::from_be_bytes(buf)
  }

  fn read_u72_le_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..9].copy_from_slice(self.read_at(offset, 9).as_ref());
    u128::from_le_bytes(buf)
  }

  fn read_i80_be_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[6..16].copy_from_slice(self.read_at(offset, 10).as_ref());
    i128::from_be_bytes(buf)
  }

  fn read_i80_le_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..10].copy_from_slice(self.read_at(offset, 10).as_ref());
    i128::from_le_bytes(buf)
  }

  fn read_u80_be_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[6..16].copy_from_slice(self.read_at(offset, 10).as_ref());
    u128::from_be_bytes(buf)
  }

  fn read_u80_le_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..10].copy_from_slice(self.read_at(offset, 10).as_ref());
    u128::from_le_bytes(buf)
  }

  fn read_i88_be_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[5..16].copy_from_slice(self.read_at(offset, 11).as_ref());
    i128::from_be_bytes(buf)
  }

  fn read_i88_le_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..11].copy_from_slice(self.read_at(offset, 11).as_ref());
    i128::from_le_bytes(buf)
  }

  fn read_u88_be_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[5..16].copy_from_slice(self.read_at(offset, 11).as_ref());
    u128::from_be_bytes(buf)
  }

  fn read_u88_le_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..11].copy_from_slice(self.read_at(offset, 11).as_ref());
    u128::from_le_bytes(buf)
  }

  fn read_i96_be_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[4..16].copy_from_slice(self.read_at(offset, 12).as_ref());
    i128::from_be_bytes(buf)
  }

  fn read_i96_le_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..12].copy_from_slice(self.read_at(offset, 12).as_ref());
    i128::from_le_bytes(buf)
  }

  fn read_u96_be_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[4..16].copy_from_slice(self.read_at(offset, 12).as_ref());
    u128::from_be_bytes(buf)
  }

  fn read_u96_le_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..12].copy_from_slice(self.read_at(offset, 12).as_ref());
    u128::from_le_bytes(buf)
  }

  fn read_i104_be_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[3..16].copy_from_slice(self.read_at(offset, 13).as_ref());
    i128::from_be_bytes(buf)
  }

  fn read_i104_le_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..13].copy_from_slice(self.read_at(offset, 13).as_ref());
    i128::from_le_bytes(buf)
  }

  fn read_u104_be_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[3..16].copy_from_slice(self.read_at(offset, 13).as_ref());
    u128::from_be_bytes(buf)
  }

  fn read_u104_le_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..13].copy_from_slice(self.read_at(offset, 13).as_ref());
    u128::from_le_bytes(buf)
  }

  fn read_i112_be_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[2..16].copy_from_slice(self.read_at(offset, 14).as_ref());
    i128::from_be_bytes(buf)
  }

  fn read_i112_le_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..14].copy_from_slice(self.read_at(offset, 14).as_ref());
    i128::from_le_bytes(buf)
  }

  fn read_u112_be_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[2..16].copy_from_slice(self.read_at(offset, 14).as_ref());
    u128::from_be_bytes(buf)
  }

  fn read_u112_le_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..14].copy_from_slice(self.read_at(offset, 14).as_ref());
    u128::from_le_bytes(buf)
  }

  fn read_i120_be_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[1..16].copy_from_slice(self.read_at(offset, 15).as_ref());
    i128::from_be_bytes(buf)
  }

  fn read_i120_le_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..15].copy_from_slice(self.read_at(offset, 15).as_ref());
    i128::from_le_bytes(buf)
  }

  fn read_u120_be_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[1..16].copy_from_slice(self.read_at(offset, 15).as_ref());
    u128::from_be_bytes(buf)
  }

  fn read_u120_le_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..15].copy_from_slice(self.read_at(offset, 15).as_ref());
    u128::from_le_bytes(buf)
  }

  fn read_i128_be_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..16].copy_from_slice(self.read_at(offset, 16).as_ref());
    i128::from_be_bytes(buf)
  }

  fn read_i128_le_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..16].copy_from_slice(self.read_at(offset, 16).as_ref());
    i128::from_le_bytes(buf)
  }

  fn read_u128_be_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..16].copy_from_slice(self.read_at(offset, 16).as_ref());
    u128::from_be_bytes(buf)
  }

  fn read_u128_le_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..16].copy_from_slice(self.read_at(offset, 16).as_ref());
    u128::from_le_bytes(buf)
  }
}
#[async_trait::async_trait]
pub trait Off64AsyncReadInt<'a, T: 'a + AsRef<[u8]>>: Off64AsyncRead<'a, T> {
  async fn read_i8_at(&'a self, offset: u64) -> i8 {
    self.read_at(offset, 1).await.as_ref()[0] as i8
  }

  async fn read_u8_at(&'a self, offset: u64) -> u8 {
    self.read_at(offset, 1).await.as_ref()[0] as u8
  }

  async fn read_i16_be_at(&'a self, offset: u64) -> i16 {
    let mut buf = [0u8; 2];
    buf[0..2].copy_from_slice(self.read_at(offset, 2).await.as_ref());
    i16::from_be_bytes(buf)
  }

  async fn read_i16_le_at(&'a self, offset: u64) -> i16 {
    let mut buf = [0u8; 2];
    buf[0..2].copy_from_slice(self.read_at(offset, 2).await.as_ref());
    i16::from_le_bytes(buf)
  }

  async fn read_u16_be_at(&'a self, offset: u64) -> u16 {
    let mut buf = [0u8; 2];
    buf[0..2].copy_from_slice(self.read_at(offset, 2).await.as_ref());
    u16::from_be_bytes(buf)
  }

  async fn read_u16_le_at(&'a self, offset: u64) -> u16 {
    let mut buf = [0u8; 2];
    buf[0..2].copy_from_slice(self.read_at(offset, 2).await.as_ref());
    u16::from_le_bytes(buf)
  }

  async fn read_i24_be_at(&'a self, offset: u64) -> i32 {
    let mut buf = [0u8; 4];
    buf[1..4].copy_from_slice(self.read_at(offset, 3).await.as_ref());
    i32::from_be_bytes(buf)
  }

  async fn read_i24_le_at(&'a self, offset: u64) -> i32 {
    let mut buf = [0u8; 4];
    buf[0..3].copy_from_slice(self.read_at(offset, 3).await.as_ref());
    i32::from_le_bytes(buf)
  }

  async fn read_u24_be_at(&'a self, offset: u64) -> u32 {
    let mut buf = [0u8; 4];
    buf[1..4].copy_from_slice(self.read_at(offset, 3).await.as_ref());
    u32::from_be_bytes(buf)
  }

  async fn read_u24_le_at(&'a self, offset: u64) -> u32 {
    let mut buf = [0u8; 4];
    buf[0..3].copy_from_slice(self.read_at(offset, 3).await.as_ref());
    u32::from_le_bytes(buf)
  }

  async fn read_i32_be_at(&'a self, offset: u64) -> i32 {
    let mut buf = [0u8; 4];
    buf[0..4].copy_from_slice(self.read_at(offset, 4).await.as_ref());
    i32::from_be_bytes(buf)
  }

  async fn read_i32_le_at(&'a self, offset: u64) -> i32 {
    let mut buf = [0u8; 4];
    buf[0..4].copy_from_slice(self.read_at(offset, 4).await.as_ref());
    i32::from_le_bytes(buf)
  }

  async fn read_u32_be_at(&'a self, offset: u64) -> u32 {
    let mut buf = [0u8; 4];
    buf[0..4].copy_from_slice(self.read_at(offset, 4).await.as_ref());
    u32::from_be_bytes(buf)
  }

  async fn read_u32_le_at(&'a self, offset: u64) -> u32 {
    let mut buf = [0u8; 4];
    buf[0..4].copy_from_slice(self.read_at(offset, 4).await.as_ref());
    u32::from_le_bytes(buf)
  }

  async fn read_i40_be_at(&'a self, offset: u64) -> i64 {
    let mut buf = [0u8; 8];
    buf[3..8].copy_from_slice(self.read_at(offset, 5).await.as_ref());
    i64::from_be_bytes(buf)
  }

  async fn read_i40_le_at(&'a self, offset: u64) -> i64 {
    let mut buf = [0u8; 8];
    buf[0..5].copy_from_slice(self.read_at(offset, 5).await.as_ref());
    i64::from_le_bytes(buf)
  }

  async fn read_u40_be_at(&'a self, offset: u64) -> u64 {
    let mut buf = [0u8; 8];
    buf[3..8].copy_from_slice(self.read_at(offset, 5).await.as_ref());
    u64::from_be_bytes(buf)
  }

  async fn read_u40_le_at(&'a self, offset: u64) -> u64 {
    let mut buf = [0u8; 8];
    buf[0..5].copy_from_slice(self.read_at(offset, 5).await.as_ref());
    u64::from_le_bytes(buf)
  }

  async fn read_i48_be_at(&'a self, offset: u64) -> i64 {
    let mut buf = [0u8; 8];
    buf[2..8].copy_from_slice(self.read_at(offset, 6).await.as_ref());
    i64::from_be_bytes(buf)
  }

  async fn read_i48_le_at(&'a self, offset: u64) -> i64 {
    let mut buf = [0u8; 8];
    buf[0..6].copy_from_slice(self.read_at(offset, 6).await.as_ref());
    i64::from_le_bytes(buf)
  }

  async fn read_u48_be_at(&'a self, offset: u64) -> u64 {
    let mut buf = [0u8; 8];
    buf[2..8].copy_from_slice(self.read_at(offset, 6).await.as_ref());
    u64::from_be_bytes(buf)
  }

  async fn read_u48_le_at(&'a self, offset: u64) -> u64 {
    let mut buf = [0u8; 8];
    buf[0..6].copy_from_slice(self.read_at(offset, 6).await.as_ref());
    u64::from_le_bytes(buf)
  }

  async fn read_i56_be_at(&'a self, offset: u64) -> i64 {
    let mut buf = [0u8; 8];
    buf[1..8].copy_from_slice(self.read_at(offset, 7).await.as_ref());
    i64::from_be_bytes(buf)
  }

  async fn read_i56_le_at(&'a self, offset: u64) -> i64 {
    let mut buf = [0u8; 8];
    buf[0..7].copy_from_slice(self.read_at(offset, 7).await.as_ref());
    i64::from_le_bytes(buf)
  }

  async fn read_u56_be_at(&'a self, offset: u64) -> u64 {
    let mut buf = [0u8; 8];
    buf[1..8].copy_from_slice(self.read_at(offset, 7).await.as_ref());
    u64::from_be_bytes(buf)
  }

  async fn read_u56_le_at(&'a self, offset: u64) -> u64 {
    let mut buf = [0u8; 8];
    buf[0..7].copy_from_slice(self.read_at(offset, 7).await.as_ref());
    u64::from_le_bytes(buf)
  }

  async fn read_i64_be_at(&'a self, offset: u64) -> i64 {
    let mut buf = [0u8; 8];
    buf[0..8].copy_from_slice(self.read_at(offset, 8).await.as_ref());
    i64::from_be_bytes(buf)
  }

  async fn read_i64_le_at(&'a self, offset: u64) -> i64 {
    let mut buf = [0u8; 8];
    buf[0..8].copy_from_slice(self.read_at(offset, 8).await.as_ref());
    i64::from_le_bytes(buf)
  }

  async fn read_u64_be_at(&'a self, offset: u64) -> u64 {
    let mut buf = [0u8; 8];
    buf[0..8].copy_from_slice(self.read_at(offset, 8).await.as_ref());
    u64::from_be_bytes(buf)
  }

  async fn read_u64_le_at(&'a self, offset: u64) -> u64 {
    let mut buf = [0u8; 8];
    buf[0..8].copy_from_slice(self.read_at(offset, 8).await.as_ref());
    u64::from_le_bytes(buf)
  }

  async fn read_i72_be_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[7..16].copy_from_slice(self.read_at(offset, 9).await.as_ref());
    i128::from_be_bytes(buf)
  }

  async fn read_i72_le_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..9].copy_from_slice(self.read_at(offset, 9).await.as_ref());
    i128::from_le_bytes(buf)
  }

  async fn read_u72_be_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[7..16].copy_from_slice(self.read_at(offset, 9).await.as_ref());
    u128::from_be_bytes(buf)
  }

  async fn read_u72_le_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..9].copy_from_slice(self.read_at(offset, 9).await.as_ref());
    u128::from_le_bytes(buf)
  }

  async fn read_i80_be_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[6..16].copy_from_slice(self.read_at(offset, 10).await.as_ref());
    i128::from_be_bytes(buf)
  }

  async fn read_i80_le_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..10].copy_from_slice(self.read_at(offset, 10).await.as_ref());
    i128::from_le_bytes(buf)
  }

  async fn read_u80_be_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[6..16].copy_from_slice(self.read_at(offset, 10).await.as_ref());
    u128::from_be_bytes(buf)
  }

  async fn read_u80_le_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..10].copy_from_slice(self.read_at(offset, 10).await.as_ref());
    u128::from_le_bytes(buf)
  }

  async fn read_i88_be_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[5..16].copy_from_slice(self.read_at(offset, 11).await.as_ref());
    i128::from_be_bytes(buf)
  }

  async fn read_i88_le_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..11].copy_from_slice(self.read_at(offset, 11).await.as_ref());
    i128::from_le_bytes(buf)
  }

  async fn read_u88_be_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[5..16].copy_from_slice(self.read_at(offset, 11).await.as_ref());
    u128::from_be_bytes(buf)
  }

  async fn read_u88_le_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..11].copy_from_slice(self.read_at(offset, 11).await.as_ref());
    u128::from_le_bytes(buf)
  }

  async fn read_i96_be_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[4..16].copy_from_slice(self.read_at(offset, 12).await.as_ref());
    i128::from_be_bytes(buf)
  }

  async fn read_i96_le_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..12].copy_from_slice(self.read_at(offset, 12).await.as_ref());
    i128::from_le_bytes(buf)
  }

  async fn read_u96_be_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[4..16].copy_from_slice(self.read_at(offset, 12).await.as_ref());
    u128::from_be_bytes(buf)
  }

  async fn read_u96_le_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..12].copy_from_slice(self.read_at(offset, 12).await.as_ref());
    u128::from_le_bytes(buf)
  }

  async fn read_i104_be_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[3..16].copy_from_slice(self.read_at(offset, 13).await.as_ref());
    i128::from_be_bytes(buf)
  }

  async fn read_i104_le_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..13].copy_from_slice(self.read_at(offset, 13).await.as_ref());
    i128::from_le_bytes(buf)
  }

  async fn read_u104_be_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[3..16].copy_from_slice(self.read_at(offset, 13).await.as_ref());
    u128::from_be_bytes(buf)
  }

  async fn read_u104_le_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..13].copy_from_slice(self.read_at(offset, 13).await.as_ref());
    u128::from_le_bytes(buf)
  }

  async fn read_i112_be_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[2..16].copy_from_slice(self.read_at(offset, 14).await.as_ref());
    i128::from_be_bytes(buf)
  }

  async fn read_i112_le_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..14].copy_from_slice(self.read_at(offset, 14).await.as_ref());
    i128::from_le_bytes(buf)
  }

  async fn read_u112_be_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[2..16].copy_from_slice(self.read_at(offset, 14).await.as_ref());
    u128::from_be_bytes(buf)
  }

  async fn read_u112_le_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..14].copy_from_slice(self.read_at(offset, 14).await.as_ref());
    u128::from_le_bytes(buf)
  }

  async fn read_i120_be_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[1..16].copy_from_slice(self.read_at(offset, 15).await.as_ref());
    i128::from_be_bytes(buf)
  }

  async fn read_i120_le_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..15].copy_from_slice(self.read_at(offset, 15).await.as_ref());
    i128::from_le_bytes(buf)
  }

  async fn read_u120_be_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[1..16].copy_from_slice(self.read_at(offset, 15).await.as_ref());
    u128::from_be_bytes(buf)
  }

  async fn read_u120_le_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..15].copy_from_slice(self.read_at(offset, 15).await.as_ref());
    u128::from_le_bytes(buf)
  }

  async fn read_i128_be_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..16].copy_from_slice(self.read_at(offset, 16).await.as_ref());
    i128::from_be_bytes(buf)
  }

  async fn read_i128_le_at(&'a self, offset: u64) -> i128 {
    let mut buf = [0u8; 16];
    buf[0..16].copy_from_slice(self.read_at(offset, 16).await.as_ref());
    i128::from_le_bytes(buf)
  }

  async fn read_u128_be_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..16].copy_from_slice(self.read_at(offset, 16).await.as_ref());
    u128::from_be_bytes(buf)
  }

  async fn read_u128_le_at(&'a self, offset: u64) -> u128 {
    let mut buf = [0u8; 16];
    buf[0..16].copy_from_slice(self.read_at(offset, 16).await.as_ref());
    u128::from_le_bytes(buf)
  }
}
#[async_trait::async_trait]
pub trait Off64AsyncWriteMutInt: Off64AsyncWriteMut {
  async fn write_i8_at(&mut self, offset: u64, value: i8) {
    self.write_at(offset, &[value as u8]).await;
  }

  async fn write_u8_at(&mut self, offset: u64, value: u8) {
    self.write_at(offset, &[value as u8]).await;
  }

  async fn write_i16_be_at(&mut self, offset: u64, value: i16) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..2]).await;
  }

  async fn write_i16_le_at(&mut self, offset: u64, value: i16) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..2]).await;
  }

  async fn write_u16_be_at(&mut self, offset: u64, value: u16) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..2]).await;
  }

  async fn write_u16_le_at(&mut self, offset: u64, value: u16) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..2]).await;
  }

  async fn write_i24_be_at(&mut self, offset: u64, value: i32) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..4]).await;
  }

  async fn write_i24_le_at(&mut self, offset: u64, value: i32) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..3]).await;
  }

  async fn write_u24_be_at(&mut self, offset: u64, value: u32) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..4]).await;
  }

  async fn write_u24_le_at(&mut self, offset: u64, value: u32) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..3]).await;
  }

  async fn write_i32_be_at(&mut self, offset: u64, value: i32) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..4]).await;
  }

  async fn write_i32_le_at(&mut self, offset: u64, value: i32) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..4]).await;
  }

  async fn write_u32_be_at(&mut self, offset: u64, value: u32) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..4]).await;
  }

  async fn write_u32_le_at(&mut self, offset: u64, value: u32) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..4]).await;
  }

  async fn write_i40_be_at(&mut self, offset: u64, value: i64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[3..8]).await;
  }

  async fn write_i40_le_at(&mut self, offset: u64, value: i64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..5]).await;
  }

  async fn write_u40_be_at(&mut self, offset: u64, value: u64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[3..8]).await;
  }

  async fn write_u40_le_at(&mut self, offset: u64, value: u64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..5]).await;
  }

  async fn write_i48_be_at(&mut self, offset: u64, value: i64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[2..8]).await;
  }

  async fn write_i48_le_at(&mut self, offset: u64, value: i64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..6]).await;
  }

  async fn write_u48_be_at(&mut self, offset: u64, value: u64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[2..8]).await;
  }

  async fn write_u48_le_at(&mut self, offset: u64, value: u64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..6]).await;
  }

  async fn write_i56_be_at(&mut self, offset: u64, value: i64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..8]).await;
  }

  async fn write_i56_le_at(&mut self, offset: u64, value: i64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..7]).await;
  }

  async fn write_u56_be_at(&mut self, offset: u64, value: u64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..8]).await;
  }

  async fn write_u56_le_at(&mut self, offset: u64, value: u64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..7]).await;
  }

  async fn write_i64_be_at(&mut self, offset: u64, value: i64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..8]).await;
  }

  async fn write_i64_le_at(&mut self, offset: u64, value: i64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..8]).await;
  }

  async fn write_u64_be_at(&mut self, offset: u64, value: u64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..8]).await;
  }

  async fn write_u64_le_at(&mut self, offset: u64, value: u64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..8]).await;
  }

  async fn write_i72_be_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[7..16]).await;
  }

  async fn write_i72_le_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..9]).await;
  }

  async fn write_u72_be_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[7..16]).await;
  }

  async fn write_u72_le_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..9]).await;
  }

  async fn write_i80_be_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[6..16]).await;
  }

  async fn write_i80_le_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..10]).await;
  }

  async fn write_u80_be_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[6..16]).await;
  }

  async fn write_u80_le_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..10]).await;
  }

  async fn write_i88_be_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[5..16]).await;
  }

  async fn write_i88_le_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..11]).await;
  }

  async fn write_u88_be_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[5..16]).await;
  }

  async fn write_u88_le_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..11]).await;
  }

  async fn write_i96_be_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[4..16]).await;
  }

  async fn write_i96_le_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..12]).await;
  }

  async fn write_u96_be_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[4..16]).await;
  }

  async fn write_u96_le_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..12]).await;
  }

  async fn write_i104_be_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[3..16]).await;
  }

  async fn write_i104_le_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..13]).await;
  }

  async fn write_u104_be_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[3..16]).await;
  }

  async fn write_u104_le_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..13]).await;
  }

  async fn write_i112_be_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[2..16]).await;
  }

  async fn write_i112_le_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..14]).await;
  }

  async fn write_u112_be_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[2..16]).await;
  }

  async fn write_u112_le_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..14]).await;
  }

  async fn write_i120_be_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..16]).await;
  }

  async fn write_i120_le_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..15]).await;
  }

  async fn write_u120_be_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..16]).await;
  }

  async fn write_u120_le_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..15]).await;
  }

  async fn write_i128_be_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..16]).await;
  }

  async fn write_i128_le_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..16]).await;
  }

  async fn write_u128_be_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..16]).await;
  }

  async fn write_u128_le_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..16]).await;
  }
}
#[async_trait::async_trait]
pub trait Off64AsyncWriteInt: Off64AsyncWrite {
  async fn write_i8_at(&self, offset: u64, value: i8) {
    self.write_at(offset, &[value as u8]).await;
  }

  async fn write_u8_at(&self, offset: u64, value: u8) {
    self.write_at(offset, &[value as u8]).await;
  }

  async fn write_i16_be_at(&self, offset: u64, value: i16) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..2]).await;
  }

  async fn write_i16_le_at(&self, offset: u64, value: i16) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..2]).await;
  }

  async fn write_u16_be_at(&self, offset: u64, value: u16) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..2]).await;
  }

  async fn write_u16_le_at(&self, offset: u64, value: u16) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..2]).await;
  }

  async fn write_i24_be_at(&self, offset: u64, value: i32) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..4]).await;
  }

  async fn write_i24_le_at(&self, offset: u64, value: i32) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..3]).await;
  }

  async fn write_u24_be_at(&self, offset: u64, value: u32) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..4]).await;
  }

  async fn write_u24_le_at(&self, offset: u64, value: u32) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..3]).await;
  }

  async fn write_i32_be_at(&self, offset: u64, value: i32) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..4]).await;
  }

  async fn write_i32_le_at(&self, offset: u64, value: i32) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..4]).await;
  }

  async fn write_u32_be_at(&self, offset: u64, value: u32) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..4]).await;
  }

  async fn write_u32_le_at(&self, offset: u64, value: u32) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..4]).await;
  }

  async fn write_i40_be_at(&self, offset: u64, value: i64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[3..8]).await;
  }

  async fn write_i40_le_at(&self, offset: u64, value: i64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..5]).await;
  }

  async fn write_u40_be_at(&self, offset: u64, value: u64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[3..8]).await;
  }

  async fn write_u40_le_at(&self, offset: u64, value: u64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..5]).await;
  }

  async fn write_i48_be_at(&self, offset: u64, value: i64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[2..8]).await;
  }

  async fn write_i48_le_at(&self, offset: u64, value: i64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..6]).await;
  }

  async fn write_u48_be_at(&self, offset: u64, value: u64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[2..8]).await;
  }

  async fn write_u48_le_at(&self, offset: u64, value: u64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..6]).await;
  }

  async fn write_i56_be_at(&self, offset: u64, value: i64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..8]).await;
  }

  async fn write_i56_le_at(&self, offset: u64, value: i64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..7]).await;
  }

  async fn write_u56_be_at(&self, offset: u64, value: u64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..8]).await;
  }

  async fn write_u56_le_at(&self, offset: u64, value: u64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..7]).await;
  }

  async fn write_i64_be_at(&self, offset: u64, value: i64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..8]).await;
  }

  async fn write_i64_le_at(&self, offset: u64, value: i64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..8]).await;
  }

  async fn write_u64_be_at(&self, offset: u64, value: u64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..8]).await;
  }

  async fn write_u64_le_at(&self, offset: u64, value: u64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..8]).await;
  }

  async fn write_i72_be_at(&self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[7..16]).await;
  }

  async fn write_i72_le_at(&self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..9]).await;
  }

  async fn write_u72_be_at(&self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[7..16]).await;
  }

  async fn write_u72_le_at(&self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..9]).await;
  }

  async fn write_i80_be_at(&self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[6..16]).await;
  }

  async fn write_i80_le_at(&self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..10]).await;
  }

  async fn write_u80_be_at(&self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[6..16]).await;
  }

  async fn write_u80_le_at(&self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..10]).await;
  }

  async fn write_i88_be_at(&self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[5..16]).await;
  }

  async fn write_i88_le_at(&self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..11]).await;
  }

  async fn write_u88_be_at(&self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[5..16]).await;
  }

  async fn write_u88_le_at(&self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..11]).await;
  }

  async fn write_i96_be_at(&self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[4..16]).await;
  }

  async fn write_i96_le_at(&self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..12]).await;
  }

  async fn write_u96_be_at(&self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[4..16]).await;
  }

  async fn write_u96_le_at(&self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..12]).await;
  }

  async fn write_i104_be_at(&self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[3..16]).await;
  }

  async fn write_i104_le_at(&self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..13]).await;
  }

  async fn write_u104_be_at(&self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[3..16]).await;
  }

  async fn write_u104_le_at(&self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..13]).await;
  }

  async fn write_i112_be_at(&self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[2..16]).await;
  }

  async fn write_i112_le_at(&self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..14]).await;
  }

  async fn write_u112_be_at(&self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[2..16]).await;
  }

  async fn write_u112_le_at(&self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..14]).await;
  }

  async fn write_i120_be_at(&self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..16]).await;
  }

  async fn write_i120_le_at(&self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..15]).await;
  }

  async fn write_u120_be_at(&self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..16]).await;
  }

  async fn write_u120_le_at(&self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..15]).await;
  }

  async fn write_i128_be_at(&self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..16]).await;
  }

  async fn write_i128_le_at(&self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..16]).await;
  }

  async fn write_u128_be_at(&self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..16]).await;
  }

  async fn write_u128_le_at(&self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..16]).await;
  }
}
pub trait Off64WriteMutInt: Off64WriteMut {
  fn write_i8_at(&mut self, offset: u64, value: i8) {
    self.write_at(offset, &[value as u8]);
  }

  fn write_u8_at(&mut self, offset: u64, value: u8) {
    self.write_at(offset, &[value as u8]);
  }

  fn write_i16_be_at(&mut self, offset: u64, value: i16) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..2]);
  }

  fn write_i16_le_at(&mut self, offset: u64, value: i16) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..2]);
  }

  fn write_u16_be_at(&mut self, offset: u64, value: u16) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..2]);
  }

  fn write_u16_le_at(&mut self, offset: u64, value: u16) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..2]);
  }

  fn write_i24_be_at(&mut self, offset: u64, value: i32) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..4]);
  }

  fn write_i24_le_at(&mut self, offset: u64, value: i32) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..3]);
  }

  fn write_u24_be_at(&mut self, offset: u64, value: u32) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..4]);
  }

  fn write_u24_le_at(&mut self, offset: u64, value: u32) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..3]);
  }

  fn write_i32_be_at(&mut self, offset: u64, value: i32) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..4]);
  }

  fn write_i32_le_at(&mut self, offset: u64, value: i32) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..4]);
  }

  fn write_u32_be_at(&mut self, offset: u64, value: u32) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..4]);
  }

  fn write_u32_le_at(&mut self, offset: u64, value: u32) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..4]);
  }

  fn write_i40_be_at(&mut self, offset: u64, value: i64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[3..8]);
  }

  fn write_i40_le_at(&mut self, offset: u64, value: i64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..5]);
  }

  fn write_u40_be_at(&mut self, offset: u64, value: u64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[3..8]);
  }

  fn write_u40_le_at(&mut self, offset: u64, value: u64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..5]);
  }

  fn write_i48_be_at(&mut self, offset: u64, value: i64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[2..8]);
  }

  fn write_i48_le_at(&mut self, offset: u64, value: i64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..6]);
  }

  fn write_u48_be_at(&mut self, offset: u64, value: u64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[2..8]);
  }

  fn write_u48_le_at(&mut self, offset: u64, value: u64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..6]);
  }

  fn write_i56_be_at(&mut self, offset: u64, value: i64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..8]);
  }

  fn write_i56_le_at(&mut self, offset: u64, value: i64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..7]);
  }

  fn write_u56_be_at(&mut self, offset: u64, value: u64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..8]);
  }

  fn write_u56_le_at(&mut self, offset: u64, value: u64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..7]);
  }

  fn write_i64_be_at(&mut self, offset: u64, value: i64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..8]);
  }

  fn write_i64_le_at(&mut self, offset: u64, value: i64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..8]);
  }

  fn write_u64_be_at(&mut self, offset: u64, value: u64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..8]);
  }

  fn write_u64_le_at(&mut self, offset: u64, value: u64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..8]);
  }

  fn write_i72_be_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[7..16]);
  }

  fn write_i72_le_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..9]);
  }

  fn write_u72_be_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[7..16]);
  }

  fn write_u72_le_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..9]);
  }

  fn write_i80_be_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[6..16]);
  }

  fn write_i80_le_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..10]);
  }

  fn write_u80_be_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[6..16]);
  }

  fn write_u80_le_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..10]);
  }

  fn write_i88_be_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[5..16]);
  }

  fn write_i88_le_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..11]);
  }

  fn write_u88_be_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[5..16]);
  }

  fn write_u88_le_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..11]);
  }

  fn write_i96_be_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[4..16]);
  }

  fn write_i96_le_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..12]);
  }

  fn write_u96_be_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[4..16]);
  }

  fn write_u96_le_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..12]);
  }

  fn write_i104_be_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[3..16]);
  }

  fn write_i104_le_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..13]);
  }

  fn write_u104_be_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[3..16]);
  }

  fn write_u104_le_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..13]);
  }

  fn write_i112_be_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[2..16]);
  }

  fn write_i112_le_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..14]);
  }

  fn write_u112_be_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[2..16]);
  }

  fn write_u112_le_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..14]);
  }

  fn write_i120_be_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..16]);
  }

  fn write_i120_le_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..15]);
  }

  fn write_u120_be_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..16]);
  }

  fn write_u120_le_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..15]);
  }

  fn write_i128_be_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..16]);
  }

  fn write_i128_le_at(&mut self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..16]);
  }

  fn write_u128_be_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..16]);
  }

  fn write_u128_le_at(&mut self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..16]);
  }
}
pub trait Off64WriteInt: Off64Write {
  fn write_i8_at(&self, offset: u64, value: i8) {
    self.write_at(offset, &[value as u8]);
  }

  fn write_u8_at(&self, offset: u64, value: u8) {
    self.write_at(offset, &[value as u8]);
  }

  fn write_i16_be_at(&self, offset: u64, value: i16) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..2]);
  }

  fn write_i16_le_at(&self, offset: u64, value: i16) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..2]);
  }

  fn write_u16_be_at(&self, offset: u64, value: u16) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..2]);
  }

  fn write_u16_le_at(&self, offset: u64, value: u16) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..2]);
  }

  fn write_i24_be_at(&self, offset: u64, value: i32) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..4]);
  }

  fn write_i24_le_at(&self, offset: u64, value: i32) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..3]);
  }

  fn write_u24_be_at(&self, offset: u64, value: u32) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..4]);
  }

  fn write_u24_le_at(&self, offset: u64, value: u32) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..3]);
  }

  fn write_i32_be_at(&self, offset: u64, value: i32) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..4]);
  }

  fn write_i32_le_at(&self, offset: u64, value: i32) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..4]);
  }

  fn write_u32_be_at(&self, offset: u64, value: u32) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..4]);
  }

  fn write_u32_le_at(&self, offset: u64, value: u32) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..4]);
  }

  fn write_i40_be_at(&self, offset: u64, value: i64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[3..8]);
  }

  fn write_i40_le_at(&self, offset: u64, value: i64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..5]);
  }

  fn write_u40_be_at(&self, offset: u64, value: u64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[3..8]);
  }

  fn write_u40_le_at(&self, offset: u64, value: u64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..5]);
  }

  fn write_i48_be_at(&self, offset: u64, value: i64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[2..8]);
  }

  fn write_i48_le_at(&self, offset: u64, value: i64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..6]);
  }

  fn write_u48_be_at(&self, offset: u64, value: u64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[2..8]);
  }

  fn write_u48_le_at(&self, offset: u64, value: u64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..6]);
  }

  fn write_i56_be_at(&self, offset: u64, value: i64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..8]);
  }

  fn write_i56_le_at(&self, offset: u64, value: i64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..7]);
  }

  fn write_u56_be_at(&self, offset: u64, value: u64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..8]);
  }

  fn write_u56_le_at(&self, offset: u64, value: u64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..7]);
  }

  fn write_i64_be_at(&self, offset: u64, value: i64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..8]);
  }

  fn write_i64_le_at(&self, offset: u64, value: i64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..8]);
  }

  fn write_u64_be_at(&self, offset: u64, value: u64) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..8]);
  }

  fn write_u64_le_at(&self, offset: u64, value: u64) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..8]);
  }

  fn write_i72_be_at(&self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[7..16]);
  }

  fn write_i72_le_at(&self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..9]);
  }

  fn write_u72_be_at(&self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[7..16]);
  }

  fn write_u72_le_at(&self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..9]);
  }

  fn write_i80_be_at(&self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[6..16]);
  }

  fn write_i80_le_at(&self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..10]);
  }

  fn write_u80_be_at(&self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[6..16]);
  }

  fn write_u80_le_at(&self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..10]);
  }

  fn write_i88_be_at(&self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[5..16]);
  }

  fn write_i88_le_at(&self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..11]);
  }

  fn write_u88_be_at(&self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[5..16]);
  }

  fn write_u88_le_at(&self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..11]);
  }

  fn write_i96_be_at(&self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[4..16]);
  }

  fn write_i96_le_at(&self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..12]);
  }

  fn write_u96_be_at(&self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[4..16]);
  }

  fn write_u96_le_at(&self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..12]);
  }

  fn write_i104_be_at(&self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[3..16]);
  }

  fn write_i104_le_at(&self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..13]);
  }

  fn write_u104_be_at(&self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[3..16]);
  }

  fn write_u104_le_at(&self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..13]);
  }

  fn write_i112_be_at(&self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[2..16]);
  }

  fn write_i112_le_at(&self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..14]);
  }

  fn write_u112_be_at(&self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[2..16]);
  }

  fn write_u112_le_at(&self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..14]);
  }

  fn write_i120_be_at(&self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..16]);
  }

  fn write_i120_le_at(&self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..15]);
  }

  fn write_u120_be_at(&self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[1..16]);
  }

  fn write_u120_le_at(&self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..15]);
  }

  fn write_i128_be_at(&self, offset: u64, value: i128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..16]);
  }

  fn write_i128_le_at(&self, offset: u64, value: i128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..16]);
  }

  fn write_u128_be_at(&self, offset: u64, value: u128) {
    let buf = value.to_be_bytes();
    self.write_at(offset, &buf[0..16]);
  }

  fn write_u128_le_at(&self, offset: u64, value: u128) {
    let buf = value.to_le_bytes();
    self.write_at(offset, &buf[0..16]);
  }
}
pub fn create_i8(value: i8) -> [u8; 1] {
  [value as u8]
}

pub fn create_u8(value: u8) -> [u8; 1] {
  [value as u8]
}

pub fn create_i16_be(value: i16) -> [u8; 2] {
  let buf = value.to_be_bytes();
  buf[0..2].try_into().unwrap()
}

pub fn create_i16_le(value: i16) -> [u8; 2] {
  let buf = value.to_le_bytes();
  buf[0..2].try_into().unwrap()
}

pub fn create_u16_be(value: u16) -> [u8; 2] {
  let buf = value.to_be_bytes();
  buf[0..2].try_into().unwrap()
}

pub fn create_u16_le(value: u16) -> [u8; 2] {
  let buf = value.to_le_bytes();
  buf[0..2].try_into().unwrap()
}

pub fn create_i24_be(value: i32) -> [u8; 3] {
  let buf = value.to_be_bytes();
  buf[1..4].try_into().unwrap()
}

pub fn create_i24_le(value: i32) -> [u8; 3] {
  let buf = value.to_le_bytes();
  buf[0..3].try_into().unwrap()
}

pub fn create_u24_be(value: u32) -> [u8; 3] {
  let buf = value.to_be_bytes();
  buf[1..4].try_into().unwrap()
}

pub fn create_u24_le(value: u32) -> [u8; 3] {
  let buf = value.to_le_bytes();
  buf[0..3].try_into().unwrap()
}

pub fn create_i32_be(value: i32) -> [u8; 4] {
  let buf = value.to_be_bytes();
  buf[0..4].try_into().unwrap()
}

pub fn create_i32_le(value: i32) -> [u8; 4] {
  let buf = value.to_le_bytes();
  buf[0..4].try_into().unwrap()
}

pub fn create_u32_be(value: u32) -> [u8; 4] {
  let buf = value.to_be_bytes();
  buf[0..4].try_into().unwrap()
}

pub fn create_u32_le(value: u32) -> [u8; 4] {
  let buf = value.to_le_bytes();
  buf[0..4].try_into().unwrap()
}

pub fn create_i40_be(value: i64) -> [u8; 5] {
  let buf = value.to_be_bytes();
  buf[3..8].try_into().unwrap()
}

pub fn create_i40_le(value: i64) -> [u8; 5] {
  let buf = value.to_le_bytes();
  buf[0..5].try_into().unwrap()
}

pub fn create_u40_be(value: u64) -> [u8; 5] {
  let buf = value.to_be_bytes();
  buf[3..8].try_into().unwrap()
}

pub fn create_u40_le(value: u64) -> [u8; 5] {
  let buf = value.to_le_bytes();
  buf[0..5].try_into().unwrap()
}

pub fn create_i48_be(value: i64) -> [u8; 6] {
  let buf = value.to_be_bytes();
  buf[2..8].try_into().unwrap()
}

pub fn create_i48_le(value: i64) -> [u8; 6] {
  let buf = value.to_le_bytes();
  buf[0..6].try_into().unwrap()
}

pub fn create_u48_be(value: u64) -> [u8; 6] {
  let buf = value.to_be_bytes();
  buf[2..8].try_into().unwrap()
}

pub fn create_u48_le(value: u64) -> [u8; 6] {
  let buf = value.to_le_bytes();
  buf[0..6].try_into().unwrap()
}

pub fn create_i56_be(value: i64) -> [u8; 7] {
  let buf = value.to_be_bytes();
  buf[1..8].try_into().unwrap()
}

pub fn create_i56_le(value: i64) -> [u8; 7] {
  let buf = value.to_le_bytes();
  buf[0..7].try_into().unwrap()
}

pub fn create_u56_be(value: u64) -> [u8; 7] {
  let buf = value.to_be_bytes();
  buf[1..8].try_into().unwrap()
}

pub fn create_u56_le(value: u64) -> [u8; 7] {
  let buf = value.to_le_bytes();
  buf[0..7].try_into().unwrap()
}

pub fn create_i64_be(value: i64) -> [u8; 8] {
  let buf = value.to_be_bytes();
  buf[0..8].try_into().unwrap()
}

pub fn create_i64_le(value: i64) -> [u8; 8] {
  let buf = value.to_le_bytes();
  buf[0..8].try_into().unwrap()
}

pub fn create_u64_be(value: u64) -> [u8; 8] {
  let buf = value.to_be_bytes();
  buf[0..8].try_into().unwrap()
}

pub fn create_u64_le(value: u64) -> [u8; 8] {
  let buf = value.to_le_bytes();
  buf[0..8].try_into().unwrap()
}

pub fn create_i72_be(value: i128) -> [u8; 9] {
  let buf = value.to_be_bytes();
  buf[7..16].try_into().unwrap()
}

pub fn create_i72_le(value: i128) -> [u8; 9] {
  let buf = value.to_le_bytes();
  buf[0..9].try_into().unwrap()
}

pub fn create_u72_be(value: u128) -> [u8; 9] {
  let buf = value.to_be_bytes();
  buf[7..16].try_into().unwrap()
}

pub fn create_u72_le(value: u128) -> [u8; 9] {
  let buf = value.to_le_bytes();
  buf[0..9].try_into().unwrap()
}

pub fn create_i80_be(value: i128) -> [u8; 10] {
  let buf = value.to_be_bytes();
  buf[6..16].try_into().unwrap()
}

pub fn create_i80_le(value: i128) -> [u8; 10] {
  let buf = value.to_le_bytes();
  buf[0..10].try_into().unwrap()
}

pub fn create_u80_be(value: u128) -> [u8; 10] {
  let buf = value.to_be_bytes();
  buf[6..16].try_into().unwrap()
}

pub fn create_u80_le(value: u128) -> [u8; 10] {
  let buf = value.to_le_bytes();
  buf[0..10].try_into().unwrap()
}

pub fn create_i88_be(value: i128) -> [u8; 11] {
  let buf = value.to_be_bytes();
  buf[5..16].try_into().unwrap()
}

pub fn create_i88_le(value: i128) -> [u8; 11] {
  let buf = value.to_le_bytes();
  buf[0..11].try_into().unwrap()
}

pub fn create_u88_be(value: u128) -> [u8; 11] {
  let buf = value.to_be_bytes();
  buf[5..16].try_into().unwrap()
}

pub fn create_u88_le(value: u128) -> [u8; 11] {
  let buf = value.to_le_bytes();
  buf[0..11].try_into().unwrap()
}

pub fn create_i96_be(value: i128) -> [u8; 12] {
  let buf = value.to_be_bytes();
  buf[4..16].try_into().unwrap()
}

pub fn create_i96_le(value: i128) -> [u8; 12] {
  let buf = value.to_le_bytes();
  buf[0..12].try_into().unwrap()
}

pub fn create_u96_be(value: u128) -> [u8; 12] {
  let buf = value.to_be_bytes();
  buf[4..16].try_into().unwrap()
}

pub fn create_u96_le(value: u128) -> [u8; 12] {
  let buf = value.to_le_bytes();
  buf[0..12].try_into().unwrap()
}

pub fn create_i104_be(value: i128) -> [u8; 13] {
  let buf = value.to_be_bytes();
  buf[3..16].try_into().unwrap()
}

pub fn create_i104_le(value: i128) -> [u8; 13] {
  let buf = value.to_le_bytes();
  buf[0..13].try_into().unwrap()
}

pub fn create_u104_be(value: u128) -> [u8; 13] {
  let buf = value.to_be_bytes();
  buf[3..16].try_into().unwrap()
}

pub fn create_u104_le(value: u128) -> [u8; 13] {
  let buf = value.to_le_bytes();
  buf[0..13].try_into().unwrap()
}

pub fn create_i112_be(value: i128) -> [u8; 14] {
  let buf = value.to_be_bytes();
  buf[2..16].try_into().unwrap()
}

pub fn create_i112_le(value: i128) -> [u8; 14] {
  let buf = value.to_le_bytes();
  buf[0..14].try_into().unwrap()
}

pub fn create_u112_be(value: u128) -> [u8; 14] {
  let buf = value.to_be_bytes();
  buf[2..16].try_into().unwrap()
}

pub fn create_u112_le(value: u128) -> [u8; 14] {
  let buf = value.to_le_bytes();
  buf[0..14].try_into().unwrap()
}

pub fn create_i120_be(value: i128) -> [u8; 15] {
  let buf = value.to_be_bytes();
  buf[1..16].try_into().unwrap()
}

pub fn create_i120_le(value: i128) -> [u8; 15] {
  let buf = value.to_le_bytes();
  buf[0..15].try_into().unwrap()
}

pub fn create_u120_be(value: u128) -> [u8; 15] {
  let buf = value.to_be_bytes();
  buf[1..16].try_into().unwrap()
}

pub fn create_u120_le(value: u128) -> [u8; 15] {
  let buf = value.to_le_bytes();
  buf[0..15].try_into().unwrap()
}

pub fn create_i128_be(value: i128) -> [u8; 16] {
  let buf = value.to_be_bytes();
  buf[0..16].try_into().unwrap()
}

pub fn create_i128_le(value: i128) -> [u8; 16] {
  let buf = value.to_le_bytes();
  buf[0..16].try_into().unwrap()
}

pub fn create_u128_be(value: u128) -> [u8; 16] {
  let buf = value.to_be_bytes();
  buf[0..16].try_into().unwrap()
}

pub fn create_u128_le(value: u128) -> [u8; 16] {
  let buf = value.to_le_bytes();
  buf[0..16].try_into().unwrap()
}
