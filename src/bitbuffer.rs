/// Error returned when a write would exceed the buffer's byte limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BufferFull;

impl std::fmt::Display for BufferFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "write would exceed bit buffer byte limit")
    }
}

impl std::error::Error for BufferFull {}

/// A growable bit buffer that supports writing and reading individual bits
/// and multi-bit values. Used as the underlying storage for Gorilla compression.
///
/// An optional byte limit can be set to cap memory usage. When the limit is
/// reached, write operations return `Err(BufferFull)` instead of growing.
#[derive(Debug, Clone)]
pub struct BitBuffer {
    bytes: Vec<u8>,
    /// Number of valid bits in the last byte (1..=8, or 0 if empty).
    bit_count: u8,
    /// Maximum number of bytes the buffer is allowed to hold (`None` = unlimited).
    max_bytes: Option<usize>,
}

impl BitBuffer {
    /// Creates a new empty `BitBuffer` with no size limit.
    pub fn new() -> Self {
        Self {
            bytes: Vec::new(),
            bit_count: 0,
            max_bytes: None,
        }
    }

    /// Creates a `BitBuffer` with the given pre-allocated capacity in bytes.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
            bit_count: 0,
            max_bytes: None,
        }
    }

    /// Creates a `BitBuffer` with a maximum byte limit.
    ///
    /// Once the buffer contains `max_bytes` bytes, further writes that would
    /// require a new byte return `Err(BufferFull)`.
    pub fn with_limit(max_bytes: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(max_bytes.min(128)),
            bit_count: 0,
            max_bytes: Some(max_bytes),
        }
    }

    /// Sets (or clears) the maximum byte limit on an existing buffer.
    pub fn set_limit(&mut self, max_bytes: Option<usize>) {
        self.max_bytes = max_bytes;
    }

    /// Returns the current byte limit, if any.
    pub fn limit(&self) -> Option<usize> {
        self.max_bytes
    }

    /// Creates a `BitBuffer` from raw bytes and total bit length.
    pub fn from_raw(bytes: Vec<u8>, total_bits: usize) -> Self {
        let full_bytes = total_bits / 8;
        let remaining = (total_bits % 8) as u8;
        debug_assert!(
            full_bytes < bytes.len() || (full_bytes == bytes.len() && remaining == 0),
            "total_bits exceeds bytes length"
        );
        Self {
            bytes,
            bit_count: if remaining == 0 { 8 } else { remaining },
            max_bytes: None,
        }
    }

    /// Returns the total number of bits written.
    #[inline]
    pub fn len_bits(&self) -> usize {
        if self.bytes.is_empty() {
            0
        } else {
            (self.bytes.len() - 1) * 8 + self.bit_count as usize
        }
    }

    /// Returns `true` if no bits have been written.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Returns a reference to the underlying byte slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Consumes the buffer and returns the raw byte vector.
    #[inline]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Writes a single bit (the lowest bit of `bit`).
    ///
    /// Returns `Err(BufferFull)` if adding a new byte would exceed the limit.
    #[inline]
    pub fn write_bit(&mut self, bit: bool) -> Result<(), BufferFull> {
        if self.bit_count == 0 || self.bit_count == 8 {
            if let Some(max) = self.max_bytes {
                if self.bytes.len() >= max {
                    return Err(BufferFull);
                }
            }
            self.bytes.push(0);
            self.bit_count = 0;
        }
        if bit {
            let last = self.bytes.last_mut().unwrap();
            *last |= 1 << (7 - self.bit_count);
        }
        self.bit_count += 1;
        Ok(())
    }

    /// Writes the lowest `n` bits of `value` (big-endian order). `n` must be <= 64.
    ///
    /// Returns `Err(BufferFull)` if writing would exceed the limit. On error the
    /// buffer may contain a partial write (some bits of this call may have been
    /// written). Callers that need atomicity should check `remaining_capacity`
    /// before writing.
    pub fn write_bits(&mut self, value: u64, n: u8) -> Result<(), BufferFull> {
        debug_assert!(n <= 64);
        if n == 0 {
            return Ok(());
        }
        for i in (0..n).rev() {
            self.write_bit((value >> i) & 1 == 1)?;
        }
        Ok(())
    }

    /// Returns the number of bytes that can still be added before hitting the
    /// limit, or `None` if no limit is set.
    pub fn remaining_capacity(&self) -> Option<usize> {
        self.max_bytes.map(|max| max.saturating_sub(self.bytes.len()))
    }
}

impl Default for BitBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// A cursor for reading bits sequentially from a `BitBuffer`.
#[derive(Debug)]
pub struct BitReader<'a> {
    bytes: &'a [u8],
    /// Total number of valid bits.
    total_bits: usize,
    /// Current bit position (0-indexed from the start).
    pos: usize,
}

impl<'a> BitReader<'a> {
    /// Creates a new `BitReader` over the given buffer.
    pub fn new(buffer: &'a BitBuffer) -> Self {
        Self {
            bytes: buffer.as_bytes(),
            total_bits: buffer.len_bits(),
            pos: 0,
        }
    }

    /// Creates a `BitReader` from raw bytes and a total bit count.
    pub fn from_raw(bytes: &'a [u8], total_bits: usize) -> Self {
        Self {
            bytes,
            total_bits,
            pos: 0,
        }
    }

    /// Returns the number of bits remaining.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.total_bits.saturating_sub(self.pos)
    }

    /// Returns `true` if there are no more bits to read.
    #[inline]
    pub fn is_exhausted(&self) -> bool {
        self.pos >= self.total_bits
    }

    /// Reads a single bit. Returns `None` if exhausted.
    #[inline]
    pub fn read_bit(&mut self) -> Option<bool> {
        if self.pos >= self.total_bits {
            return None;
        }
        let byte_idx = self.pos / 8;
        let bit_idx = self.pos % 8;
        self.pos += 1;
        Some((self.bytes[byte_idx] >> (7 - bit_idx)) & 1 == 1)
    }

    /// Reads `n` bits as a `u64` (big-endian). Returns `None` if not enough bits remain.
    pub fn read_bits(&mut self, n: u8) -> Option<u64> {
        if n == 0 {
            return Some(0);
        }
        if self.remaining() < n as usize {
            return None;
        }
        let mut value: u64 = 0;
        for _ in 0..n {
            value = (value << 1) | (self.read_bit()? as u64);
        }
        Some(value)
    }

    /// Peeks at the next bit without advancing the position.
    #[inline]
    pub fn peek_bit(&self) -> Option<bool> {
        if self.pos >= self.total_bits {
            return None;
        }
        let byte_idx = self.pos / 8;
        let bit_idx = self.pos % 8;
        Some((self.bytes[byte_idx] >> (7 - bit_idx)) & 1 == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_and_read_bits() {
        let mut buf = BitBuffer::new();
        buf.write_bit(true).unwrap();
        buf.write_bit(false).unwrap();
        buf.write_bit(true).unwrap();
        buf.write_bit(true).unwrap();
        assert_eq!(buf.len_bits(), 4);

        let mut reader = BitReader::new(&buf);
        assert_eq!(reader.read_bit(), Some(true));
        assert_eq!(reader.read_bit(), Some(false));
        assert_eq!(reader.read_bit(), Some(true));
        assert_eq!(reader.read_bit(), Some(true));
        assert_eq!(reader.read_bit(), None);
    }

    #[test]
    fn test_write_and_read_multi_bits() {
        let mut buf = BitBuffer::new();
        buf.write_bits(0b11010, 5).unwrap();
        buf.write_bits(0xFF, 8).unwrap();
        buf.write_bits(0x00, 8).unwrap();
        assert_eq!(buf.len_bits(), 21);

        let mut reader = BitReader::new(&buf);
        assert_eq!(reader.read_bits(5), Some(0b11010));
        assert_eq!(reader.read_bits(8), Some(0xFF));
        assert_eq!(reader.read_bits(8), Some(0x00));
        assert!(reader.is_exhausted());
    }

    #[test]
    fn test_64_bit_value() {
        let mut buf = BitBuffer::new();
        let val: u64 = 0xDEAD_BEEF_CAFE_BABE;
        buf.write_bits(val, 64).unwrap();
        assert_eq!(buf.len_bits(), 64);

        let mut reader = BitReader::new(&buf);
        assert_eq!(reader.read_bits(64), Some(val));
    }

    #[test]
    fn test_empty_buffer() {
        let buf = BitBuffer::new();
        assert!(buf.is_empty());
        assert_eq!(buf.len_bits(), 0);

        let mut reader = BitReader::new(&buf);
        assert!(reader.is_exhausted());
        assert_eq!(reader.read_bit(), None);
    }

    #[test]
    fn test_with_limit_allows_within_budget() {
        let mut buf = BitBuffer::with_limit(2);
        // 2 bytes = 16 bits should be fine
        buf.write_bits(0xABCD, 16).unwrap();
        assert_eq!(buf.len_bits(), 16);
        assert_eq!(buf.remaining_capacity(), Some(0));
    }

    #[test]
    fn test_with_limit_rejects_overflow() {
        let mut buf = BitBuffer::with_limit(1);
        // First 8 bits fit in 1 byte.
        buf.write_bits(0xFF, 8).unwrap();
        // The 9th bit requires a second byte — should fail.
        assert_eq!(buf.write_bit(true), Err(BufferFull));
        // Buffer should still contain exactly 8 bits.
        assert_eq!(buf.len_bits(), 8);
    }

    #[test]
    fn test_with_limit_partial_byte_ok() {
        let mut buf = BitBuffer::with_limit(1);
        // Writing 5 bits only needs 1 byte.
        buf.write_bits(0b10101, 5).unwrap();
        assert_eq!(buf.len_bits(), 5);
        // 3 more bits still fit in the same byte.
        buf.write_bits(0b010, 3).unwrap();
        assert_eq!(buf.len_bits(), 8);
        // 9th bit would need a 2nd byte — rejected.
        assert!(buf.write_bit(false).is_err());
    }

    #[test]
    fn test_no_limit_is_unlimited() {
        let mut buf = BitBuffer::new();
        assert_eq!(buf.limit(), None);
        assert_eq!(buf.remaining_capacity(), None);
        // Should never fail.
        buf.write_bits(0xDEADBEEF, 32).unwrap();
        buf.write_bits(0xDEADBEEF, 32).unwrap();
    }

    #[test]
    fn test_set_limit() {
        let mut buf = BitBuffer::new();
        assert_eq!(buf.limit(), None);
        buf.set_limit(Some(4));
        assert_eq!(buf.limit(), Some(4));
        buf.write_bits(0xDEADBEEF, 32).unwrap();
        assert!(buf.write_bit(true).is_err());
        // Remove limit
        buf.set_limit(None);
        buf.write_bit(true).unwrap();
    }
}
