/// A growable bit buffer that supports writing and reading individual bits
/// and multi-bit values. Used as the underlying storage for Gorilla compression.
#[derive(Debug, Clone)]
pub struct BitBuffer {
    bytes: Vec<u8>,
    /// Number of valid bits in the last byte (1..=8, or 0 if empty).
    bit_count: u8,
}

impl BitBuffer {
    /// Creates a new empty `BitBuffer`.
    pub fn new() -> Self {
        Self {
            bytes: Vec::new(),
            bit_count: 0,
        }
    }

    /// Creates a `BitBuffer` with the given pre-allocated capacity in bytes.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
            bit_count: 0,
        }
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
    #[inline]
    pub fn write_bit(&mut self, bit: bool) {
        if self.bit_count == 0 || self.bit_count == 8 {
            self.bytes.push(0);
            self.bit_count = 0;
        }
        if bit {
            let last = self.bytes.last_mut().unwrap();
            *last |= 1 << (7 - self.bit_count);
        }
        self.bit_count += 1;
    }

    /// Writes the lowest `n` bits of `value` (big-endian order). `n` must be <= 64.
    pub fn write_bits(&mut self, value: u64, n: u8) {
        debug_assert!(n <= 64);
        if n == 0 {
            return;
        }
        for i in (0..n).rev() {
            self.write_bit((value >> i) & 1 == 1);
        }
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
        buf.write_bit(true);
        buf.write_bit(false);
        buf.write_bit(true);
        buf.write_bit(true);
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
        buf.write_bits(0b11010, 5);
        buf.write_bits(0xFF, 8);
        buf.write_bits(0x00, 8);
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
        buf.write_bits(val, 64);
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
}
