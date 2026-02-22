use crate::bitbuffer::{BitBuffer, BufferFull};

/// A single time-series data point: a Unix timestamp (seconds) and an f64 value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DataPoint {
    pub timestamp: u64,
    pub value: f64,
}

impl DataPoint {
    /// Creates a new `DataPoint`.
    pub fn new(timestamp: u64, value: f64) -> Self {
        Self { timestamp, value }
    }
}

/// The Gorilla compressor (encoder).
///
/// Implements the compression scheme from Facebook's Gorilla paper:
/// - **Timestamps**: delta-of-delta encoding with variable-length prefixes.
/// - **Values**: XOR-based compression with leading/trailing zero tracking.
///
/// # Example
/// ```
/// use gorilla::{Encoder, DataPoint};
///
/// let mut encoder = Encoder::new();
/// encoder.encode(DataPoint::new(1609459200, 12.0)).unwrap();
/// encoder.encode(DataPoint::new(1609459260, 12.5)).unwrap();
/// encoder.encode(DataPoint::new(1609459320, 13.0)).unwrap();
/// encoder.finish().unwrap();
///
/// let compressed = encoder.into_compressed();
/// ```
pub struct Encoder {
    buf: BitBuffer,
    /// Number of data points encoded so far.
    count: u64,
    /// Previous timestamp.
    prev_timestamp: u64,
    /// Previous delta between timestamps.
    prev_delta: i64,
    /// Previous value as raw bits.
    prev_value_bits: u64,
    /// Number of leading zeros in the previous XOR result.
    prev_leading_zeros: u8,
    /// Number of trailing zeros in the previous XOR result.
    prev_trailing_zeros: u8,
    /// Whether `finish()` has been called.
    finished: bool,
}

impl Encoder {
    /// Creates a new `Encoder` with a default buffer.
    pub fn new() -> Self {
        Self {
            buf: BitBuffer::with_capacity(128),
            count: 0,
            prev_timestamp: 0,
            prev_delta: 0,
            prev_value_bits: 0,
            prev_leading_zeros: 64,
            prev_trailing_zeros: 64,
            finished: false,
        }
    }

    /// Creates a new `Encoder` whose internal buffer will not grow beyond
    /// `max_bytes` bytes. Once the limit is reached, `encode()` will return
    /// `Err(BufferFull)`.
    pub fn with_limit(max_bytes: usize) -> Self {
        Self {
            buf: BitBuffer::with_limit(max_bytes),
            count: 0,
            prev_timestamp: 0,
            prev_delta: 0,
            prev_value_bits: 0,
            prev_leading_zeros: 64,
            prev_trailing_zeros: 64,
            finished: false,
        }
    }

    /// Encodes a data point into the compressed stream.
    ///
    /// Data points should be appended in strictly increasing timestamp order.
    ///
    /// Returns `Err(BufferFull)` if the buffer's byte limit would be exceeded.
    /// On error the encoder may be in a partially-written state; use
    /// `into_compressed()` to recover the data encoded so far.
    pub fn encode(&mut self, dp: DataPoint) -> Result<(), BufferFull> {
        assert!(!self.finished, "cannot encode after finish()");

        if self.count == 0 {
            self.encode_first(dp)?;
        } else if self.count == 1 {
            self.encode_second(dp)?;
        } else {
            self.encode_subsequent(dp)?;
        }

        self.count += 1;
        Ok(())
    }

    /// Writes the end-of-stream marker. Must be called after all data points
    /// have been encoded.
    ///
    /// Returns `Err(BufferFull)` if the buffer cannot fit the marker.
    pub fn finish(&mut self) -> Result<(), BufferFull> {
        if self.finished {
            return Ok(());
        }
        self.buf.write_bits(0b1111, 4)?;
        self.buf.write_bits(0xFFFF_FFFF_FFFF_FFFF, 64)?;
        self.finished = true;
        Ok(())
    }

    /// Returns a reference to the underlying `BitBuffer`.
    pub fn buffer(&self) -> &BitBuffer {
        &self.buf
    }

    /// Consumes the encoder and returns the compressed `BitBuffer`.
    pub fn into_buffer(self) -> BitBuffer {
        self.buf
    }

    /// Returns the compressed data as `(bytes, total_bits)`.
    pub fn into_compressed(self) -> CompressedBlock {
        CompressedBlock {
            total_bits: self.buf.len_bits(),
            bytes: self.buf.into_bytes(),
            count: self.count,
        }
    }

    /// Returns the number of data points encoded so far.
    pub fn count(&self) -> u64 {
        self.count
    }

    // ── internal helpers ───────────────────────────────────────────────

    fn encode_first(&mut self, dp: DataPoint) -> Result<(), BufferFull> {
        self.buf.write_bits(dp.timestamp, 64)?;
        let bits = dp.value.to_bits();
        self.buf.write_bits(bits, 64)?;

        self.prev_timestamp = dp.timestamp;
        self.prev_value_bits = bits;
        Ok(())
    }

    fn encode_second(&mut self, dp: DataPoint) -> Result<(), BufferFull> {
        let delta = dp.timestamp as i64 - self.prev_timestamp as i64;
        self.encode_delta_of_delta(delta)?;

        self.encode_value(dp.value)?;

        self.prev_delta = delta;
        self.prev_timestamp = dp.timestamp;
        Ok(())
    }

    fn encode_subsequent(&mut self, dp: DataPoint) -> Result<(), BufferFull> {
        let delta = dp.timestamp as i64 - self.prev_timestamp as i64;
        let dod = delta - self.prev_delta;
        self.encode_delta_of_delta(dod)?;

        self.encode_value(dp.value)?;

        self.prev_delta = delta;
        self.prev_timestamp = dp.timestamp;
        Ok(())
    }

    /// Encodes a delta-of-delta value using the Gorilla variable-length scheme:
    ///
    /// | dod == 0       | `0`                            | 1 bit   |
    /// | [-63, 64]      | `10` + 7-bit value             | 9 bits  |
    /// | [-255, 256]    | `110` + 9-bit value            | 12 bits |
    /// | [-2047, 2048]  | `1110` + 12-bit value          | 16 bits |
    /// | otherwise      | `1111` + 64-bit value          | 68 bits |
    fn encode_delta_of_delta(&mut self, dod: i64) -> Result<(), BufferFull> {
        if dod == 0 {
            self.buf.write_bit(false)?;
        } else if dod >= -63 && dod <= 64 {
            self.buf.write_bits(0b10, 2)?;
            self.buf.write_bits((dod as u64) & 0x7F, 7)?;
        } else if dod >= -255 && dod <= 256 {
            self.buf.write_bits(0b110, 3)?;
            self.buf.write_bits((dod as u64) & 0x1FF, 9)?;
        } else if dod >= -2047 && dod <= 2048 {
            self.buf.write_bits(0b1110, 4)?;
            self.buf.write_bits((dod as u64) & 0xFFF, 12)?;
        } else {
            self.buf.write_bits(0b1111, 4)?;
            self.buf.write_bits(dod as u64, 64)?;
        }
        Ok(())
    }

    /// XOR-based value compression:
    ///
    /// 1. XOR with previous value.
    /// 2. If XOR == 0: write single `0` bit.
    /// 3. Else:
    ///    a. Write `1`.  
    ///    b. If leading/trailing zeros fit within previous window:
    ///       write `0` + meaningful bits.  
    ///    c. Else: write `1` + 6-bit leading zeros + 6-bit meaningful length + meaningful bits.
    fn encode_value(&mut self, value: f64) -> Result<(), BufferFull> {
        let bits = value.to_bits();
        let xor = bits ^ self.prev_value_bits;

        if xor == 0 {
            self.buf.write_bit(false)?;
        } else {
            self.buf.write_bit(true)?; // '1' — value changed

            let leading = xor.leading_zeros() as u8;
            let trailing = xor.trailing_zeros() as u8;

            if leading >= self.prev_leading_zeros && trailing >= self.prev_trailing_zeros {
                // The meaningful bits fit within the previous window.
                self.buf.write_bit(false)?; // '0' — reuse window
                let meaningful_bits = 64 - self.prev_leading_zeros - self.prev_trailing_zeros;
                let meaningful_value = (xor >> self.prev_trailing_zeros) & bitmask(meaningful_bits);
                self.buf.write_bits(meaningful_value, meaningful_bits)?;
            } else {
                // New window.
                self.buf.write_bit(true)?; // '1' — new window
                let meaningful_bits = 64 - leading - trailing;
                self.buf.write_bits(leading as u64, 6)?;
                self.buf.write_bits((meaningful_bits - 1) as u64, 6)?;
                let meaningful_value = (xor >> trailing) & bitmask(meaningful_bits);
                self.buf.write_bits(meaningful_value, meaningful_bits)?;

                self.prev_leading_zeros = leading;
                self.prev_trailing_zeros = trailing;
            }
        }

        self.prev_value_bits = bits;
        Ok(())
    }
}

impl Default for Encoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns a bitmask with the lowest `n` bits set. Handles `n == 64` without overflow.
#[inline]
fn bitmask(n: u8) -> u64 {
    if n >= 64 {
        u64::MAX
    } else {
        (1u64 << n) - 1
    }
}

/// A compressed block of Gorilla-encoded time-series data.
#[derive(Debug, Clone)]
pub struct CompressedBlock {
    /// The compressed byte data.
    pub bytes: Vec<u8>,
    /// Total number of valid bits in `bytes`.
    pub total_bits: usize,
    /// Number of data points in this block.
    pub count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_single_point() {
        let mut enc = Encoder::new();
        enc.encode(DataPoint::new(1609459200, 42.0)).unwrap();
        enc.finish().unwrap();
        assert_eq!(enc.count(), 1);
        assert!(enc.buffer().len_bits() > 0);
    }

    #[test]
    fn test_encode_identical_values() {
        let mut enc = Encoder::new();
        for i in 0..10 {
            enc.encode(DataPoint::new(1609459200 + i * 60, 42.0)).unwrap();
        }
        enc.finish().unwrap();
        assert_eq!(enc.count(), 10);
        // Identical values should compress very efficiently.
    }

    #[test]
    fn test_encode_varying_deltas() {
        let mut enc = Encoder::new();
        enc.encode(DataPoint::new(100, 1.0)).unwrap();
        enc.encode(DataPoint::new(160, 2.0)).unwrap(); // delta=60
        enc.encode(DataPoint::new(220, 3.0)).unwrap(); // delta=60, dod=0
        enc.encode(DataPoint::new(290, 4.0)).unwrap(); // delta=70, dod=10
        enc.encode(DataPoint::new(500, 5.0)).unwrap(); // delta=210, dod=140
        enc.finish().unwrap();
        assert_eq!(enc.count(), 5);
    }

    #[test]
    fn test_encode_with_limit_ok() {
        // 256 bytes is plenty for a few points with constant values.
        let mut enc = Encoder::with_limit(256);
        enc.encode(DataPoint::new(1609459200, 42.0)).unwrap();
        enc.encode(DataPoint::new(1609459260, 42.0)).unwrap();
        enc.encode(DataPoint::new(1609459320, 42.0)).unwrap();
        enc.finish().unwrap();
        assert_eq!(enc.count(), 3);
    }

    #[test]
    fn test_encode_with_limit_exceeded() {
        // 1 byte can't even fit the first 64-bit timestamp.
        let mut enc = Encoder::with_limit(1);
        let result = enc.encode(DataPoint::new(1609459200, 42.0));
        assert!(result.is_err());
    }
}
