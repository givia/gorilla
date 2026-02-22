use crate::bitbuffer::BitReader;
use crate::encoder::{CompressedBlock, DataPoint};

/// Error type for decoding failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// The compressed stream ended unexpectedly.
    UnexpectedEnd,
    /// The stream contains no data points.
    Empty,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::UnexpectedEnd => write!(f, "unexpected end of compressed stream"),
            DecodeError::Empty => write!(f, "compressed stream is empty"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// The Gorilla decompressor (decoder).
///
/// Reconstructs time-series data points from a Gorilla-compressed bit stream.
///
/// # Example
/// ```
/// use gorilla::{Encoder, Decoder, DataPoint};
///
/// let mut encoder = Encoder::new();
/// encoder.encode(DataPoint::new(1609459200, 12.0)).unwrap();
/// encoder.encode(DataPoint::new(1609459260, 12.5)).unwrap();
/// encoder.finish().unwrap();
///
/// let compressed = encoder.into_compressed();
/// let points = Decoder::decode(&compressed).unwrap();
/// assert_eq!(points.len(), 2);
/// assert_eq!(points[0].value, 12.0);
/// ```
pub struct Decoder;

impl Decoder {
    /// Decodes all data points from a `CompressedBlock`.
    pub fn decode(block: &CompressedBlock) -> Result<Vec<DataPoint>, DecodeError> {
        let mut reader = BitReader::from_raw(&block.bytes, block.total_bits);
        Self::decode_from_reader(&mut reader)
    }

    /// Decodes all data points from raw bytes + total bit count.
    pub fn decode_raw(bytes: &[u8], total_bits: usize) -> Result<Vec<DataPoint>, DecodeError> {
        let mut reader = BitReader::from_raw(bytes, total_bits);
        Self::decode_from_reader(&mut reader)
    }

    /// Returns an iterator that lazily decodes data points from a `CompressedBlock`.
    pub fn iter(block: &CompressedBlock) -> DecoderIter<'_> {
        let reader = BitReader::from_raw(&block.bytes, block.total_bits);
        DecoderIter {
            reader,
            state: IterState::Initial,
            prev_timestamp: 0,
            prev_delta: 0,
            prev_value_bits: 0,
            prev_leading_zeros: 0,
            prev_trailing_zeros: 0,
            done: false,
        }
    }

    fn decode_from_reader(reader: &mut BitReader<'_>) -> Result<Vec<DataPoint>, DecodeError> {
        let mut points = Vec::new();
        let mut prev_timestamp: u64;
        let mut prev_delta: i64;
        let mut prev_value_bits: u64;
        let mut prev_leading_zeros: u8 = 0;
        let mut prev_trailing_zeros: u8 = 0;

        // ── First data point ────────────────────────────────────────
        let ts = reader.read_bits(64).ok_or(DecodeError::Empty)?;
        let val_bits = reader.read_bits(64).ok_or(DecodeError::UnexpectedEnd)?;
        prev_timestamp = ts;
        prev_value_bits = val_bits;
        prev_delta = 0;
        points.push(DataPoint::new(ts, f64::from_bits(val_bits)));

        // ── Subsequent data points ──────────────────────────────────
        loop {
            // Decode delta-of-delta.
            let dod = match Self::decode_delta_of_delta(reader)? {
                DodResult::Value(v) => v,
                DodResult::EndOfStream => break,
            };

            if points.len() == 1 {
                // Second point: dod IS the delta.
                prev_delta = dod;
            } else {
                prev_delta += dod;
            }
            prev_timestamp = (prev_timestamp as i64 + prev_delta) as u64;

            // Decode value.
            let (val_bits, leading, trailing) =
                Self::decode_value(reader, prev_value_bits, prev_leading_zeros, prev_trailing_zeros)?;
            prev_value_bits = val_bits;
            prev_leading_zeros = leading;
            prev_trailing_zeros = trailing;

            points.push(DataPoint::new(prev_timestamp, f64::from_bits(val_bits)));
        }

        Ok(points)
    }

    /// Decodes a variable-length delta-of-delta value.
    fn decode_delta_of_delta(reader: &mut BitReader<'_>) -> Result<DodResult, DecodeError> {
        let bit = reader.read_bit().ok_or(DecodeError::UnexpectedEnd)?;
        if !bit {
            // '0' => dod == 0
            return Ok(DodResult::Value(0));
        }

        let bit = reader.read_bit().ok_or(DecodeError::UnexpectedEnd)?;
        if !bit {
            // '10' => 7-bit value
            let raw = reader.read_bits(7).ok_or(DecodeError::UnexpectedEnd)?;
            let dod = sign_extend(raw, 7);
            return Ok(DodResult::Value(dod));
        }

        let bit = reader.read_bit().ok_or(DecodeError::UnexpectedEnd)?;
        if !bit {
            // '110' => 9-bit value
            let raw = reader.read_bits(9).ok_or(DecodeError::UnexpectedEnd)?;
            let dod = sign_extend(raw, 9);
            return Ok(DodResult::Value(dod));
        }

        let bit = reader.read_bit().ok_or(DecodeError::UnexpectedEnd)?;
        if !bit {
            // '1110' => 12-bit value
            let raw = reader.read_bits(12).ok_or(DecodeError::UnexpectedEnd)?;
            let dod = sign_extend(raw, 12);
            return Ok(DodResult::Value(dod));
        }

        // '1111' => 64-bit value (or end-of-stream sentinel)
        let raw = reader.read_bits(64).ok_or(DecodeError::UnexpectedEnd)?;
        if raw == 0xFFFF_FFFF_FFFF_FFFF {
            return Ok(DodResult::EndOfStream);
        }
        let dod = raw as i64;
        Ok(DodResult::Value(dod))
    }

    /// Decodes an XOR-compressed value.
    fn decode_value(
        reader: &mut BitReader<'_>,
        prev_value_bits: u64,
        prev_leading_zeros: u8,
        prev_trailing_zeros: u8,
    ) -> Result<(u64, u8, u8), DecodeError> {
        let bit = reader.read_bit().ok_or(DecodeError::UnexpectedEnd)?;
        if !bit {
            // XOR is zero — same value.
            return Ok((prev_value_bits, prev_leading_zeros, prev_trailing_zeros));
        }

        let control = reader.read_bit().ok_or(DecodeError::UnexpectedEnd)?;
        if !control {
            // '10' — reuse previous leading/trailing zero window.
            let meaningful_bits = 64 - prev_leading_zeros - prev_trailing_zeros;
            let meaningful = reader
                .read_bits(meaningful_bits)
                .ok_or(DecodeError::UnexpectedEnd)?;
            let xor = meaningful << prev_trailing_zeros;
            let value_bits = prev_value_bits ^ xor;
            Ok((value_bits, prev_leading_zeros, prev_trailing_zeros))
        } else {
            // '11' — new window.
            let leading = reader.read_bits(6).ok_or(DecodeError::UnexpectedEnd)? as u8;
            let meaningful_bits = reader.read_bits(6).ok_or(DecodeError::UnexpectedEnd)? as u8 + 1;
            let trailing = 64 - leading - meaningful_bits;
            let meaningful = reader
                .read_bits(meaningful_bits)
                .ok_or(DecodeError::UnexpectedEnd)?;
            let xor = meaningful << trailing;
            let value_bits = prev_value_bits ^ xor;
            Ok((value_bits, leading, trailing))
        }
    }
}

/// Sign-extend an `n`-bit value stored in a `u64` to a full `i64`.
#[inline]
fn sign_extend(value: u64, bits: u8) -> i64 {
    let shift = 64 - bits;
    ((value << shift) as i64) >> shift
}

enum DodResult {
    Value(i64),
    EndOfStream,
}

// ── Lazy iterator ──────────────────────────────────────────────────────

#[derive(Debug)]
enum IterState {
    Initial,
    SecondPoint,
    Subsequent,
}

/// A lazy iterator that yields `DataPoint`s from a compressed block.
pub struct DecoderIter<'a> {
    reader: BitReader<'a>,
    state: IterState,
    prev_timestamp: u64,
    prev_delta: i64,
    prev_value_bits: u64,
    prev_leading_zeros: u8,
    prev_trailing_zeros: u8,
    done: bool,
}

impl<'a> Iterator for DecoderIter<'a> {
    type Item = Result<DataPoint, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        match self.state {
            IterState::Initial => {
                // Read first data point.
                let ts = match self.reader.read_bits(64) {
                    Some(v) => v,
                    None => {
                        self.done = true;
                        return None; // empty stream
                    }
                };
                let val_bits = match self.reader.read_bits(64) {
                    Some(v) => v,
                    None => {
                        self.done = true;
                        return Some(Err(DecodeError::UnexpectedEnd));
                    }
                };
                self.prev_timestamp = ts;
                self.prev_value_bits = val_bits;
                self.state = IterState::SecondPoint;
                Some(Ok(DataPoint::new(ts, f64::from_bits(val_bits))))
            }
            IterState::SecondPoint | IterState::Subsequent => {
                let dod = match Decoder::decode_delta_of_delta(&mut self.reader) {
                    Ok(DodResult::Value(v)) => v,
                    Ok(DodResult::EndOfStream) => {
                        self.done = true;
                        return None;
                    }
                    Err(e) => {
                        self.done = true;
                        return Some(Err(e));
                    }
                };

                match self.state {
                    IterState::SecondPoint => {
                        self.prev_delta = dod;
                        self.state = IterState::Subsequent;
                    }
                    _ => {
                        self.prev_delta += dod;
                    }
                }
                self.prev_timestamp = (self.prev_timestamp as i64 + self.prev_delta) as u64;

                match Decoder::decode_value(
                    &mut self.reader,
                    self.prev_value_bits,
                    self.prev_leading_zeros,
                    self.prev_trailing_zeros,
                ) {
                    Ok((val_bits, leading, trailing)) => {
                        self.prev_value_bits = val_bits;
                        self.prev_leading_zeros = leading;
                        self.prev_trailing_zeros = trailing;
                        Some(Ok(DataPoint::new(
                            self.prev_timestamp,
                            f64::from_bits(val_bits),
                        )))
                    }
                    Err(e) => {
                        self.done = true;
                        Some(Err(e))
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoder::Encoder;

    #[test]
    fn test_roundtrip_basic() {
        let input = vec![
            DataPoint::new(1609459200, 12.0),
            DataPoint::new(1609459260, 12.5),
            DataPoint::new(1609459320, 13.0),
            DataPoint::new(1609459380, 11.5),
            DataPoint::new(1609459440, 12.0),
        ];

        let mut enc = Encoder::new();
        for dp in &input {
            enc.encode(*dp).unwrap();
        }
        enc.finish().unwrap();
        let block = enc.into_compressed();

        let output = Decoder::decode(&block).unwrap();
        assert_eq!(input, output);
    }

    #[test]
    fn test_roundtrip_identical_values() {
        let input: Vec<DataPoint> = (0..100)
            .map(|i| DataPoint::new(1000 + i * 60, 42.0))
            .collect();

        let mut enc = Encoder::new();
        for dp in &input {
            enc.encode(*dp).unwrap();
        }
        enc.finish().unwrap();
        let block = enc.into_compressed();

        let output = Decoder::decode(&block).unwrap();
        assert_eq!(input, output);
    }

    #[test]
    fn test_roundtrip_single() {
        let input = vec![DataPoint::new(12345, 99.99)];
        let mut enc = Encoder::new();
        enc.encode(input[0]).unwrap();
        enc.finish().unwrap();
        let block = enc.into_compressed();
        let output = Decoder::decode(&block).unwrap();
        assert_eq!(input, output);
    }

    #[test]
    fn test_iterator() {
        let input = vec![
            DataPoint::new(100, 1.0),
            DataPoint::new(160, 2.0),
            DataPoint::new(220, 3.0),
        ];

        let mut enc = Encoder::new();
        for dp in &input {
            enc.encode(*dp).unwrap();
        }
        enc.finish().unwrap();
        let block = enc.into_compressed();

        let output: Vec<DataPoint> = Decoder::iter(&block)
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(input, output);
    }
}
