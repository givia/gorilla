//! # Gorilla
//!
//! A Rust implementation of Facebook's Gorilla time-series compression algorithm,
//! as described in *"Gorilla: A Fast, Scalable, In-Memory Time Series Database"*
//! (VLDB 2015).
//!
//! ## Algorithm overview
//!
//! Gorilla achieves high compression ratios for time-series data by exploiting
//! two key observations:
//!
//! - **Timestamps** in real-world time series tend to arrive at regular intervals.
//!   The *delta-of-delta* encoding captures deviations from the expected interval
//!   using variable-length bit prefixes, often requiring only 1 bit per timestamp.
//!
//! - **Values** (IEEE 754 doubles) in adjacent readings are frequently close or
//!   identical. XOR-based encoding stores only the changed bits, with a
//!   leading/trailing zero window that is reused across consecutive values.
//!
//! ## Example
//!
//! ```rust
//! use gorilla::{Encoder, Decoder, DataPoint};
//!
//! // Compress
//! let mut encoder = Encoder::new();
//! encoder.encode(DataPoint::new(1609459200, 12.0));
//! encoder.encode(DataPoint::new(1609459260, 12.5));
//! encoder.encode(DataPoint::new(1609459320, 13.0));
//! encoder.finish();
//!
//! let compressed = encoder.into_compressed();
//! println!("Compressed {} points into {} bytes", compressed.count, compressed.bytes.len());
//!
//! // Decompress
//! let points = Decoder::decode(&compressed).unwrap();
//! assert_eq!(points.len(), 3);
//! assert_eq!(points[0], DataPoint::new(1609459200, 12.0));
//! ```
//!
//! ## Lazy iteration
//!
//! For large blocks, use `Decoder::iter()` to avoid allocating the full output:
//!
//! ```rust
//! # use gorilla::{Encoder, Decoder, DataPoint};
//! # let mut encoder = Encoder::new();
//! # encoder.encode(DataPoint::new(1609459200, 12.0));
//! # encoder.encode(DataPoint::new(1609459260, 12.5));
//! # encoder.finish();
//! # let block = encoder.into_compressed();
//! for result in Decoder::iter(&block) {
//!     let dp = result.unwrap();
//!     println!("{}: {}", dp.timestamp, dp.value);
//! }
//! ```

pub mod bitbuffer;
pub mod decoder;
pub mod encoder;

// Re-export primary types at the crate root.
pub use decoder::{DecodeError, Decoder, DecoderIter};
pub use encoder::{CompressedBlock, DataPoint, Encoder};
