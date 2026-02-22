# gorilla

A Rust implementation of Facebook's **Gorilla** time-series compression algorithm, as described in the VLDB 2015 paper *"Gorilla: A Fast, Scalable, In-Memory Time Series Database"*.

## Algorithm

Gorilla compresses streams of `(timestamp, f64 value)` pairs using two techniques:

| Component    | Technique                          | Typical cost per point |
|--------------|------------------------------------|------------------------|
| **Timestamps** | Delta-of-delta with variable-length prefixes | 1 bit (constant interval) |
| **Values**     | XOR encoding with leading/trailing zero windows | 1 bit (identical values) |

### Timestamp encoding (delta-of-delta)

| Range             | Prefix   | Payload   | Total bits |
|-------------------|----------|-----------|------------|
| `dod == 0`        | `0`      | —         | 1          |
| `[-63, 64]`       | `10`     | 7 bits    | 9          |
| `[-255, 256]`     | `110`    | 9 bits    | 12         |
| `[-2047, 2048]`   | `1110`   | 12 bits   | 16         |
| anything else     | `1111`   | 64 bits   | 68         |

### Value encoding (XOR-based)

1. XOR the current value with the previous one.
2. If XOR is zero → write a single `0` bit.
3. Otherwise write `1`, then either:
   - `0` + meaningful bits (reusing the previous leading/trailing zero window), or
   - `1` + 6-bit leading zeros + 6-bit length + meaningful bits (new window).

## Usage

```rust
use gorilla::{Encoder, Decoder, DataPoint};

// Compress
let mut encoder = Encoder::new();
encoder.encode(DataPoint::new(1609459200, 12.0));
encoder.encode(DataPoint::new(1609459260, 12.5));
encoder.encode(DataPoint::new(1609459320, 13.0));
encoder.finish();

let compressed = encoder.into_compressed();
println!(
    "Compressed {} points into {} bytes",
    compressed.count,
    compressed.bytes.len()
);

// Decompress (all at once)
let points = Decoder::decode(&compressed).unwrap();
assert_eq!(points.len(), 3);

// Or lazily iterate
for result in Decoder::iter(&compressed) {
    let dp = result.unwrap();
    println!("{}: {}", dp.timestamp, dp.value);
}
```

## Crate structure

| Module       | Description                              |
|--------------|------------------------------------------|
| `bitbuffer`  | Growable bit buffer and sequential reader |
| `encoder`    | Gorilla compressor                       |
| `decoder`    | Gorilla decompressor + lazy iterator     |

## License

MIT
