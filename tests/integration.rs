use gorilla::{DataPoint, Decoder, Encoder};

/// Round-trip: encode then decode, verify exact equality.
fn roundtrip(input: &[DataPoint]) -> Vec<DataPoint> {
    let mut enc = Encoder::new();
    for dp in input {
        enc.encode(*dp).unwrap();
    }
    enc.finish().unwrap();
    let block = enc.into_compressed();
    Decoder::decode(&block).expect("decode failed")
}

#[test]
fn test_empty_stream() {
    let mut enc = Encoder::new();
    enc.finish().unwrap();
    let block = enc.into_compressed();
    // The stream has no actual data points (only the end marker).
    // Decoding should return an error (empty) since there's no header.
    let result = Decoder::decode(&block);
    assert!(result.is_err() || result.unwrap().is_empty());
}

#[test]
fn test_single_point_roundtrip() {
    let input = vec![DataPoint::new(1609459200, 3.14159)];
    assert_eq!(roundtrip(&input), input);
}

#[test]
fn test_two_points_roundtrip() {
    let input = vec![
        DataPoint::new(1609459200, 100.0),
        DataPoint::new(1609459260, 101.5),
    ];
    assert_eq!(roundtrip(&input), input);
}

#[test]
fn test_constant_values() {
    let input: Vec<DataPoint> = (0..1000)
        .map(|i| DataPoint::new(1_000_000 + i * 60, 42.0))
        .collect();
    let output = roundtrip(&input);
    assert_eq!(output, input);
}

#[test]
fn test_constant_interval_varying_values() {
    let input: Vec<DataPoint> = (0..500)
        .map(|i| {
            let t = 1_000_000 + i * 60;
            let v = (i as f64) * 0.1 + (i as f64).sin();
            DataPoint::new(t, v)
        })
        .collect();
    assert_eq!(roundtrip(&input), input);
}

#[test]
fn test_varying_intervals() {
    let input = vec![
        DataPoint::new(100, 1.0),
        DataPoint::new(160, 2.0),
        DataPoint::new(225, 3.0),
        DataPoint::new(400, 4.0),
        DataPoint::new(401, 5.0),
        DataPoint::new(10_000, 6.0),
    ];
    assert_eq!(roundtrip(&input), input);
}

#[test]
fn test_negative_values() {
    let input = vec![
        DataPoint::new(1000, -100.5),
        DataPoint::new(1060, -99.3),
        DataPoint::new(1120, 0.0),
        DataPoint::new(1180, 99.3),
        DataPoint::new(1240, -0.0),
    ];
    let output = roundtrip(&input);
    // Note: -0.0 and 0.0 have different bit representations.
    assert_eq!(output.len(), input.len());
    for (a, b) in input.iter().zip(output.iter()) {
        assert_eq!(a.timestamp, b.timestamp);
        assert_eq!(a.value.to_bits(), b.value.to_bits());
    }
}

#[test]
fn test_special_float_values() {
    let input = vec![
        DataPoint::new(1000, f64::MIN),
        DataPoint::new(1060, f64::MAX),
        DataPoint::new(1120, f64::EPSILON),
        DataPoint::new(1180, f64::MIN_POSITIVE),
        DataPoint::new(1240, f64::INFINITY),
        DataPoint::new(1300, f64::NEG_INFINITY),
        DataPoint::new(1360, 0.0),
    ];
    assert_eq!(roundtrip(&input), input);
}

#[test]
fn test_nan_roundtrip() {
    let input = vec![
        DataPoint::new(1000, 1.0),
        DataPoint::new(1060, f64::NAN),
        DataPoint::new(1120, 2.0),
    ];
    let output = roundtrip(&input);
    assert_eq!(output.len(), 3);
    assert_eq!(output[0].value, 1.0);
    assert!(output[1].value.is_nan());
    assert_eq!(output[2].value, 2.0);
}

#[test]
fn test_large_dataset_roundtrip() {
    let input: Vec<DataPoint> = (0..10_000)
        .map(|i| {
            let t = 1_609_459_200 + i * 15; // 15-second intervals
            let v = 20.0 + 5.0 * ((i as f64) * 0.01).sin() + (i as f64) * 0.001;
            DataPoint::new(t, v)
        })
        .collect();
    let output = roundtrip(&input);
    assert_eq!(output.len(), input.len());
    for (a, b) in input.iter().zip(output.iter()) {
        assert_eq!(a.timestamp, b.timestamp);
        assert_eq!(a.value.to_bits(), b.value.to_bits());
    }
}

#[test]
fn test_compression_ratio_identical_values() {
    // Best case: identical timestamps intervals + identical values.
    // 10,000 points × 16 bytes each = 160 KB uncompressed.
    let input: Vec<DataPoint> = (0..10_000)
        .map(|i| DataPoint::new(1_000_000 + i * 60, 42.0))
        .collect();

    let mut enc = Encoder::new();
    for dp in &input {
        enc.encode(*dp).unwrap();
    }
    enc.finish().unwrap();
    let block = enc.into_compressed();

    let uncompressed_bytes = input.len() * 16;
    let compressed_bytes = block.bytes.len();
    let ratio = uncompressed_bytes as f64 / compressed_bytes as f64;

    // Identical values + constant interval: ~2 bits per point → very high ratio.
    assert!(
        ratio > 40.0,
        "compression ratio too low for identical data: {:.2}x ({} -> {} bytes)",
        ratio,
        uncompressed_bytes,
        compressed_bytes
    );
}

#[test]
fn test_compression_ratio_varying_values() {
    // Realistic case: constant interval + slowly varying values.
    let input: Vec<DataPoint> = (0..10_000)
        .map(|i| DataPoint::new(1_000_000 + i * 60, 42.0 + (i % 10) as f64 * 0.1))
        .collect();

    let mut enc = Encoder::new();
    for dp in &input {
        enc.encode(*dp).unwrap();
    }
    enc.finish().unwrap();
    let block = enc.into_compressed();

    let uncompressed_bytes = input.len() * 16;
    let compressed_bytes = block.bytes.len();
    let ratio = uncompressed_bytes as f64 / compressed_bytes as f64;

    // Varying float values still compress decently (XOR shares leading zeros).
    assert!(
        ratio > 2.0,
        "compression ratio too low: {:.2}x ({} -> {} bytes)",
        ratio,
        uncompressed_bytes,
        compressed_bytes
    );
}

#[test]
fn test_iterator_matches_decode() {
    let input: Vec<DataPoint> = (0..200)
        .map(|i| DataPoint::new(1000 + i * 60, (i as f64).sqrt()))
        .collect();

    let mut enc = Encoder::new();
    for dp in &input {
        enc.encode(*dp).unwrap();
    }
    enc.finish().unwrap();
    let block = enc.into_compressed();

    let decoded = Decoder::decode(&block).unwrap();
    let iterated: Vec<DataPoint> = Decoder::iter(&block).map(|r| r.unwrap()).collect();

    assert_eq!(decoded, iterated);
    assert_eq!(decoded, input);
}

#[test]
fn test_large_timestamp_gaps() {
    let input = vec![
        DataPoint::new(0, 1.0),
        DataPoint::new(1_000_000_000, 2.0),
        DataPoint::new(2_000_000_000, 3.0),
        DataPoint::new(2_000_000_001, 4.0),
    ];
    assert_eq!(roundtrip(&input), input);
}
