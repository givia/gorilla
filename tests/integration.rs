use gorilla::{BufferFull, DataPoint, Decoder, Encoder};

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

// ── Buffer limit tests ─────────────────────────────────────────────────

#[test]
fn test_limit_too_small_for_first_point() {
    // First data point needs 128 bits (16 bytes): 64 for timestamp + 64 for value.
    // A limit of 15 bytes should fail on the very first encode.
    let mut enc = Encoder::with_limit(15);
    let result = enc.encode(DataPoint::new(1_000_000, 42.0));
    assert!(result.is_err());
    assert_eq!(enc.count(), 0);
}

#[test]
fn test_limit_exact_fit_first_point() {
    // 16 bytes is exactly enough for the first data point's 128 bits.
    let mut enc = Encoder::with_limit(16);
    enc.encode(DataPoint::new(1_000_000, 42.0)).unwrap();
    assert_eq!(enc.count(), 1);
    // finish() needs 68 more bits (4-prefix + 64-sentinel) = 9 bytes → should fail.
    assert!(enc.finish().is_err());
}

#[test]
fn test_limit_allows_single_point_roundtrip() {
    // First point = 16 bytes, finish marker = 68 bits ≤ 9 bytes → 25 bytes total.
    let mut enc = Encoder::with_limit(25);
    enc.encode(DataPoint::new(1_000_000, 42.0)).unwrap();
    enc.finish().unwrap();
    let block = enc.into_compressed();
    let output = Decoder::decode(&block).unwrap();
    assert_eq!(output, vec![DataPoint::new(1_000_000, 42.0)]);
}

#[test]
fn test_limit_encodes_until_full() {
    // Use a generous-but-bounded buffer and encode until we hit the limit.
    let mut enc = Encoder::with_limit(32);
    let mut count = 0u64;
    for i in 0..1000 {
        match enc.encode(DataPoint::new(1_000_000 + i * 60, 42.0)) {
            Ok(()) => count += 1,
            Err(_) => break,
        }
    }
    // We should have encoded at least the first point (16 bytes) and
    // stopped before exhausting 1000 points.
    assert!(count >= 1, "should encode at least one point");
    assert!(count < 1000, "should hit the limit before 1000 points");
    assert_eq!(enc.count(), count);
}

#[test]
fn test_limit_partial_data_still_decodable() {
    // Encode constant-value points in a limited buffer, binary-searching for the
    // maximum count that still leaves room for the finish marker.
    let limit = 128;

    // First, find how many points we can pack.
    let mut enc = Encoder::with_limit(limit);
    let mut max_encoded = 0u64;
    for i in 0..10_000 {
        if enc.encode(DataPoint::new(1_000_000 + i * 60, 42.0)).is_err() {
            break;
        }
        max_encoded += 1;
    }
    assert!(max_encoded > 1, "should encode multiple points");

    // Now find the largest N ≤ max_encoded where encode(N) + finish() both succeed.
    let mut good_n = 0usize;
    for n in (1..=max_encoded as usize).rev() {
        let mut trial = Encoder::with_limit(limit);
        let mut ok = true;
        for i in 0..n {
            if trial
                .encode(DataPoint::new(1_000_000 + (i as u64) * 60, 42.0))
                .is_err()
            {
                ok = false;
                break;
            }
        }
        if ok && trial.finish().is_ok() {
            good_n = n;
            break;
        }
    }
    assert!(good_n >= 1, "should find at least one finishable count");

    // Re-encode at good_n and verify round-trip.
    let mut enc = Encoder::with_limit(limit);
    let mut input = Vec::new();
    for i in 0..good_n {
        let dp = DataPoint::new(1_000_000 + (i as u64) * 60, 42.0);
        enc.encode(dp).unwrap();
        input.push(dp);
    }
    enc.finish().unwrap();
    let block = enc.into_compressed();
    let output = Decoder::decode(&block).unwrap();
    assert_eq!(output, input);
    assert!(block.bytes.len() <= limit);
}

#[test]
fn test_limit_error_is_buffer_full() {
    let mut enc = Encoder::with_limit(1);
    let err = enc.encode(DataPoint::new(100, 1.0)).unwrap_err();
    // Verify it's the expected BufferFull type.
    let _: BufferFull = err;
}

#[test]
fn test_limit_constant_values_high_count() {
    // Constant-value streams compress very well (~2 bits per point after the first).
    // With 256 bytes we should fit many points.
    let limit = 256;
    let mut enc = Encoder::with_limit(limit);
    let mut count = 0u64;
    for i in 0..100_000 {
        match enc.encode(DataPoint::new(1_000_000 + i * 60, 42.0)) {
            Ok(()) => count += 1,
            Err(_) => break,
        }
    }
    // First point = 16 bytes, remaining ≈ 240 bytes × 8 = 1920 bits.
    // Each subsequent constant point ≈ 2 bits → ~960 points.
    assert!(
        count > 500,
        "constant-value stream should fit many points in 256 bytes, got {count}"
    );
}

#[test]
fn test_limit_varying_values_lower_count() {
    // Varying values use more bits per point than constant values.
    let limit = 256;
    let mut enc_const = Encoder::with_limit(limit);
    let mut count_const = 0u64;
    for i in 0..100_000 {
        match enc_const.encode(DataPoint::new(1_000_000 + i * 60, 42.0)) {
            Ok(()) => count_const += 1,
            Err(_) => break,
        }
    }

    let mut enc_vary = Encoder::with_limit(limit);
    let mut count_vary = 0u64;
    for i in 0..100_000 {
        let v = (i as f64) * 1.23456 + (i as f64).sin();
        match enc_vary.encode(DataPoint::new(1_000_000 + i * 60, v)) {
            Ok(()) => count_vary += 1,
            Err(_) => break,
        }
    }

    // Varying values should fit fewer points in the same space.
    assert!(
        count_vary < count_const,
        "varying values ({count_vary}) should fit fewer points than constant ({count_const})"
    );
}

#[test]
fn test_limit_zero_bytes() {
    // A zero-byte limit should reject everything.
    let mut enc = Encoder::with_limit(0);
    assert!(enc.encode(DataPoint::new(100, 1.0)).is_err());
    assert_eq!(enc.count(), 0);
}

#[test]
fn test_limit_finish_without_encode() {
    // finish() on an empty encoder with a very small limit should fail
    // because the end-of-stream marker needs 68 bits (≥ 9 bytes).
    let mut enc = Encoder::with_limit(8);
    assert!(enc.finish().is_err());
}

#[test]
fn test_limit_finish_without_encode_sufficient_space() {
    // 9 bytes = 72 bits, enough for the 68-bit end-of-stream marker.
    let mut enc = Encoder::with_limit(9);
    enc.finish().unwrap();
}

#[test]
fn test_limit_roundtrip_multiple_points() {
    // Pick a limit that comfortably fits several points + the marker.
    let limit = 64;
    let mut enc = Encoder::with_limit(limit);
    let points = vec![
        DataPoint::new(1_000_000, 10.0),
        DataPoint::new(1_000_060, 10.5),
        DataPoint::new(1_000_120, 11.0),
        DataPoint::new(1_000_180, 10.5),
    ];
    for dp in &points {
        enc.encode(*dp).unwrap();
    }
    enc.finish().unwrap();
    let block = enc.into_compressed();
    let output = Decoder::decode(&block).unwrap();
    assert_eq!(output, points);
    assert!(block.bytes.len() <= limit);
}

#[test]
fn test_limit_compressed_size_respects_limit() {
    // Verify the compressed output never exceeds the byte limit.
    for limit in [16, 32, 64, 128, 256, 512] {
        let mut enc = Encoder::with_limit(limit);
        for i in 0..10_000 {
            if enc
                .encode(DataPoint::new(1_000_000 + i * 60, (i as f64).sqrt()))
                .is_err()
            {
                break;
            }
        }
        // Don't call finish() — just check the raw buffer size.
        let block = enc.into_compressed();
        assert!(
            block.bytes.len() <= limit,
            "limit={limit}, actual size={}",
            block.bytes.len()
        );
    }
}

#[test]
fn test_limit_encoder_count_tracks_successful_encodes() {
    let mut enc = Encoder::with_limit(20);
    // First point takes exactly 16 bytes; should succeed.
    enc.encode(DataPoint::new(100, 1.0)).unwrap();
    assert_eq!(enc.count(), 1);

    // Second point needs at least 2 more bits; 4 remaining bytes → should succeed.
    enc.encode(DataPoint::new(160, 1.0)).unwrap();
    assert_eq!(enc.count(), 2);

    // Eventually we'll hit the limit.
    let mut last_count = 2;
    for i in 2..1000u64 {
        match enc.encode(DataPoint::new(100 + i * 60, 1.0)) {
            Ok(()) => last_count += 1,
            Err(_) => break,
        }
    }
    assert_eq!(enc.count(), last_count);
}
