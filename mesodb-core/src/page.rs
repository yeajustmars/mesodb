// mesodb-core/src/page.rs

use bytemuck::{Pod, Zeroable};

pub const PAGE_SIZE: usize = 4096;
pub const NUM_CELLS: usize = 127;

/// Fixed-width key representing a target for a "Now" point-lookup.
/// 16 bytes total.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Pod, Zeroable)]
pub struct IndexKey {
    pub e: u64,    // Entity ID
    pub a: u32,    // Attribute ID
    pub _pad: u32, // Explicit padding to maintain 8-byte alignment boundaries
}

/// Fixed-width value representation.
/// 16 bytes total.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct IndexValue {
    pub type_tag: u8,
    pub padding: [u8; 7],
    pub payload: [u8; 8], // 64-bit payload handles Int, Float, Ref, Time. Uuid fits in 16 if needed later via overflow.
}

/// 8 bytes total.
#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct PageHeader {
    pub is_leaf: u8,
    pub padding: [u8; 1],
    pub num_cells: u16,
    pub right_sibling: u32, // Next Page ID for fast range scans
}

/// Represents a single 4KB OS Page holding our B+Tree Node.
/// Exact size calculation:
/// - Header: 8 bytes
/// - Keys: 127 * 16 = 2032 bytes
/// - Values: 127 * 16 = 2032 bytes
/// - Padding: 24 bytes
/// - Total: 4096 bytes
#[repr(C, align(64))]
#[derive(Copy, Clone)]
pub struct NodePage {
    pub header: PageHeader,
    pub keys: [IndexKey; NUM_CELLS],
    pub values: [IndexValue; NUM_CELLS],
    pub _padding: [u8; 24],
}

// Manually implement bytemuck traits for NodePage due to array length limits
unsafe impl bytemuck::Zeroable for NodePage {}
unsafe impl bytemuck::Pod for NodePage {}

#[repr(C, align(64))]
#[derive(Copy, Clone)]
pub struct OverflowPage {
    pub next_page_id: u32,
    pub length: u32,
    pub data: [u8; 4088], // 4 + 4 + 4088 = 4096 bytes
}

// Manually implement bytemuck traits to bypass array length limits in the derive macro
unsafe impl bytemuck::Zeroable for OverflowPage {}
unsafe impl bytemuck::Pod for OverflowPage {}

// --- AVT INDEX (Attribute -> Value -> Entity) ---

pub const AV_NUM_CELLS: usize = 101;

/// Fixed-width key representing a target for a Value-based Range Lookup.
/// 24 bytes total. Because it is a Covering Index, there is no Value array!
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Pod, Zeroable)]
pub struct AvIndexKey {
    pub a: u32,
    pub _pad: u32, // MUST always be 0 to ensure Ord works correctly
    pub v_sort: u64,
    pub e: u64,
}

/// Represents a single 4KB OS Page holding our Avt B+Tree Node.
/// Exact size calculation:
/// - Header: 8 bytes
/// - Keys (101 * 24 bytes): 2424 bytes
/// - Values (101 * 16 bytes): 1616 bytes
/// - Padding: 48 bytes
/// - Total: 4096 bytes
#[repr(C, align(64))]
#[derive(Copy, Clone)]
pub struct AvNodePage {
    pub header: PageHeader,
    pub keys: [AvIndexKey; AV_NUM_CELLS],
    pub values: [IndexValue; AV_NUM_CELLS],
    pub _padding: [u8; 48],
}

// Manually implement bytemuck traits for AvNodePage due to array length limits
unsafe impl bytemuck::Zeroable for AvNodePage {}
unsafe impl bytemuck::Pod for AvNodePage {}

// --- HARDWARE-LEVEL BIT TWIDDLING ---

/// Flips the highest bit so negative i64s sort perfectly below positive i64s as u64s.
#[inline(always)]
pub fn encode_i64(v: i64) -> u64 {
    (v as u64) ^ 0x8000_0000_0000_0000
}

#[inline(always)]
pub fn decode_i64(v: u64) -> i64 {
    (v ^ 0x8000_0000_0000_0000) as i64
}

/// Flips the sign bit for positive floats, and all bits for negative floats.
/// This perfectly aligns IEEE 754 floats to a contiguous u64 sort order.
#[inline(always)]
pub fn encode_f64(v: f64) -> u64 {
    let bits = v.to_bits();
    if (bits & 0x8000_0000_0000_0000) == 0 {
        // Positive: flip the sign bit
        bits ^ 0x8000_0000_0000_0000
    } else {
        // Negative: flip all bits
        bits ^ 0xFFFF_FFFF_FFFF_FFFF
    }
}

#[inline(always)]
pub fn decode_f64(v: u64) -> f64 {
    let bits = if (v & 0x8000_0000_0000_0000) != 0 {
        // Was positive: flip the sign bit back
        v ^ 0x8000_0000_0000_0000
    } else {
        // Was negative: flip all bits back
        v ^ 0xFFFF_FFFF_FFFF_FFFF
    };
    f64::from_bits(bits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_page_memory_alignment() {
        // If this fails, our mmap slices will panic at runtime.
        // We MUST guarantee the struct is exactly 4096 bytes.
        assert_eq!(
            std::mem::size_of::<NodePage>(),
            PAGE_SIZE,
            "NodePage must exactly match the 4KB OS Page size"
        );

        assert_eq!(
            std::mem::size_of::<AvNodePage>(),
            PAGE_SIZE,
            "AvNodePage must exactly match the 4KB OS Page size"
        );
    }

    #[test]
    fn test_i64_twiddling_sort_order() {
        let neg_large = -999999i64;
        let neg_small = -1i64;
        let zero = 0i64;
        let pos_small = 1i64;
        let pos_large = 999999i64;

        let enc_nl = encode_i64(neg_large);
        let enc_ns = encode_i64(neg_small);
        let enc_z = encode_i64(zero);
        let enc_ps = encode_i64(pos_small);
        let enc_pl = encode_i64(pos_large);

        // Prove natural unsigned byte sorting correctly orders signed integers!
        assert!(enc_nl < enc_ns);
        assert!(enc_ns < enc_z);
        assert!(enc_z < enc_ps);
        assert!(enc_ps < enc_pl);

        // Prove zero-loss round trip
        assert_eq!(decode_i64(enc_nl), neg_large);
        assert_eq!(decode_i64(enc_ns), neg_small);
        assert_eq!(decode_i64(enc_pl), pos_large);
    }

    #[test]
    fn test_f64_twiddling_sort_order() {
        let neg_large = -999.99f64;
        let neg_small = -0.01f64;
        let zero = 0.0f64;
        let pos_small = 0.01f64;
        let pos_large = 999.99f64;

        let enc_nl = encode_f64(neg_large);
        let enc_ns = encode_f64(neg_small);
        let enc_z = encode_f64(zero);
        let enc_ps = encode_f64(pos_small);
        let enc_pl = encode_f64(pos_large);

        // Prove IEEE 754 floats can be perfectly sorted as raw unsigned bytes!
        assert!(enc_nl < enc_ns);
        assert!(enc_ns < enc_z);
        assert!(enc_z < enc_ps);
        assert!(enc_ps < enc_pl);

        // Prove zero-loss round trip
        assert_eq!(decode_f64(enc_nl), neg_large);
        assert_eq!(decode_f64(enc_ns), neg_small);
        assert_eq!(decode_f64(enc_pl), pos_large);
    }
}
