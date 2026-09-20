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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_page_memory_alignment() {
        // Military-grade check: If this fails, our mmap slices will panic at runtime.
        // We MUST guarantee the struct is exactly 4096 bytes.
        assert_eq!(
            std::mem::size_of::<NodePage>(),
            PAGE_SIZE,
            "NodePage must exactly match the 4KB OS Page size"
        );
    }
}
