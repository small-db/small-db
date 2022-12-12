pub fn u32_to_bytes(num: u32) -> [u8; 4] {
    num.to_be_bytes()
}

pub fn bytes_to_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}
