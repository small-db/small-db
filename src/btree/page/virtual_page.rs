use super::BTreePageID;

pub struct BTreeVirtualPage {
    pid: BTreePageID,
    bytes: Vec<u8>,
    key_field: usize,
}

impl BTreeVirtualPage {
    pub fn get_pid(&self) -> BTreePageID {
        self.pid
    }

    // TODO: find a way to remove the clone
    pub fn get_bytes(&self) -> Vec<u8> {
        self.bytes.clone()
    }

    pub fn get_key_field(&self) -> usize {
        self.key_field
    }
}
