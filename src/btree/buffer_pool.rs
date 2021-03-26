use std::{cell::RefCell, collections::HashMap, rc::Rc};

use super::file::{BTreeLeafPage, BTreePageID};

// pub const BUFFER_POOL: HashMap<i32, BTreeLeafPage> = HashMap::new();

pub struct BufferPool {
    buffer: HashMap<Key, Value>,
}

type Key = BTreePageID;
type Value = Rc<RefCell<BTreeLeafPage>>;

impl BufferPool {
    pub fn new() -> BufferPool {
        BufferPool {
            buffer: HashMap::new(),
        }
    }

    pub fn get_page(&mut self, key: &Key) -> Option<&Value> {
        self.buffer.get(key)
    }
}
