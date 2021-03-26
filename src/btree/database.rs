use std::{cell::RefCell, rc::Rc};

use super::buffer_pool::BufferPool;

pub struct Database {
    buffer_pool: BPPointer,
}

type BPPointer = Rc<RefCell<BufferPool>>;

impl Database {
    pub fn new() -> Database {
        Database {
            buffer_pool: Rc::new(RefCell::new(BufferPool::new())),
        }
    }

    pub fn get_buffer_pool(&self) -> BPPointer {
        self.buffer_pool.clone()
    }
}