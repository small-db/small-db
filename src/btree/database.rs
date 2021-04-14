use std::{
    cell::RefCell,
    rc::{Rc, Weak},
};

use super::{buffer_pool::BufferPool, catalog::Catalog};

pub struct Database {
    buffer_pool: BPPointer,
    catalog: CTPointer,
}

type BPPointer = Rc<RefCell<BufferPool>>;
type CTPointer = Rc<RefCell<Catalog>>;

impl Database {
    pub fn new() -> Rc<Database> {
        let bp  = Rc::new(RefCell::new(BufferPool::new()));
        let ct = Rc::new(RefCell::new(Catalog::new()));
        let db = Database {
            buffer_pool: bp,
            catalog: ct,
        };

        let pointer = Rc::new(db);
        pointer.buffer_pool.borrow_mut().set_db(Rc::clone(&pointer));

        // Rc::new(db)
        pointer
    }

    pub fn get_buffer_pool(&self) -> BPPointer {
        self.buffer_pool.clone()
    }

    pub fn get_catalog(&self) -> CTPointer {
        self.catalog.clone()
    }
}
