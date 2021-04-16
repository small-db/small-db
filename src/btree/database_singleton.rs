use std::{rc::Rc, time::Duration};
use std::{
    cell::RefCell,
    sync::{Arc, Mutex, Once},
};
use std::{mem, thread};

use super::{buffer_pool::BufferPool, catalog::Catalog};

#[derive(Clone)]
pub struct SingletonDB {
    // Since we will be used in many threads, we need to protect
    // concurrent access
    // inner: Arc<Mutex<u8>>,
    buffer_pool: BPPointer,
    catalog: CTPointer,
}

type BPPointer = Arc<RefCell<BufferPool>>;
type CTPointer = Arc<RefCell<Catalog>>;

pub fn singleton_db() -> SingletonDB {
    // Initialize it to a null value
    static mut SINGLETON: *const SingletonDB = 0 as *const SingletonDB;
    static ONCE: Once = Once::new();

    unsafe {
        ONCE.call_once(|| {
            let bp = Arc::new(RefCell::new(BufferPool::new()));
            let ct = Arc::new(RefCell::new(Catalog::new()));

            // Make it
            let singleton = SingletonDB {
                buffer_pool: bp,
                catalog: ct,
            };

            // Put it in the heap so it can outlive this call
            SINGLETON = mem::transmute(Box::new(singleton));
        });

        // Now we give out a copy of the data that is safe to use concurrently.
        (*SINGLETON).clone()
    }
}

impl SingletonDB {
    pub fn new() -> Self {
        let bp  = Arc::new(RefCell::new(BufferPool::new()));
        let ct = Arc::new(RefCell::new(Catalog::new()));
        Self {
            buffer_pool: bp,
            catalog: ct,
        }
    }

    pub fn get_buffer_pool(&self) -> BPPointer {
        self.buffer_pool.clone()
    }

    pub fn get_catalog(&self) -> CTPointer {
        self.catalog.clone()
    }
}
