use std::{cell::RefMut, mem};
use std::rc::Rc;
use std::{cell::RefCell, sync::Once};

use super::{buffer_pool::BufferPool, catalog::Catalog};

#[derive(Clone)]
pub struct SingletonDB {
    // Since we will be used in many threads, we need to protect
    // concurrent access
    // inner: Arc<Mutex<u8>>,
    buffer_pool: BPPointer,
    catalog: CTPointer,
}

type BPPointer = Rc<RefCell<BufferPool>>;
type CTPointer = Rc<RefCell<Catalog>>;

pub fn singleton_db() -> &'static SingletonDB {
    // Initialize it to a null value
    static mut SINGLETON: *const SingletonDB = 0 as *const SingletonDB;
    static ONCE: Once = Once::new();

    unsafe {
        ONCE.call_once(|| {
            let bp = Rc::new(RefCell::new(BufferPool::new()));
            let ct = Rc::new(RefCell::new(Catalog::new()));

            // Make it
            let singleton = SingletonDB {
                buffer_pool: bp,
                catalog: ct,
            };

            // Put it in the heap so it can outlive this call
            SINGLETON = mem::transmute(Box::new(singleton));
        });

        // Now we give out a copy of the data that is safe to use concurrently.
        // (*SINGLETON).clone()
        SINGLETON.as_ref().unwrap()
    }
}

impl SingletonDB {
    pub fn new() -> Self {
        let bp = Rc::new(RefCell::new(BufferPool::new()));
        let ct = Rc::new(RefCell::new(Catalog::new()));
        Self {
            buffer_pool: bp,
            catalog: ct,
        }
    }

    pub fn get_buffer_pool(&self) -> RefMut<BufferPool> {
        // let container = Rc::clone(&self.buffer_pool);
        // (*container).borrow_mut()

        // Rc::clone(&self.buffer_pool).borrow_mut()

        self.buffer_pool.borrow_mut()
    }

    pub fn get_catalog(&self) -> CTPointer {
        self.catalog.clone()
    }
}
