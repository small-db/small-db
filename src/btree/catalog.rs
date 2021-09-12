use std::{cell::RefCell, collections::HashMap, rc::Rc};


use std::{mem, sync::Once};

use super::table::BTreeTable;

pub struct Catalog {
    map: HashMap<Key, Value>,
}

type Key = i32;
type Value = Rc<RefCell<BTreeTable>>;

impl Catalog {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn global() -> &'static mut Self {
        // Initialize it to a null value
        static mut SINGLETON: *mut Catalog = 0 as *mut Catalog;
        static ONCE: Once = Once::new();

        ONCE.call_once(|| {
            // Make it
            let singleton = Self::new();

            unsafe {
                // Put it in the heap so it can outlive this call
                SINGLETON = mem::transmute(Box::new(singleton));
            }
        });

        unsafe {
            // Now we give out a copy of the data that is safe to use
            // concurrently.
            SINGLETON.as_mut().unwrap()
        }
    }

    pub fn get_table(&self, key: &Key) -> Option<&Value> {
        self.map.get(key)
    }

    pub fn add_table(&mut self, file: Value) {
        self.map.insert(file.borrow().get_id(), Rc::clone(&file));
    }
}
