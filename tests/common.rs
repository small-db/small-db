use log::info;
use rand::prelude::*;
use std::{
    cell::{Ref, RefCell},
    rc::Rc,
};

use simple_db_rust::*;

pub fn setup() {
    test_utils::init_log();
    btree::buffer_pool::BufferPool::global().clear();
}
