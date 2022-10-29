use std::sync::{Arc, RwLock};

use crate::error::SimpleError;

// Type alias, not a new type, cannot define methods on it
pub type Pod<T> = Arc<RwLock<T>>;

// Define a new type, can define methods on it, but different with the
// underlying type, so the original methods cannot be used
// pub struct Pod<T>(Arc<RwLock<T>>);

pub type ResultPod<T> = Result<Pod<T>, SimpleError>;
pub type SimpleResult = Result<(), SimpleError>;
