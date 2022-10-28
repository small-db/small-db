use std::sync::{Arc, RwLock};

use crate::error::SimpleError;

pub type Pod<T> = Arc<RwLock<T>>;
pub type ResultPod<T> = Result<Pod<T>, SimpleError>;
pub type SimpleResult = Result<(), SimpleError>;
