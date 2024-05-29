use std::{
    hash::{DefaultHasher, Hash, Hasher},
    time::SystemTime,
};

pub struct Schema {
    pub id: u32,

    pub name: String,
}

impl Schema {
    pub fn new(name: &str) -> Self {
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);

        let unix_time = SystemTime::now();
        unix_time.hash(&mut hasher);

        let id = hasher.finish() as u32;

        Self {
            id,
            name: name.to_string(),
        }
    }
}
