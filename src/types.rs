use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use crate::{error::SmallError, utils::HandyRwLock};

// Type alias, not a new type, cannot define methods on it
pub type Pod<T> = Arc<RwLock<T>>;

// Define a new type, can define methods on it, but different with the
// underlying type, so the original methods cannot be used
// pub struct Pod<T>(Arc<RwLock<T>>);

pub type ResultPod<T> = Result<Pod<T>, SmallError>;
pub type SmallResult = Result<(), SmallError>;

pub struct ConcurrentHashMap<K, V> {
    map: Arc<RwLock<HashMap<K, V>>>,
}

impl<K, V> ConcurrentHashMap<K, V> {
    pub fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_inner(&self) -> Arc<RwLock<HashMap<K, V>>> {
        self.map.clone()
    }

    pub fn get_inner_rl(&self) -> RwLockReadGuard<HashMap<K, V>> {
        self.map.rl()
    }

    pub fn get_inner_wl(&self) -> RwLockWriteGuard<HashMap<K, V>> {
        self.map.wl()
    }

    pub fn get_or_insert(
        &self,
        key: &K,
        value_gen_fn: impl Fn(&K) -> Result<V, SmallError>,
    ) -> Result<V, SmallError>
    where
        K: std::cmp::Eq + std::hash::Hash + Clone,
        V: Clone,
    {
        let mut buffer = self.map.wl();
        match buffer.get(&key) {
            Some(v) => Ok(v.clone()),
            None => {
                let v = value_gen_fn(key)?;
                buffer.insert(key.clone(), v.clone());
                Ok(v)
            }
        }
    }

    pub fn alter_value(
        &self,
        key: &K,
        alter_fn: impl Fn(&mut V) -> Result<(), SmallError>,
    ) -> Result<(), SmallError>
    where
        K: std::cmp::Eq + std::hash::Hash + Clone,
        V: Clone + std::default::Default,
    {
        let mut map = self.map.wl();

        if let Some(v) = map.get_mut(key) {
            alter_fn(v)
        } else {
            let mut new_v = Default::default();
            alter_fn(&mut new_v)?;
            map.insert(key.clone(), new_v);
            Ok(())
        }
    }

    /// Return true if `map[&k] == v`, or `map[&k]` is not exist.
    ///
    /// Return false if `map[&k] != v`.
    pub fn exact_or_empty(&self, k: &K, v: &V) -> bool
    where
        K: std::cmp::Eq + std::hash::Hash,
        V: std::cmp::Eq,
    {
        let map = self.map.rl();
        map.get(k).map_or(true, |v2| v == v2)
    }

    pub fn clear(&self) {
        self.map.wl().clear();
    }

    pub fn remove(&self, key: &K) -> Option<V>
    where
        K: std::cmp::Eq + std::hash::Hash,
    {
        self.map.wl().remove(key)
    }

    pub fn insert(&self, key: K, value: V) -> Option<V>
    where
        K: std::cmp::Eq + std::hash::Hash,
    {
        self.map.wl().insert(key, value)
    }
}

pub struct SmallLock {
    name: String,
    lock: Arc<Mutex<()>>,
}

impl SmallLock {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn lock(&self) -> std::sync::MutexGuard<()> {
        self.lock.lock().unwrap()
    }
}

impl Drop for SmallLock {
    fn drop(&mut self) {
        println!("> Dropping {}", self.name);
    }
}

#[cfg(test)]
mod tests {
    use std::thread::{self, sleep};

    use log::debug;

    use crate::utils::init_log;

    #[test]
    fn test_small_lock() {
        init_log();
        {
            let lock = super::SmallLock::new("test");
            let _guard = lock.lock();
            debug!("Locking");
        }
        debug!("Dropped");

        let global_lock = super::SmallLock::new("global");
        thread::scope(|s| {
            let mut threads = vec![];
            for _ in 0..5 {
                let handle = s.spawn(|| {
                    let thread_name =
                        format!("thread-{:?}", thread::current().id());
                    debug!("{}: start", thread_name);
                    {
                        // We have to give the guard a name, otherwise it will
                        // be dropped immediately. (i.e, this block of code will
                        // be protected by the lock)
                        let _guard = global_lock.lock();
                        sleep(std::time::Duration::from_millis(10));
                        debug!("{}: lock acquired", thread_name);
                        sleep(std::time::Duration::from_millis(1000));
                    }
                    debug!("{}: end", thread_name);
                });
                threads.push(handle);
            }

            for handle in threads {
                handle.join().unwrap();
            }
        });
    }
}
