use std::fmt::Debug;
use std::hash::Hash;

pub trait Cache<K, V>: Send + Sync + Debug + CacheClone<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug,
    V: Clone,
{
    /// Inserts a new entry into the cache.
    fn insert(&mut self, key: K, value: V);

    /// Retrieves the value associated with the given key from the cache.
    fn get(&mut self, key: &K) -> Option<V>;
}

pub(crate) trait CacheClone<K, V>
where
    K: Clone,
    V: Clone,
{
    fn clone_box(&self) -> Box<dyn Cache<K, V>>;
}

impl<T, K, V> CacheClone<K, V> for T
where
    T: 'static + Cache<K, V> + Clone,
    K: PartialEq + Eq + Hash + Clone + Debug,
    V: Clone,
{
    fn clone_box(&self) -> Box<dyn Cache<K, V>> {
        Box::new(self.clone())
    }
}
