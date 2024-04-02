use crate::system::Component;
use crate::system::ComponentContext;
use crate::system::Handler;
use async_trait::async_trait;
use std::fmt::Debug;
use std::hash::Hash;

pub trait Cache<K, V>: Send + Sync + Debug + CacheClone<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug + 'static,
    V: Clone + 'static,
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
    K: PartialEq + Eq + Hash + Clone + Debug + 'static,
    V: Clone + 'static,
{
    fn clone_box(&self) -> Box<dyn Cache<K, V>> {
        Box::new(self.clone())
    }
}

// Cache operation trait
pub(crate) trait CacheOperation<K, V>: Send + Sync + Debug
where
    K: PartialEq + Eq + Hash + Clone + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    fn get_key(&mut self) -> K;
    fn get_value(&mut self) -> V;
}

// Insert operation
#[derive(Debug)]
pub(crate) struct InsertOperation<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    key: K,
    value: V,
}

impl<K, V> InsertOperation<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug + Send + Sync + 'static,
    V: Clone + Debug + Send + Sync + 'static,
{
    pub fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}

impl<K, V> CacheOperation<K, V> for InsertOperation<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug + Send + Sync + 'static,
    V: Clone + Debug + Send + Sync + 'static,
{
    fn get_key(&mut self) -> K {
        self.key.clone()
    }

    fn get_value(&mut self) -> V {
        self.value.clone()
    }
}

// Get operation
#[derive(Debug)]
pub(crate) struct GetOperation<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    key: K,
    value: Option<V>,
    reply_channel: tokio::sync::mpsc::Sender<Option<V>>,
}

impl<K, V> GetOperation<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug + Send + Sync + 'static,
    V: Clone + Debug + Send + Sync + 'static,
{
    pub fn new(key: K, reply_channel: tokio::sync::mpsc::Sender<Option<V>>) -> Self {
        Self {
            key,
            value: None,
            reply_channel,
        }
    }
}

impl<K, V> CacheOperation<K, V> for GetOperation<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug + Send + Sync + 'static,
    V: Clone + Debug + Send + Sync + 'static,
{
    fn get_key(&mut self) -> K {
        self.key.clone()
    }

    fn get_value(&mut self) -> V {
        self.value.take().unwrap().clone()
    }
}

#[derive(Debug)]
pub(crate) struct CacheManager<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug + Send + Sync + 'static,
    V: Clone + Debug + Send + Sync + 'static,
{
    cache: Box<dyn Cache<K, V>>,
}

impl<K, V> Component for CacheManager<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug + Send + Sync + 'static,
    V: Clone + Debug + Send + Sync + 'static,
{
    fn queue_size(&self) -> usize {
        1000
    }
}

pub(crate) type CacheOperationMessage<K, V> = Box<dyn CacheOperation<K, V>>;

#[async_trait]
impl<K, V> Handler<InsertOperation<K, V>> for CacheManager<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug + Send + Sync + 'static,
    V: Clone + Debug + Send + Sync + 'static,
{
    async fn handle(
        &mut self,
        operation: InsertOperation<K, V>,
        _ctx: &ComponentContext<CacheManager<K, V>>,
    ) {
        let mut operation = operation;
        let key = operation.get_key();
        let value = operation.get_value();
        self.cache.insert(key, value);
    }
}

#[async_trait]
impl<K, V> Handler<GetOperation<K, V>> for CacheManager<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug + Send + Sync + 'static,
    V: Clone + Debug + Send + Sync + 'static,
{
    async fn handle(
        &mut self,
        operation: GetOperation<K, V>,
        _ctx: &ComponentContext<CacheManager<K, V>>,
    ) {
        let mut operation = operation;
        let key = operation.get_key();
        let value = self.cache.get(&key);
        operation.reply_channel.send(value).await.unwrap();
    }
}
