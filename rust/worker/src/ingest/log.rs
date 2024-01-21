use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

#[derive(Clone)]
pub(crate) struct Task {
    id: i32,
    collection_id: String,
    tenant_id: String,
    cursor: i64,
    log: Arc<RwLock<InMemoryLog<LogRecord>>>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogRecord {
    collection_id: String,
    tenant_id: String,
    id: i64,
    offset: i64,
}

pub(crate) struct WriteCursor {
    collection_id: String,
    tenant_id: String,
    cursor: i64,
}

pub(crate) struct FlushCursor {
    collection_id: String,
    tenant_id: String,
    cursor: i64,
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub(crate) struct CollectionRecord {
    pub(crate) id: String,
    pub(crate) tenant_id: String,
}

pub(crate) trait Log<T> {
    fn push(&mut self, msg: T);
    fn get(&self, collection_id: String, index: usize, batch_size: usize) -> Option<Vec<T>>;
    fn get_collections(&self) -> Vec<CollectionRecord>;
}

pub(crate) struct InMemoryLog<T> {
    logs: HashMap<String, Vec<T>>,
}

impl<T> InMemoryLog<T> {
    pub fn new() -> InMemoryLog<T> {
        InMemoryLog {
            logs: HashMap::new(),
        }
    }
}

impl Log<LogRecord> for InMemoryLog<LogRecord> {
    fn push(&mut self, msg: LogRecord) {
        let collection_id = msg.collection_id.clone();
        if !self.logs.contains_key(&collection_id) {
            self.logs.insert(collection_id.clone(), Vec::new());
        }
        self.logs.get_mut(&collection_id).unwrap().push(msg);
    }

    fn get(
        &self,
        collection_id: String,
        index: usize,
        batch_size: usize,
    ) -> Option<Vec<LogRecord>> {
        if !self.logs.contains_key(&collection_id) {
            return None;
        }
        let logs = self.logs.get(&collection_id).unwrap();
        let mut result = Vec::new();
        for i in index..index + batch_size {
            if i >= logs.len() {
                break;
            }
            result.push(logs[i].clone());
        }
        Some(result)
    }

    fn get_collections(&self) -> Vec<CollectionRecord> {
        let mut result = HashSet::new();
        for (collection_id, log_records) in self.logs.iter() {
            log_records.iter().for_each(|log_record| {
                // TODO: only need to check the first record
                let collection_record = CollectionRecord {
                    id: collection_id.clone(),
                    tenant_id: log_record.tenant_id.clone(),
                };
                result.insert(collection_record);
            });
        }
        result.into_iter().collect()
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_log() {
        let mut log = InMemoryLog::new();
        let log_record = LogRecord {
            collection_id: String::from("collection1"),
            tenant_id: String::from("tenant1"),
            id: 1,
            offset: 0,
        };
        log.push(log_record.clone());
        let log_record2 = LogRecord {
            collection_id: String::from("collection1"),
            tenant_id: String::from("tenant1"),
            id: 2,
            offset: 0,
        };
        log.push(log_record2.clone());
        let log_record3 = LogRecord {
            collection_id: String::from("collection2"),
            tenant_id: String::from("tenant1"),
            id: 3,
            offset: 0,
        };
        log.push(log_record3.clone());
        let log_record4 = LogRecord {
            collection_id: String::from("collection2"),
            tenant_id: String::from("tenant2"),
            id: 4,
            offset: 0,
        };
        log.push(log_record4.clone());
    }
}
