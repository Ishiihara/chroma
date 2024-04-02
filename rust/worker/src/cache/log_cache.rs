use crate::cache::cache::Cache;
use crate::types::LogRecord;

#[derive(Debug)]
pub struct LogCache {
    log_cache: Box<dyn Cache<i64, LogRecord>>,
}

impl Clone for LogCache {
    fn clone(&self) -> Self {
        Self {
            log_cache: self.log_cache.clone_box(),
        }
    }
}

impl LogCache {
    pub fn new(log_cache: Box<dyn Cache<i64, LogRecord>>) -> Self {
        Self { log_cache }
    }

    pub fn insert_log(&mut self, log: LogRecord) {
        self.log_cache.insert(log.log_offset, log);
    }

    pub fn get_log(&mut self, log_offset: &i64) -> Option<LogRecord> {
        self.log_cache.get(log_offset)
    }

    pub fn get_logs(&mut self, offset: i64, batch_size: i32) -> Vec<LogRecord> {
        let mut logs = Vec::new();
        for i in 0..batch_size {
            logs.push(self.log_cache.get(&(offset + i as i64)).unwrap());
        }
        logs
    }
}
