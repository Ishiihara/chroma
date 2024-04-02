use crate::system::Receiver;
use crate::types::LogRecord;

use super::cache::{CacheOperationMessage, GetOperation, InsertOperation};

#[derive(Debug)]
pub struct LogCache {
    cache: Box<dyn Receiver<CacheOperationMessage<i64, LogRecord>>>,
}

impl Clone for LogCache {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
        }
    }
}

impl LogCache {
    pub fn new(cache: Box<dyn Receiver<CacheOperationMessage<i64, LogRecord>>>) -> Self {
        Self { cache }
    }

    pub async fn insert_log(&mut self, log: LogRecord) {
        let operation = Box::new(InsertOperation::new(log.log_offset, log));
        match self.cache.send(operation).await {
            Ok(_) => {}
            Err(e) => {
                println!("Error inserting log: {:?}", e)
            }
        }
    }

    pub async fn get_log(&mut self, log_offset: &i64) -> Option<LogRecord> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let operation = Box::new(GetOperation::new(*log_offset, tx));
        match self.cache.send(operation).await {
            Ok(_) => {}
            Err(e) => {
                println!("Error inserting log: {:?}", e)
            }
        }
        let value = rx.recv().await.unwrap();
        value
    }

    pub async fn get_logs(&mut self, offset: i64, batch_size: i32) -> Vec<LogRecord> {
        let mut logs = Vec::new();
        for i in 0..batch_size {
            let log_offset = offset + i as i64;
            let log = self.get_log(&log_offset).await;
            if let Some(log) = log {
                logs.push(log);
            }
        }
        logs
    }
}
