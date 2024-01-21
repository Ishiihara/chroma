use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::thread;

use crate::{
    assignment::assignment_policy::{self, AssignmentPolicy},
    config::{Configurable, WorkerConfig},
    errors::ChromaError,
    ingest::log::{CollectionRecord, FlushCursor, InMemoryLog, LogRecord, WriteCursor},
    memberlist::Memberlist,
    segment::flusher::Flusher,
    segment::segment_batch_ingestor::SegmentBatchIngestor,
    segment::segment_manager::SegmentManager,
    sysdb::sysdb::{GrpcSysDb, SysDb},
    system::{Component, ComponentContext, Handler},
    types::EmbeddingRecord,
};

use super::log::Log;

pub(crate) struct LogIngest {
    assignment_policy: RwLock<Box<dyn AssignmentPolicy + Sync + Send>>,
    assignments: RwLock<Vec<CollectionRecord>>,
    my_ip: String,
    log: Arc<RwLock<InMemoryLog<LogRecord>>>,
    sysdb: Box<dyn SysDb>,
    segment_ingestors: Vec<SegmentBatchIngestor>,
    segment_flusher: Flusher,
    write_cursors: RwLock<HashMap<String, WriteCursor>>,
    flush_cursors: RwLock<HashMap<String, FlushCursor>>,
}

pub(crate) struct Task {
    pub(crate) collection_id: String,
    pub(crate) tenant_id: String,
}

pub(crate) struct TaskDispatcher {
    log: Arc<RwLock<InMemoryLog<LogRecord>>>,
    sender: mpsc::Sender<Task>,
}

impl TaskDispatcher {
    pub(crate) fn new(
        log: Arc<RwLock<InMemoryLog<LogRecord>>>,
        sender: mpsc::Sender<Task>,
    ) -> TaskDispatcher {
        TaskDispatcher { log, sender }
    }

    pub(crate) fn get_collections(&self) -> Vec<CollectionRecord> {
        self.log.read().unwrap().get_collections()
    }

    pub(crate) fn dispatch(&self, task: Task) {
        self.sender.send(task);
    }
}

impl Component for LogIngest {
    fn queue_size(&self) -> usize {
        return 1000;
    }

    fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        let (task_tx, task_rx) = mpsc::channel::<Task>();
        let (result_tx, result_rx) = mpsc::channel();

        // Start the segment ingestors
        let task_receiver = Arc::new(Mutex::new(task_rx));
        for id in 0..self.segment_ingestors.len() {
            let receiver = task_receiver.clone();
            let sender = result_tx.clone();
            let ingestor_thread = thread::spawn(move || {
                let segment_ingestor = &self.segment_ingestors[id];
                loop {
                    let task = receiver.lock().unwrap().recv().unwrap();
                    let collection_id = task.collection_id;
                    let tenant_id = task.tenant_id;

                    // TODO: change this to active collections from the log service and get from the cursor
                    let records = self.log.read().unwrap().get(collection_id, 1000, 1000);
                    let records = match records {
                        Some(records) => records,
                        None => {
                            continue;
                        }
                    };
                    let mut batch = Vec::new();
                    for record in records {
                        // let embedding_record = EmbeddingRecord {};
                        // batch.push(Box::new(embedding_record));
                    }
                    // This is async, so we need to use await to drive the execution
                    let result = segment_ingestor.ingest(batch);
                    sender.send(result).unwrap();
                }
            });
        }

        for id in 0..self.segment_ingestors.len() {
            ctx.system
                .clone()
                .start_component(self.segment_ingestors[id]);
        }

        // Start the flushers, use one flusher for demonstration purposes, we can create multiple threads
        let (flusher_tx, flusher_rx) = mpsc::channel();
        let (flush_result_tx, flush_result_rx) = mpsc::channel();

        let flusher_thread = thread::spawn(move || loop {
            let flusher = &mut self.segment_flusher;
            let flusher_tx = flusher_tx.clone();
            let assignments = self.assignments.read().unwrap();
            for assignment in assignments.iter() {
                flusher.flush(assignment.id.clone());
            }
        });

        // Dispatch tasks to the segment ingestors
        let task_dispatcher = TaskDispatcher::new(self.log.clone(), task_tx);
        let dispatcher_thread = thread::spawn(move || loop {
            let collections = task_dispatcher.get_collections();
            for collection in collections {
                let task = Task {
                    collection_id: collection.id,
                    tenant_id: collection.tenant_id,
                };
                task_dispatcher.dispatch(task);
            }

            // Check the ingestion results in a loop
            loop {
                let result = result_rx.try_recv();
                match result {
                    // TODO: handle the ingestion re
                    Ok(result) => {
                        // Update the cursor for the collection
                    }
                    Err(_) => {
                        // For taaks that failed, we can put them into a retry queue and
                        // schedule them later
                        break;
                    }
                }
            }

            // Dispatch flush tasks
            {
                let cursor_state = self.flush_cursors.read().unwrap();

                for (collection_id, cursor) in cursor_state.iter() {
                    if cursor.is_flush_ready() {
                        flusher_tx.send(collection_id.clone()).unwrap();
                    }
                }
            }

            // Check the flush results, note that we don't need to check in a loop,
            // as flush can be async and we can use await to drive the execution.
            loop {
                let result = flush_result_rx.try_recv();
                match result {
                    Ok(result) => {
                        // Update the cursor for the collection
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        });

        flusher_thread.join().unwrap();
        dispatcher_thread.join().unwrap();
    }
}

impl Debug for LogIngest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogIngest").finish()
    }
}

#[async_trait]
impl Configurable for LogIngest {
    async fn try_from_config(worker_config: &WorkerConfig) -> Result<Self, Box<dyn ChromaError>> {
        let assignment_policy = assignment_policy::RendezvousHashingAssignmentPolicy::new(
            // Note: Pulsar tenant and namespace are not used in the rendezvous hashing assignment
            worker_config.pulsar_tenant.clone(),
            worker_config.pulsar_namespace.clone(),
        );

        let sysdb = GrpcSysDb::try_from_config(worker_config).await;
        let sysdb = match sysdb {
            Ok(sysdb) => sysdb,
            Err(err) => {
                return Err(err);
            }
        };
        let log = Arc::new(RwLock::new(InMemoryLog::new()));

        let segment_manager = SegmentManager::new(Box::new(sysdb), &worker_config.storage_path);
        let segment_ingestors = Vec::new();
        for _ in 0..worker_config.num_indexing_threads {
            let segment_ingestor = SegmentBatchIngestor::new(segment_manager.clone());
            segment_ingestors.push(segment_ingestor);
        }

        Ok(LogIngest {
            assignment_policy: RwLock::new(Box::new(assignment_policy)),
            assignments: RwLock::new(Vec::new()),
            my_ip: worker_config.my_ip.clone(),
            sysdb: Box::new(sysdb),
            log: log,
            segment_ingestors: segment_ingestors,
        })
    }
}

#[async_trait]
impl Handler<Memberlist> for LogIngest {
    async fn handle(&mut self, msg: Memberlist, ctx: &ComponentContext<Self>) {
        // We need to do the folloing:
        // 1. Get the list of collections that we are responsible for based on the memberlist and
        //  the assignment policy
        let mut new_assignments = Vec::new();

        // TODO: change this to active collections from the log service
        let collections = self
            .sysdb
            .get_collections(None, None, None, None, None)
            .await;

        let collections = match collections {
            Ok(collections) => collections,
            Err(err) => {
                println!("Failed to get collections from sysdb: {:?}", err);
                return;
            }
        };
        // Scope for assigner write lock to be released so we don't hold it over await
        {
            let mut assigner = match self.assignment_policy.write() {
                Ok(assigner) => assigner,
                Err(err) => {
                    println!("Failed to read assignment policy: {:?}", err);
                    return;
                }
            };
            assigner.set_members(msg);
            for collection in collections.iter() {
                let assignment = assigner.assign(&String::from(collection.id));
                let assignment = match assignment {
                    Ok(assignment) => assignment,
                    Err(err) => {
                        // TODO: Log error
                        continue;
                    }
                };
                if assignment == self.my_ip {
                    let new_assignment = CollectionRecord {
                        id: String::from(collection.id),
                        tenant_id: collection.tenant.clone(),
                    };
                    new_assignments.push(new_assignment);
                }
            }
        }

        // Expose the new assignment to the ingestor
        {
            let mut assignments = match self.assignments.write() {
                Ok(assignments) => assignments,
                Err(err) => {
                    println!("Failed to read assignments: {:?}", err);
                    return;
                }
            };
            *assignments = new_assignments;
        }
    }
}

// unit tests
#[cfg(test)]
mod tests {}
