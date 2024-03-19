use crate::compactor::compaction_job::CompactionJob;
use crate::compactor::compaction_job::CompactionStatus;
use crate::compactor::compaction_task::DedupTask;
use crate::compactor::compaction_task::FlushTask;
use crate::compactor::compaction_task::FlushTaskResponse;
use crate::compactor::compaction_task::PullTask;
use crate::compactor::compaction_task::ScanTask;
use crate::compactor::compaction_task::TaskStatus;
use crate::compactor::compaction_task::WriteTask;
use crate::compactor::compaction_task::WriteTaskResponse;
use crate::compactor::scheduler::Scheduler;
use crate::compactor::types::Task;
use crate::log::log::Log;
use crate::segment::SegmentManager;
use crate::system::dedicated_executor::JobError;
use crate::system::Component;
use crate::system::ComponentContext;
use crate::system::Handler;
use crate::system::Receiver;
use crate::types::EmbeddingRecord;
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use uuid::Uuid;

use super::compaction_task::TaskExecution;

pub(crate) struct CompactionManager {
    log: Box<dyn Log>,
    scheduler: Arc<Scheduler>,
    segment_manager: SegmentManager,
    compaction_jobs: HashMap<Uuid, CompactionJob>,
    flush_worker: Box<dyn Receiver<FlushTask>>,
    compaction_writer: Box<dyn Receiver<WriteTask>>,
    ctx: Option<ComponentContext<CompactionManager>>,
}

impl CompactionManager {
    pub(crate) fn new(
        log: Box<dyn Log>,
        scheduler: Arc<Scheduler>,
        segment_manager: SegmentManager,
        flush_worker: Box<dyn Receiver<FlushTask>>,
        compaction_writer: Box<dyn Receiver<WriteTask>>,
    ) -> Self {
        CompactionManager {
            log,
            scheduler,
            segment_manager,
            compaction_jobs: HashMap::new(),
            flush_worker,
            compaction_writer,
            ctx: None,
        }
    }

    async fn dispatch_write_task(&mut self, write_task: WriteTask) {
        self.compaction_writer
            .send(write_task)
            .await
            .expect("Failed to send write task");
    }

    async fn dispatch_flush_task(&mut self, flush_task: FlushTask) {
        self.flush_worker
            .send(flush_task)
            .await
            .expect("Failed to send flush task");
    }

    async fn handle_write_task_response(&mut self, response: WriteTaskResponse) {
        let task_id = response.response.task_id;
        let job_id = response.response.job_id;
        let status = response.response.status;
        let handle = response.response.result;
        let job = self
            .compaction_jobs
            .get_mut(&job_id)
            .expect("Failed to get compaction job");
        match status {
            TaskStatus::Running => {
                job.task_handles.push(handle.unwrap());
            }
            TaskStatus::Done => {
                job.finished_write_tasks += 1;
                if job.finished_write_tasks == job.num_write_tasks {
                    job.status = CompactionStatus::Flushing;
                    let flush_task = FlushTask::new(job.task.clone(), job.segment_manager.clone());
                    let _ = self.dispatch_flush_task(flush_task).await;
                }
            }
            TaskStatus::Failed => {
                let write_task = job.write_tasks.get(&task_id).unwrap();
                let write_task_clone = write_task.clone();
                let _ = self.dispatch_write_task(write_task_clone).await;
            }
            TaskStatus::Timeout => {
                let write_task = job.write_tasks.get(&task_id).unwrap();
                // TODO: do not clone the write task
                let write_task_clone = write_task.clone();
                let _ = self.dispatch_write_task(write_task_clone).await;
            }
        }
    }

    async fn handle_new_task(&mut self, task: Task) -> Vec<WriteTask> {
        let num_write_tasks = 1;
        let mut compaction_job =
            CompactionJob::new(task.clone(), num_write_tasks, self.segment_manager.clone());
        let job_id = compaction_job.get_job_id();

        let mut scan_task = ScanTask::new(job_id, task.clone(), self.log.clone());
        compaction_job.status = CompactionStatus::Scanning;
        let records = scan_task.run().await;

        let mut dedup_task = DedupTask::new(job_id, task.clone(), records);
        compaction_job.status = CompactionStatus::Deduping;
        let records = dedup_task.run().await;
        // partition records into num_write_tasks
        let partitioned_records = self.partition_records(records, num_write_tasks);
        for records in partitioned_records {
            let write_task =
                WriteTask::new(job_id, task.clone(), records, self.segment_manager.clone());
            compaction_job.add_write_task(write_task.clone());
        }

        let write_tasks: Vec<WriteTask> = compaction_job.write_tasks.values().cloned().collect();
        self.compaction_jobs.insert(job_id, compaction_job);

        write_tasks
    }

    fn partition_records(
        &self,
        records: Vec<Box<EmbeddingRecord>>,
        num_partitions: i32,
    ) -> Vec<Vec<Box<EmbeddingRecord>>> {
        // partition records into num_partitions, order by the input record index
        let chunk_size = (records.len() as f64 / num_partitions as f64).ceil() as usize;
        records
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect()
    }

    pub(crate) async fn pull_task(&mut self) -> Result<Task, JobError> {
        let ctx = self.ctx.as_ref().unwrap();
        let pull_task = PullTask::new(self.scheduler.clone());
        let job: crate::system::JobHandle<Task> = ctx.system.invoke(pull_task);
        let task = job.job.await;
        task
    }

    pub(crate) async fn process_write_tasks(&mut self, write_tasks: Vec<WriteTask>) {
        let mut write_task_futures = Vec::new();
        for write_task in write_tasks {
            let write_task_job = self.ctx.as_ref().unwrap().system.invoke(write_task);
            write_task_futures.push(write_task_job.job);
        }
        futures::future::join_all(write_task_futures).await;
    }

    pub(crate) async fn compact(&mut self) {
        loop {
            tokio::select! {
                task = async {self.scheduler.take_task()} => {
                    match task {
                        Some(task) => {
                            self.handle_new_task(task).await;

                        }
                        None => {}
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Component for CompactionManager {
    type Output = ();

    fn queue_size(&self) -> usize {
        1000
    }

    fn on_start(&mut self, ctx: &ComponentContext<Self>) -> () {
        let ctx = ComponentContext {
            system: ctx.system.clone(),
            sender: ctx.sender.clone(),
            cancellation_token: ctx.cancellation_token.clone(),
            scheduler: ctx.scheduler.clone(),
        };
        self.ctx = Some(ctx);
    }

    async fn run_task(&mut self, ctx: &ComponentContext<Self>) -> () {
        // loop {
        //     let task = self.pull_task().await;
        //     let task = match task {
        //         Ok(task) => task,
        //         Err(_) => {
        //             continue;
        //         }
        //     };
        //     let write_tasks = self.handle_new_task(task).await;
        //     self.process_write_tasks(write_tasks).await;
        //     //     futures::future::join_all(write_task_futures).await;
        //     //     let flush_task = FlushTask::new(job_id, task.clone(), self.segment_manager.clone());
        // }

        tokio::select! {
            task = async {self.scheduler.take_task()} => {
                match task {
                    Some(task) => {
                        let write_tasks = self.handle_new_task(task).await;
                        self.process_write_tasks(write_tasks).await;
                    }
                    None => {}
                }
            }
        }
    }
}

#[async_trait]
impl Handler<WriteTaskResponse> for CompactionManager {
    async fn handle(
        &mut self,
        event: WriteTaskResponse,
        ctx: &ComponentContext<CompactionManager>,
    ) {
        self.handle_write_task_response(event).await;
    }
}

#[async_trait]
impl Handler<FlushTaskResponse> for CompactionManager {
    async fn handle(
        &mut self,
        event: FlushTaskResponse,
        ctx: &ComponentContext<CompactionManager>,
    ) {
        // handle flush task response
    }
}

#[async_trait]
impl Handler<Task> for CompactionManager {
    async fn handle(&mut self, event: Task, ctx: &ComponentContext<CompactionManager>) {
        // handle new task from scheduler
        // Two options:
        // 1. handle the task here
        // 2. dispatch the task to the executor
        self.handle_new_task(event).await;
    }
}

#[async_trait]

impl Debug for CompactionManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CompactionManager")
    }
}
