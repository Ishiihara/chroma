use crate::compactor::scheduler::Scheduler;
use crate::compactor::types::Task;
use crate::log::log::Log;
use crate::segment::SegmentManager;
use crate::system::dedicated_executor::Job;
use crate::system::dedicated_executor::JobError;
use crate::system::Component;
use crate::system::ComponentContext;
use crate::system::ComponentRuntime;
use crate::system::Receiver;
use crate::types::EmbeddingRecord;
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use uuid::Uuid;

#[derive(PartialEq, Debug)]
pub(crate) enum TaskStatus {
    Running,
    Done,
    Timeout,
    Failed,
}

#[derive(Debug)]
pub(crate) struct TaskResponse<O> {
    pub(crate) task_id: Uuid,
    pub(crate) job_id: Uuid,
    pub(crate) status: TaskStatus,
    pub(crate) result: O,
}

#[async_trait]
pub(crate) trait TaskExecution<O, E>: Send + Sync + 'static
where
    O: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn run(&self) -> O;
    async fn done(&self, output: O) -> E;

    async fn send(&self, response: E, sender: Box<dyn Receiver<E>>) {
        match sender.send(response).await {
            Ok(_) => {}
            Err(_) => {}
        }
    }

    async fn run_async(&self, sender: Box<dyn Receiver<E>>) {
        let self_arc = Arc::new(self);
        let output = self_arc.run().await;
        let response = self_arc.done(output).await;
        self_arc.send(response, sender).await;
    }
}

#[async_trait]
pub(crate) trait AsyncTask: Send + Sync + Debug + 'static {
    type Output: Send + Sync + 'static;
    type Response: Send + Sync + 'static;
    async fn run(&self) -> Self::Output;
    async fn done(&self, output: Self::Output) -> Self::Response;
    async fn send(&self, response: Self::Response, sender: Box<dyn Receiver<Self::Response>>) {
        match sender.send(response).await {
            Ok(_) => {}
            Err(_) => {}
        }
    }

    async fn run_async(&mut self, sender: Box<dyn Receiver<Self::Response>>) {
        let self_arc = Arc::new(self);
        let output = self_arc.run().await;
        let response = self_arc.done(output).await;
        self_arc.send(response, sender).await;
    }
}

pub(crate) struct AsyncTaskWrapper<O, E>
where
    O: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    sender: Box<dyn Receiver<E>>,
    task: Box<dyn TaskExecution<O, E>>,
}

impl<O, E> AsyncTaskWrapper<O, E>
where
    O: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    pub(crate) fn new(sender: Box<dyn Receiver<E>>, task: Box<dyn TaskExecution<O, E>>) -> Self {
        AsyncTaskWrapper { sender, task }
    }

    pub(crate) async fn run(&self) {
        self.task.run_async(self.sender.clone()).await;
    }
}

type AsyncWriteTask = AsyncTaskWrapper<Option<Job<()>>, WriteTaskResponse>;

#[async_trait]
impl Component for AsyncWriteTask {
    type Output = Result<(), JobError>;

    fn queue_size(&self) -> usize {
        100
    }

    fn runtime() -> ComponentRuntime {
        ComponentRuntime::MultiThread
    }

    async fn run_task(&mut self, ctx: &ComponentContext<Self>) -> Result<(), JobError> {
        self.run().await;
        Ok(())
    }
}

impl Debug for AsyncWriteTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AsyncWriteTask")
    }
}

#[derive(Clone)]
pub(crate) struct ScanTask {
    task_id: Uuid,
    job_id: Uuid,
    task: Task,
    log: Box<dyn Log>,
    batch_size: i32,
}

pub(crate) struct ScanTaskResponse {
    response: TaskResponse<Vec<Box<EmbeddingRecord>>>,
}

impl ScanTask {
    pub(crate) fn new(job_id: Uuid, task: Task, log: Box<dyn Log>) -> Self {
        ScanTask {
            task_id: Uuid::new_v4(),
            job_id,
            task,
            log,
            batch_size: 1000,
        }
    }
}

#[async_trait]
impl TaskExecution<Vec<Box<EmbeddingRecord>>, ScanTaskResponse> for ScanTask {
    async fn run(&self) -> Vec<Box<EmbeddingRecord>> {
        let task = self.task.clone();
        let mut log = self.log.clone();
        let records = log
            .read(task.collection_id, task.offset, self.batch_size)
            .await;
        match records {
            Ok(records) => records,
            Err(_) => Vec::new(),
        }
    }

    async fn done(&self, output: Vec<Box<EmbeddingRecord>>) -> ScanTaskResponse {
        ScanTaskResponse {
            response: TaskResponse {
                task_id: self.task_id,
                job_id: self.job_id,
                status: TaskStatus::Done,
                result: output,
            },
        }
    }
}

#[derive(Clone)]
pub(crate) struct DedupTask {
    task_id: Uuid,
    job_id: Uuid,
    task: Task,
    records: Vec<Box<EmbeddingRecord>>,
}

impl DedupTask {
    pub(crate) fn new(job_id: Uuid, task: Task, records: Vec<Box<EmbeddingRecord>>) -> Self {
        DedupTask {
            task_id: Uuid::new_v4(),
            job_id,
            task,
            records,
        }
    }
}

pub(crate) struct DedupTaskResponse {
    response: TaskResponse<Vec<Box<EmbeddingRecord>>>,
}

#[async_trait]
impl TaskExecution<Vec<Box<EmbeddingRecord>>, DedupTaskResponse> for DedupTask {
    async fn run(&self) -> Vec<Box<EmbeddingRecord>> {
        let mut deduped_records = HashMap::new();
        for record in self.records.iter() {
            let key = record.id.clone();
            deduped_records.insert(key, record.clone());
        }
        deduped_records.values().cloned().collect()
    }

    async fn done(&self, output: Vec<Box<EmbeddingRecord>>) -> DedupTaskResponse {
        DedupTaskResponse {
            response: TaskResponse {
                task_id: self.task_id,
                job_id: self.job_id,
                status: TaskStatus::Done,
                result: output,
            },
        }
    }
}

#[derive(Clone)]
pub(crate) struct WriteTask {
    pub(crate) task_id: Uuid,
    pub(crate) job_id: Uuid,
    pub(crate) task: Task,
    pub(crate) records: Vec<Box<EmbeddingRecord>>,
    pub(crate) segment_manager: SegmentManager,
}

#[derive(Debug)]
pub(crate) struct WriteTaskResponse {
    pub(crate) response: TaskResponse<Option<Job<()>>>,
}

impl PartialEq for WriteTaskResponse {
    fn eq(&self, other: &Self) -> bool {
        self.response.job_id == other.response.job_id
            && self.response.status == other.response.status
    }
}

impl WriteTask {
    pub(crate) fn new(
        job_id: Uuid,
        task: Task,
        records: Vec<Box<EmbeddingRecord>>,
        segment_manager: SegmentManager,
    ) -> Self {
        WriteTask {
            task_id: Uuid::new_v4(),
            job_id,
            task,
            records,
            segment_manager,
        }
    }

    pub(crate) fn get_id(&self) -> Uuid {
        self.task_id
    }
}

#[async_trait]
impl TaskExecution<Option<Job<()>>, WriteTaskResponse> for WriteTask {
    async fn run(&self) -> Option<Job<()>> {
        let mut segment_manager = self.segment_manager.clone();
        for record in self.records.iter() {
            segment_manager.write_record(record.clone()).await;
        }
        None
    }

    async fn done(&self, output: Option<Job<()>>) -> WriteTaskResponse {
        WriteTaskResponse {
            response: TaskResponse {
                task_id: self.task_id,
                job_id: self.job_id,
                status: TaskStatus::Done,
                result: output,
            },
        }
    }
}

#[async_trait]
impl Component for WriteTask {
    type Output = Result<WriteTaskResponse, JobError>;

    fn queue_size(&self) -> usize {
        100
    }

    fn runtime() -> ComponentRuntime {
        ComponentRuntime::MultiThread
    }

    // async fn run_task(
    //     &mut self,
    //     ctx: &ComponentContext<Self>,
    // ) -> Result<WriteTaskResponse, JobError> {
    //     let sender = ctx.sender.clone();
    //     self.run_async(sender).await;
    //}
    async fn run_task(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Result<WriteTaskResponse, JobError> {
        let sender = ctx.sender.clone();
        //self.run_async(sender).await;
        Ok(WriteTaskResponse {
            response: TaskResponse {
                task_id: self.task_id,
                job_id: self.job_id,
                status: TaskStatus::Done,
                result: None,
            },
        })
    }
}

// #[async_trait]
// impl<T, E, O, R> Handler<T> for T
// where
//     E: TaskExecution<O, R>,
// {
//     async fn handle(&mut self, task: T, ctx: &ComponentContext<T>) {}
// }

impl Debug for WriteTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WriteTask")
    }
}

#[derive(Clone)]
pub(crate) struct FlushTask {
    pub(crate) task: Task,
    pub(crate) segment_manager: SegmentManager,
}

impl FlushTask {
    pub(crate) fn new(task: Task, segment_manager: SegmentManager) -> Self {
        FlushTask {
            task,
            segment_manager,
        }
    }
}

pub(crate) struct FlushTaskResponse {
    pub(crate) response: TaskResponse<()>,
}

#[async_trait]
impl TaskExecution<(), FlushTaskResponse> for FlushTask {
    async fn run(&self) {
        // self.segment_manager.flush().await;
    }

    async fn done(&self, _output: ()) -> FlushTaskResponse {
        FlushTaskResponse {
            response: TaskResponse {
                task_id: Uuid::new_v4(),
                job_id: Uuid::new_v4(),
                status: TaskStatus::Done,
                result: (),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PullTask {
    scheduler: Arc<Scheduler>,
}

impl PullTask {
    pub(crate) fn new(scheduler: Arc<Scheduler>) -> Self {
        PullTask { scheduler }
    }
}

#[async_trait]
impl TaskExecution<Option<Task>, ()> for PullTask {
    async fn run(&self) -> Option<Task> {
        let task = self.scheduler.take_task();
        task
    }

    async fn done(&self, _output: Option<Task>) {}
}

#[async_trait]
impl Component for PullTask {
    type Output = Result<Task, JobError>;

    fn queue_size(&self) -> usize {
        100
    }

    fn runtime() -> ComponentRuntime {
        ComponentRuntime::MultiThread
    }

    async fn run_task(&mut self, ctx: &ComponentContext<Self>) -> Result<Task, JobError> {
        match self.run().await {
            Some(task) => {
                self.done(Some(task.clone())).await;
                Ok(task)
            }
            None => Err(JobError::WorkerGone),
        }
    }
}
