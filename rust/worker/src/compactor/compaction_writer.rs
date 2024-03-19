use crate::compactor::compaction_task::AsyncTaskWrapper;
use crate::compactor::compaction_task::TaskResponse;
use crate::compactor::compaction_task::TaskStatus;
use crate::compactor::compaction_task::WriteTask;
use crate::compactor::compaction_task::WriteTaskResponse;
use crate::system::dedicated_executor::Job;
use crate::system::Component;
use crate::system::ComponentContext;
use crate::system::ComponentRuntime;
use crate::system::Handler;
use crate::system::HandlerWithResult;
use crate::system::Receiver;
use crate::system::ReceiverImpl;
use async_trait::async_trait;
use std::fmt::{Debug, Formatter};

pub(crate) struct CompactionWriter {
    sender: Box<dyn Receiver<WriteTaskResponse>>,
}

impl CompactionWriter {
    pub(crate) fn new(sender: Box<dyn Receiver<WriteTaskResponse>>) -> Self {
        CompactionWriter { sender }
    }
}

#[async_trait]
impl Component for CompactionWriter {
    type Output = ();

    fn queue_size(&self) -> usize {
        100
    }

    fn runtime() -> ComponentRuntime {
        ComponentRuntime::MultiThread
    }

    async fn run_task(&mut self, ctx: &ComponentContext<CompactionWriter>) -> () {}
}

impl Debug for CompactionWriter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CompactionWriter")
    }
}

// type WriteAsyncTask = Box<dyn AsyncTask<Output = (), Response = ()>>;
// #[async_trait]
// impl Handler<WriteAsyncTask> for CompactionWriter {
//     async fn handle(&mut self, task: WriteAsyncTask, ctx: &ComponentContext<CompactionWriter>) {
//         let handle = ctx.system.invoke(task);
//     }
// }

#[async_trait]
impl Handler<WriteTask> for CompactionWriter {
    async fn handle(&mut self, task: WriteTask, ctx: &ComponentContext<CompactionWriter>) {
        let sender = Box::new(ReceiverImpl::new(ctx.sender.sender.clone()));
        let async_write_task =
            AsyncTaskWrapper::<Option<Job<()>>, WriteTaskResponse>::new(sender, Box::new(task));
        let handle = ctx.system.invoke(async_write_task);
    }
}

#[async_trait]
impl Handler<WriteTaskResponse> for CompactionWriter {
    async fn handle(&mut self, task: WriteTaskResponse, ctx: &ComponentContext<CompactionWriter>) {
        println!("WriteTaskResponse: {:?}", task);
    }
}

#[async_trait]
impl HandlerWithResult<WriteTask, WriteTaskResponse> for CompactionWriter {
    async fn handle(
        &mut self,
        task: WriteTask,
        ctx: &ComponentContext<CompactionWriter>,
    ) -> WriteTaskResponse {
        let response = WriteTaskResponse {
            response: TaskResponse {
                task_id: task.task_id,
                job_id: task.job_id,
                status: TaskStatus::Done,
                result: (),
            },
        };
        response
    }
}
