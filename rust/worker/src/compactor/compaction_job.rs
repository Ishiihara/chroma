use crate::compactor::compaction_task::WriteTask;
use crate::compactor::types::Task;
use crate::segment::SegmentManager;
use crate::system::dedicated_executor::Job;
use std::collections::HashMap;
use uuid::Uuid;

pub(crate) struct CompactionJob {
    pub(crate) id: Uuid,
    pub(crate) task: Task,
    pub(crate) write_tasks: HashMap<Uuid, WriteTask>,
    pub(crate) task_handles: Vec<Job<()>>,
    pub(crate) status: CompactionStatus,
    pub(crate) num_write_tasks: i32,
    pub(crate) finished_write_tasks: i32,
    pub(crate) segment_manager: SegmentManager,
}
impl CompactionJob {
    pub(crate) fn new(task: Task, num_write_tasks: i32, segment_manager: SegmentManager) -> Self {
        CompactionJob {
            id: Uuid::new_v4(),
            task,
            write_tasks: HashMap::new(),
            task_handles: Vec::new(),
            status: CompactionStatus::Writing,
            num_write_tasks,
            finished_write_tasks: 0,
            segment_manager,
        }
    }

    pub(crate) fn get_job_id(&self) -> Uuid {
        self.id
    }

    pub(crate) fn add_write_task(&mut self, write_task: WriteTask) {
        self.write_tasks.insert(write_task.job_id, write_task);
    }
}

pub(crate) enum CompactionStatus {
    Scanning,
    Deduping,
    Writing,
    Flushing,
    Done,
    Failed,
}
