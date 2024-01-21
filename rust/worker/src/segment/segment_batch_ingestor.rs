use std::fmt::Debug;

use super::segment_manager::SegmentManager;
use crate::system::{Component, ComponentContext, ComponentRuntime, Handler};
use crate::types::EmbeddingRecord;
use async_trait::async_trait;

pub(crate) struct SegmentBatchIngestor {
    segment_manager: SegmentManager,
}

impl Component for SegmentBatchIngestor {
    fn queue_size(&self) -> usize {
        1000
    }
    fn runtime() -> ComponentRuntime {
        ComponentRuntime::Dedicated
    }
}

impl Debug for SegmentBatchIngestor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentBatchIngestor").finish()
    }
}

impl SegmentBatchIngestor {
    pub(crate) fn new(segment_manager: SegmentManager) -> Self {
        SegmentBatchIngestor {
            segment_manager: segment_manager,
        }
    }
}

#[async_trait]
impl Handler<Vec<Box<EmbeddingRecord>>> for SegmentBatchIngestor {
    async fn handle(&mut self, message: Vec<Box<EmbeddingRecord>>, ctx: &ComponentContext<Self>) {
        for record in message.iter() {
            self.segment_manager.write_record(*record).await;
        }
    }
}
