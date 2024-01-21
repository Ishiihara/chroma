use std::fmt::Debug;

use super::segment_manager::SegmentManager;

pub(crate) struct Flusher {
    segment_manager: SegmentManager,
}

impl Debug for Flusher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Flusher").finish()
    }
}

impl Flusher {
    pub(crate) fn new(segment_manager: SegmentManager) -> Self {
        Flusher {
            segment_manager: segment_manager,
        }
    }

    pub(crate) fn flush(&mut self, collection_id: String) {
        self.segment_manager.flush(collection_id);
    }
}
