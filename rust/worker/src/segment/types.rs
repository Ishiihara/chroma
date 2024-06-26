use crate::types::LogRecord;

trait SegmentWriter {
    fn begin_transaction(&self);
    fn write_records(&self, records: Vec<Box<LogRecord>>, offset_ids: Vec<u32>);
    fn commit_transaction(&self);
    fn rollback_transaction(&self);
}

trait OffsetIdAssigner: SegmentWriter {
    fn assign_offset_ids(&self, records: Vec<Box<LogRecord>>) -> Vec<u32>;
}
