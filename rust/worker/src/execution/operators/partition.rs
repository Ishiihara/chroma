use crate::errors::{ChromaError, ErrorCodes};
use crate::execution::data::data_chunk::DataChunk;
use crate::execution::operator::Operator;
use async_trait::async_trait;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug)]
pub struct PartitionOperator {}

/// The input to the partition operator.
/// # Parameters
/// * `records` - The records to partition.
#[derive(Debug)]
pub struct PartitionInput {
    pub(crate) records: DataChunk,
    pub(crate) max_partition_size: usize,
}

impl PartitionInput {
    /// Create a new partition input.
    /// # Parameters
    /// * `records` - The records to partition.
    /// * `max_partition_size` - The maximum size of a partition. Since we are trying to
    /// partition the records by id, which can casue the partition size to be larger than this
    /// value.
    pub fn new(records: DataChunk, max_partition_size: usize) -> Self {
        PartitionInput {
            records,
            max_partition_size,
        }
    }
}

/// The output of the partition operator.
/// # Parameters
/// * `records` - The partitioned records.
#[derive(Debug)]
pub struct PartitionOutput {
    pub(crate) records: Vec<DataChunk>,
}

#[derive(Debug, Error)]
pub enum PartitionError {
    #[error("Failed to partition records.")]
    PartitionError,
}

impl ChromaError for PartitionError {
    fn code(&self) -> ErrorCodes {
        match self {
            PartitionError::PartitionError => ErrorCodes::Internal,
        }
    }
}

pub type PartitionResult = Result<PartitionOutput, PartitionError>;

impl PartitionOperator {
    pub fn new() -> Box<Self> {
        Box::new(PartitionOperator {})
    }

    pub fn partition(&self, records: &DataChunk, partition_size: usize) -> Vec<DataChunk> {
        let mut map = HashMap::new();
        for data in records.iter() {
            let record = data.0;
            let index = data.1;
            let key = record.id.clone();
            map.entry(key).or_insert_with(Vec::new).push(index);
        }
        let mut result = Vec::new();
        // Create a new DataChunk for each parition of records with partition_size without
        // data copying.
        let mut current_batch_size = 0;
        let mut new_partition = true;
        let mut visibility = vec![false; records.total_len()];
        for (_, v) in map.iter() {
            // create DataChunk with partition_size by masking the visibility of the records
            // in the partition.
            if new_partition {
                visibility = vec![false; records.total_len()];
                new_partition = false;
            }
            for i in v.iter() {
                visibility[*i] = true;
            }
            current_batch_size += v.len();
            if current_batch_size >= partition_size {
                let mut new_data_chunk = records.clone();
                new_data_chunk.set_visibility(visibility.clone());
                result.push(new_data_chunk);
                new_partition = true;
                current_batch_size = 0;
            }
        }
        // handle the case that the last group is smaller than the group_size.
        if !new_partition {
            let mut new_data_chunk = records.clone();
            new_data_chunk.set_visibility(visibility.clone());
            result.push(new_data_chunk);
        }
        result
    }

    fn determine_partition_size(&self, num_records: usize, threshold: usize) -> usize {
        if num_records < threshold {
            return num_records;
        } else {
            return threshold;
        }
    }
}

#[async_trait]
impl Operator<PartitionInput, PartitionOutput> for PartitionOperator {
    type Error = PartitionError;

    async fn run(&self, input: &PartitionInput) -> PartitionResult {
        let records = &input.records;
        let partition_size = self.determine_partition_size(records.len(), input.max_partition_size);
        let deduped_records = self.partition(records, partition_size);
        return Ok(PartitionOutput {
            records: deduped_records,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EmbeddingRecord;
    use crate::types::Operation;
    use num_bigint::BigInt;
    use std::str::FromStr;
    use std::sync::Arc;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_partition_operator() {
        let collection_uuid_1 = Uuid::from_str("00000000-0000-0000-0000-000000000001").unwrap();
        let collection_uuid_2 = Uuid::from_str("00000000-0000-0000-0000-000000000002").unwrap();
        let data = vec![
            EmbeddingRecord {
                id: "embedding_id_1".to_string(),
                seq_id: BigInt::from(1),
                embedding: None,
                encoding: None,
                metadata: None,
                operation: Operation::Add,
                collection_id: collection_uuid_1,
            },
            EmbeddingRecord {
                id: "embedding_id_2".to_string(),
                seq_id: BigInt::from(2),
                embedding: None,
                encoding: None,
                metadata: None,
                operation: Operation::Add,
                collection_id: collection_uuid_1,
            },
            EmbeddingRecord {
                id: "embedding_id_1".to_string(),
                seq_id: BigInt::from(3),
                embedding: None,
                encoding: None,
                metadata: None,
                operation: Operation::Add,
                collection_id: collection_uuid_2,
            },
        ];
        let data: Arc<[EmbeddingRecord]> = data.into();

        // Test group size is larger than the number of records
        let chunk = DataChunk::new(data.clone());
        let operator = PartitionOperator::new();
        let input = PartitionInput::new(chunk, 4);
        let result = operator.run(&input).await.unwrap();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].len(), 3);

        // Test group size is the same as the number of records
        let chunk = DataChunk::new(data.clone());
        let operator = PartitionOperator::new();
        let input = PartitionInput::new(chunk, 3);
        let result = operator.run(&input).await.unwrap();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].len(), 3);

        // Test group size is smaller than the number of records
        let chunk = DataChunk::new(data.clone());
        let operator = PartitionOperator::new();
        let input = PartitionInput::new(chunk, 2);
        let mut result = operator.run(&input).await.unwrap();

        // The result can be 1 or 2 groups depending on the order of the records.
        assert!(result.records.len() == 2 || result.records.len() == 1);
        if result.records.len() == 2 {
            result.records.sort_by(|a, b| a.len().cmp(&b.len()));
            assert_eq!(result.records[0].len(), 1);
            assert_eq!(result.records[1].len(), 2);
        } else {
            assert_eq!(result.records[0].len(), 3);
        }

        // Test group size is smaller than the number of records
        let chunk = DataChunk::new(data.clone());
        let operator = PartitionOperator::new();
        let input = PartitionInput::new(chunk, 1);
        let mut result = operator.run(&input).await.unwrap();
        assert_eq!(result.records.len(), 2);
        result.records.sort_by(|a, b| a.len().cmp(&b.len()));
        assert_eq!(result.records[0].len(), 1);
        assert_eq!(result.records[1].len(), 2);
    }
}
