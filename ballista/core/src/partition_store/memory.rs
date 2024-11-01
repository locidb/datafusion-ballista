use dashmap::DashMap;
use datafusion::{
    arrow::record_batch::RecordBatch,
    physical_plan::{memory::MemoryStream, SendableRecordBatchStream},
};
use log::debug;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{error::BallistaError, serde::scheduler::PartitionStats};

use super::PartitionStore;

struct Batches {
    batches: Vec<RecordBatch>,
    schema: datafusion::arrow::datatypes::SchemaRef,
}

pub struct InMemoryPartitionStore {
    stream_store: Arc<Mutex<HashMap<String, SendableRecordBatchStream>>>,
    batch_store: DashMap<String, Batches>,
}

impl InMemoryPartitionStore {
    pub fn new() -> Self {
        debug!("Creating InMemoryPartitionStore");
        Self {
            stream_store: Arc::new(Mutex::new(HashMap::new())),
            batch_store: DashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl PartitionStore for InMemoryPartitionStore {
    fn store_batch(&self, path: &str, batch: RecordBatch) -> Result<(), BallistaError> {
        debug!("InMemoryPartitionStore.store_batch: {}", path);
        let schema = batch.schema();

        // Get or create entity in batch store, insert batch
        let mut batches =
            self.batch_store
                .entry(path.to_string())
                .or_insert_with(|| Batches {
                    batches: Vec::new(),
                    schema: schema.clone(),
                });

        batches.batches.push(batch);

        Ok(())
    }

    fn finalize_batches(&self, path: &str) -> Result<(), BallistaError> {
        debug!("InMemoryPartitionStore.finalize_batches: {}", path);
        let batches = self.batch_store.remove(path).ok_or_else(|| {
            BallistaError::General(format!(
                "Partition not found in in-memory store: {}",
                path
            ))
        })?;

        let schema = batches.1.schema.clone();
        MemoryStream::try_new(batches.1.batches, schema, None)
            .map(|stream| {
                self.stream_store
                    .lock()
                    .unwrap()
                    .insert(path.to_string(), Box::pin(stream));
            })
            .map_err(|e| {
                BallistaError::General(format!("Error creating stream: {:?}", e))
            })?;

        Ok(())
    }

    async fn store_partition(
        &self,
        path: &str,
        stream: SendableRecordBatchStream,
    ) -> Result<Option<PartitionStats>, BallistaError> {
        debug!("InMemoryPartitionStore.store_partition: {}", path);
        // Store the state
        self.stream_store
            .lock()
            .unwrap()
            .insert(path.to_string(), stream);

        Ok(None)
    }

    // this can only be called once since the caller consumes the stream
    fn fetch_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError> {
        debug!("InMemoryPartitionStore.fetch_partition: {}", path);
        match self.stream_store.lock().unwrap().remove(path) {
            Some(stream) => Ok(stream),
            None => Err(BallistaError::General(format!(
                "Partition not found in in-memory store: {}",
                path
            ))),
        }
    }

    fn delete_partition(&self, path: &str) -> Result<(), BallistaError> {
        debug!("InMemoryPartitionStore.delete_partition: {}", path);
        self.stream_store.lock().unwrap().remove(path);
        Ok(())
    }

    fn take_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError> {
        debug!("InMemoryPartitionStore.take_partition: {}", path);
        let stream = self.fetch_partition(path)?;
        Ok(stream)
    }
}
