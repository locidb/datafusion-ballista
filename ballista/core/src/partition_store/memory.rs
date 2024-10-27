use datafusion::{
    arrow::record_batch::RecordBatch,
    physical_plan::{memory::MemoryStream, SendableRecordBatchStream},
};
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
    batch_store: Arc<Mutex<HashMap<String, Batches>>>,
}

impl InMemoryPartitionStore {
    pub fn new() -> Self {
        println!("Creating InMemoryPartitionStore");
        Self {
            stream_store: Arc::new(Mutex::new(HashMap::new())),
            batch_store: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl PartitionStore for InMemoryPartitionStore {
    fn store_batch(&self, path: &str, batch: RecordBatch) -> Result<(), BallistaError> {
        println!("InMemoryPartitionStore.store_batch: {}", path);
        let schema = batch.schema();

        // Get or create entity in batch store, insert batch
        let mut batch_store = self.batch_store.lock().unwrap();
        let batches = batch_store
            .entry(path.to_string())
            .or_insert_with(|| Batches {
                batches: Vec::new(),
                schema: schema.clone(),
            });

        batches.batches.push(batch);

        Ok(())
    }

    fn finalize_batches(&self, path: &str) -> Result<(), BallistaError> {
        println!("InMemoryPartitionStore.finalize_batches: {}", path);
        let mut batch_store = self.batch_store.lock().unwrap();
        let batches = batch_store.remove(path).ok_or_else(|| {
            BallistaError::General(format!(
                "Partition not found in in-memory store: {}",
                path
            ))
        })?;

        let schema = batches.schema.clone();
        MemoryStream::try_new(batches.batches, schema, None)
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
        println!("InMemoryPartitionStore.store_partition: {}", path);
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
        println!("InMemoryPartitionStore.fetch_partition: {}", path);
        let stream = self.stream_store.lock().unwrap().remove(path);

        match stream {
            Some(stream) => Ok(stream),
            None => Err(BallistaError::General(format!(
                "Partition not found in in-memory store: {}",
                path
            ))),
        }
    }

    fn delete_partition(&self, path: &str) -> Result<(), BallistaError> {
        println!("InMemoryPartitionStore.delete_partition: {}", path);
        self.stream_store.lock().unwrap().remove(path);
        Ok(())
    }

    fn take_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError> {
        println!("InMemoryPartitionStore.take_partition: {}", path);
        let stream = self.fetch_partition(path)?;
        Ok(stream)
    }
}
