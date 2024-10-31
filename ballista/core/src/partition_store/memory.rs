use datafusion::{
    arrow::record_batch::RecordBatch,
    physical_plan::{memory::MemoryStream, SendableRecordBatchStream},
};
use log::debug;
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::oneshot;

use super::PartitionStore;
use crate::{error::BallistaError, serde::scheduler::PartitionStats};

struct Batches {
    batches: Vec<RecordBatch>,
    schema: datafusion::arrow::datatypes::SchemaRef,
}

type StreamReceiver = oneshot::Receiver<SendableRecordBatchStream>;

pub struct InMemoryPartitionStore {
    // Store receivers instead of senders
    stream_store: Arc<RwLock<HashMap<String, StreamReceiver>>>,
    batch_store: Arc<RwLock<HashMap<String, Batches>>>,
}

impl InMemoryPartitionStore {
    pub fn new() -> Self {
        debug!("Creating InMemoryPartitionStore");
        Self {
            stream_store: Arc::new(RwLock::new(HashMap::new())),
            batch_store: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl PartitionStore for InMemoryPartitionStore {
    fn store_batch(&self, path: &str, batch: RecordBatch) -> Result<(), BallistaError> {
        debug!("InMemoryPartitionStore.store_batch: {}", path);
        let schema = batch.schema();

        let mut batch_store = self.batch_store.write();
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
        debug!("InMemoryPartitionStore.finalize_batches: {}", path);

        // Remove batches from batch store
        let mut batch_store = self.batch_store.write();
        let batches = batch_store.remove(path).ok_or_else(|| {
            BallistaError::General(format!(
                "Partition not found in in-memory store: {}",
                path
            ))
        })?;

        let schema = batches.schema.clone();
        let stream =
            MemoryStream::try_new(batches.batches, schema, None).map_err(|e| {
                BallistaError::General(format!("Error creating stream: {:?}", e))
            })?;

        // Create a channel and store the receiver
        let (tx, rx) = oneshot::channel();
        self.stream_store.write().insert(path.to_string(), rx);

        // Send the stream through the channel
        if let Err(_) = tx.send(Box::pin(stream)) {
            return Err(BallistaError::General(
                "Failed to send stream through channel".to_string(),
            ));
        }

        Ok(())
    }

    async fn store_partition(
        &self,
        path: &str,
        stream: SendableRecordBatchStream,
    ) -> Result<Option<PartitionStats>, BallistaError> {
        debug!("InMemoryPartitionStore.store_partition: {}", path);

        let (tx, rx) = oneshot::channel();
        self.stream_store.write().insert(path.to_string(), rx);

        if let Err(_) = tx.send(stream) {
            return Err(BallistaError::General(
                "Failed to send stream through channel".to_string(),
            ));
        }

        Ok(None)
    }

    fn fetch_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError> {
        debug!("InMemoryPartitionStore.fetch_partition: {}", path);

        // Remove the receiver from the store
        let rx = {
            let mut store = self.stream_store.write();
            store.remove(path).ok_or_else(|| {
                BallistaError::General(format!(
                    "Partition not found in in-memory store: {}",
                    path
                ))
            })?
        };

        // Wait for the stream
        match rx.blocking_recv() {
            Ok(stream) => Ok(stream),
            Err(_) => Err(BallistaError::General(
                "Failed to receive stream from channel".to_string(),
            )),
        }
    }

    fn delete_partition(&self, path: &str) -> Result<(), BallistaError> {
        debug!("InMemoryPartitionStore.delete_partition: {}", path);
        self.stream_store.write().remove(path);
        Ok(())
    }

    fn take_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError> {
        debug!("InMemoryPartitionStore.take_partition: {}", path);
        self.fetch_partition(path)
    }
}
