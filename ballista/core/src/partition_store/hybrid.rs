use futures::stream::StreamExt;
use tokio::sync::mpsc;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
};

use crate::{error::BallistaError, serde::scheduler::PartitionStats};

use super::{
    disk::DiskBasedPartitionStore, memory::InMemoryPartitionStore, PartitionStore,
};

pub struct HybridPartitionStore {
    in_memory_store: InMemoryPartitionStore,
    disk_store: Arc<DiskBasedPartitionStore>,
    writing_to_disk: Arc<Mutex<HashMap<String, bool>>>,
    memory_threshold_bytes: Option<u64>,
}

impl Default for HybridPartitionStore {
    fn default() -> Self {
        Self::new(None)
    }
}

impl HybridPartitionStore {
    pub fn new(memory_threshold_bytes: Option<u64>) -> Self {
        Self {
            in_memory_store: InMemoryPartitionStore::new(),
            disk_store: Arc::new(DiskBasedPartitionStore::new()),
            writing_to_disk: Arc::new(Mutex::new(HashMap::new())),
            memory_threshold_bytes,
        }
    }

    async fn store_partition_to_disk(
        disk_store: Arc<DiskBasedPartitionStore>,
        path: String,
        mut rx: mpsc::Receiver<RecordBatch>,
        writing_to_disk: Arc<Mutex<HashMap<String, bool>>>,
        schema: SchemaRef,
    ) {
        // Mark that we're writing to disk
        writing_to_disk.lock().unwrap().insert(path.clone(), true);

        // Create a stream adapter from the receiver
        let stream = RecordBatchStreamAdapter::new(
            schema,
            futures::stream::poll_fn(move |cx| {
                rx.poll_recv(cx)
                    .map(|opt| opt.map(|batch| Ok::<RecordBatch, DataFusionError>(batch)))
            }),
        );

        // Store to disk and ignore the result since this is background processing
        let _ = disk_store.store_partition(&path, Box::pin(stream)).await;

        // Remove the writing flag
        writing_to_disk.lock().unwrap().remove(&path);
    }
}

#[async_trait::async_trait]
impl PartitionStore for HybridPartitionStore {
    fn store_batch(&self, path: &str, batch: RecordBatch) -> Result<(), BallistaError> {
        let cloned_batch = batch.clone();
        let disk_store = self.disk_store.clone();
        let path_str = path.to_string();
        // store in disk asynchronously
        let _ = tokio::spawn(async move {
            disk_store.store_batch(path_str.as_str(), cloned_batch);
        });

        self.in_memory_store.store_batch(path, batch.clone())?;

        Ok(())
    }

    fn finalize_batches(&self, path: &str) -> Result<(), BallistaError> {
        // finalize both in memory and disk
        self.in_memory_store.finalize_batches(path)?;
        self.disk_store.finalize_batches(path)?;
        Ok(())
    }

    async fn store_partition(
        &self,
        path: &str,
        mut stream: SendableRecordBatchStream,
    ) -> Result<Option<PartitionStats>, BallistaError> {
        // Get the schema from the input stream
        let schema = stream.schema();
        let schema_for_disk = schema.clone();

        // Create channels to forward the stream data
        let (tx1, mut rx1) = mpsc::channel(2);
        let (tx2, rx2) = mpsc::channel(2);

        // Clone necessary components for the disk storage task
        let disk_store = self.disk_store.clone();
        let writing_to_disk = self.writing_to_disk.clone();
        let path_str = path.to_string();

        // Spawn disk storage task without awaiting it
        tokio::spawn(async move {
            Self::store_partition_to_disk(
                disk_store,
                path_str,
                rx2,
                writing_to_disk,
                schema_for_disk,
            )
            .await;
        });

        // Process the input stream
        while let Some(batch_result) = stream.next().await {
            match batch_result {
                Ok(batch) => {
                    // Send to both channels
                    let _ = tx1.send(batch.clone()).await;
                    let _ = tx2.send(batch).await;
                }
                Err(e) => {
                    return Err(BallistaError::Internal(format!(
                        "Error processing stream: {}",
                        e
                    )));
                }
            }
        }

        // Drop senders to signal completion
        drop(tx1);
        drop(tx2);

        // Create a stream adapter for in-memory storage
        let memory_stream = RecordBatchStreamAdapter::new(
            schema,
            futures::stream::poll_fn(move |cx| {
                rx1.poll_recv(cx)
                    .map(|opt| opt.map(|batch| Ok::<RecordBatch, DataFusionError>(batch)))
            }),
        );

        // Store in memory and return its stats immediately
        self.in_memory_store
            .store_partition(path, Box::pin(memory_stream))
            .await
    }

    fn fetch_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError> {
        // Check if it's currently being written to disk
        if self.writing_to_disk.lock().unwrap().contains_key(path) {
            return self.in_memory_store.fetch_partition(path);
        }

        // Try memory first
        match self.in_memory_store.fetch_partition(path) {
            Ok(stream) => Ok(stream),
            Err(_) => self.disk_store.fetch_partition(path),
        }
    }

    fn delete_partition(&self, path: &str) -> Result<(), BallistaError> {
        // Try to delete from both stores
        let _ = self.in_memory_store.delete_partition(path);
        let _ = self.disk_store.delete_partition(path);
        Ok(())
    }

    fn take_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError> {
        // Check if it's currently being written to disk
        if self.writing_to_disk.lock().unwrap().contains_key(path) {
            return self.in_memory_store.fetch_partition(path);
        }

        // Try memory first
        match self.in_memory_store.take_partition(path) {
            Ok(stream) => Ok(stream),
            Err(_) => self.disk_store.take_partition(path),
        }
    }
}
