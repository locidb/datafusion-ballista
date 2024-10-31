use dashmap::DashMap;
use std::fs::remove_file;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::ipc::reader::StreamReader;
use futures::StreamExt;
use log::debug;
use log::error;

use crate::{error::BallistaError, serde::scheduler::PartitionStats};

use super::LocalShuffleStream;
use super::PartitionStore;
use datafusion::{
    arrow::ipc::{
        writer::{IpcWriteOptions, StreamWriter},
        CompressionType,
    },
    execution::SendableRecordBatchStream,
};

pub struct DiskBasedPartitionStore {
    batch_writers: Arc<DashMap<String, StreamWriter<File>>>,
}

impl DiskBasedPartitionStore {
    pub fn new() -> Self {
        debug!("Creating DiskBasedPartitionStore");
        Self {
            batch_writers: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl PartitionStore for DiskBasedPartitionStore {
    fn store_batch(&self, path: &str, batch: RecordBatch) -> Result<(), BallistaError> {
        debug!(
            "DiskBasedPartitionStore.store_batch: {}, num rows: {}",
            path,
            batch.num_rows()
        );

        let mut writer =
            self.batch_writers
                .entry(path.to_string())
                .or_insert_with(|| {
                    let file =
                        File::create(path).expect("Failed to create partition file");
                    let options = IpcWriteOptions::default()
                        .try_with_compression(Some(CompressionType::LZ4_FRAME))
                        .expect("Failed to set compression type");
                    StreamWriter::try_new_with_options(
                        file,
                        batch.schema().as_ref(),
                        options,
                    )
                    .expect("Failed to create StreamWriter")
                });

        writer.write(&batch)?;

        Ok(())
    }

    fn finalize_batches(&self, path: &str) -> Result<(), BallistaError> {
        debug!("DiskBasedPartitionStore.finalize_batches: {}", path);
        // Remove the writer from the map and finalize it if it exists
        if let Some(mut writer_entry) = self.batch_writers.remove(path) {
            debug!(
                "DiskBasedPartitionStore.finalize_batches, finishing writer {}",
                path
            );
            writer_entry.1.finish()?;
        }

        Ok(())
    }

    async fn store_partition(
        &self,
        path: &str,
        mut stream: SendableRecordBatchStream,
    ) -> Result<Option<PartitionStats>, BallistaError> {
        debug!("DiskBasedPartitionStore.store_partition: {}", path);
        let file = File::create(path).map_err(|e| {
            error!("Failed to create partition file at {}: {:?}", path, e);
            BallistaError::IoError(e)
        })?;

        let mut num_rows = 0;
        let mut num_batches = 0;
        let mut num_bytes = 0;

        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))?;

        let mut writer =
            StreamWriter::try_new_with_options(file, stream.schema().as_ref(), options)?;

        while let Some(result) = stream.next().await {
            let batch = result?;
            let batch_size_bytes = batch.get_array_memory_size();

            num_batches += 1;
            num_rows += batch.num_rows();
            num_bytes += batch_size_bytes;

            writer.write(&batch)?;
        }
        debug!(
            "DiskBasedPartitionStore.store_partition finished: {}, num_rows: {}",
            path, num_rows
        );
        writer.finish()?;

        Ok(Some(PartitionStats::new(
            Some(num_rows as u64),
            Some(num_batches),
            Some(num_bytes as u64),
        )))
    }

    fn fetch_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError> {
        debug!("DiskBasedPartitionStore.fetch_partition: {}", path);
        let file = File::open(path).map_err(|e| {
            BallistaError::General(format!(
                "Failed to open partition file at {path}: {e:?}"
            ))
        })?;
        let file = BufReader::new(file);
        let reader = StreamReader::try_new(file, None).map_err(|e| {
            BallistaError::General(format!(
                "Failed to new arrow FileReader at {path}: {e:?}"
            ))
        })?;

        Ok(Box::pin(LocalShuffleStream::new(reader)))
    }

    fn delete_partition(&self, path: &str) -> Result<(), BallistaError> {
        debug!("DiskBasedPartitionStore.delete_partition: {}", path);
        remove_file(path).map_err(|e| {
            error!("Failed to delete partition file at {}: {:?}", path, e);
            BallistaError::IoError(e)
        })
    }

    fn take_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError> {
        debug!("DiskBasedPartitionStore.take_partition: {}", path);
        let stream = self.fetch_partition(path)?;
        self.delete_partition(path)?;
        Ok(stream)
    }
}
