use std::collections::HashMap;
use std::fs::remove_file;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use std::sync::Mutex;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::ipc::reader::StreamReader;
use futures::StreamExt;
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
    batch_writers: Arc<Mutex<HashMap<String, StreamWriter<File>>>>,
}

impl DiskBasedPartitionStore {
    pub fn new() -> Self {
        Self {
            batch_writers: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl PartitionStore for DiskBasedPartitionStore {
    fn store_batch(&self, path: &str, batch: RecordBatch) -> Result<(), BallistaError> {
        println!(
            "DiskBasedPartitionStore.store_batch: {}, num rows: {}",
            path,
            batch.num_rows()
        );
        // get or create a new writer
        let mut batch_writers = self.batch_writers.lock().unwrap();
        if !batch_writers.contains_key(path) {
            let file = File::create(path).map_err(|e| {
                error!("Failed to create partition file at {}: {:?}", path, e);
                BallistaError::IoError(e)
            })?;
            let options = IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::LZ4_FRAME))?;
            let writer = StreamWriter::try_new_with_options(
                file,
                batch.schema().as_ref(),
                options,
            )?;
            batch_writers.insert(path.to_string(), writer);
        } else {
            let writer = batch_writers.get_mut(path).unwrap();
            writer.write(&batch)?;
        }

        Ok(())
    }

    fn finalize_batches(&self, path: &str) -> Result<(), BallistaError> {
        println!("DiskBasedPartitionStore.finalize_batches: {}", path);
        let mut batch_writers = self.batch_writers.lock().unwrap();
        if let Some(mut writer) = batch_writers.remove(path) {
            writer.finish()?;
        }
        Ok(())
    }

    async fn store_partition(
        &self,
        path: &str,
        mut stream: SendableRecordBatchStream,
    ) -> Result<Option<PartitionStats>, BallistaError> {
        println!("DiskBasedPartitionStore.store_partition: {}", path);
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
        println!(
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
        println!("DiskBasedPartitionStore.fetch_partition: {}", path);
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
        println!("DiskBasedPartitionStore.delete_partition: {}", path);
        remove_file(path).map_err(|e| {
            error!("Failed to delete partition file at {}: {:?}", path, e);
            BallistaError::IoError(e)
        })
    }

    fn take_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError> {
        println!("DiskBasedPartitionStore.take_partition: {}", path);
        let stream = self.fetch_partition(path)?;
        self.delete_partition(path)?;
        Ok(stream)
    }
}
