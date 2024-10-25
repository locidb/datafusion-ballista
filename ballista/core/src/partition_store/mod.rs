use std::{
    fs::File,
    io::BufReader,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use datafusion::{
    arrow::{array::RecordBatch, datatypes::SchemaRef, ipc::reader::StreamReader},
    error::Result,
    execution::{RecordBatchStream, SendableRecordBatchStream},
    prelude::SessionConfig,
};
use futures::Stream;

use crate::{error::BallistaError, serde::scheduler::PartitionStats};

pub mod disk;
pub mod hybrid;
pub mod memory;

pub fn get_partition_store(session_config: &SessionConfig) -> Arc<dyn PartitionStore> {
    session_config
        .get_extension::<PartitionStoreRef>()
        .map(|store_ref| store_ref.0.clone())
        .unwrap()
}

#[derive(Clone)]
pub struct PartitionStoreRef(pub Arc<dyn PartitionStore>);

#[async_trait::async_trait]
pub trait PartitionStore: Send + Sync + 'static {
    fn store_batch(&self, path: &str, batch: RecordBatch) -> Result<(), BallistaError>;

    fn finalize_batches(&self, path: &str) -> Result<(), BallistaError>;

    async fn store_partition(
        &self,
        path: &str,
        stream: SendableRecordBatchStream,
    ) -> Result<Option<PartitionStats>, BallistaError>;

    fn fetch_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError>;

    fn delete_partition(&self, path: &str) -> Result<(), BallistaError>;

    fn take_partition(
        &self,
        path: &str,
    ) -> Result<SendableRecordBatchStream, BallistaError>;
}

struct LocalShuffleStream {
    reader: StreamReader<BufReader<File>>,
}

impl LocalShuffleStream {
    pub fn new(reader: StreamReader<BufReader<File>>) -> Self {
        LocalShuffleStream { reader }
    }
}

impl Stream for LocalShuffleStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(batch) = self.reader.next() {
            return Poll::Ready(Some(batch.map_err(|e| e.into())));
        }
        Poll::Ready(None)
    }
}

impl RecordBatchStream for LocalShuffleStream {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}
