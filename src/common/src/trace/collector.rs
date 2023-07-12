use std::env;
use std::sync::LazyLock;
use std::time::{Duration, SystemTime};

use base64::Engine;

static GLOBAL_TRACE_COLLECTOR: LazyLock<TraceCollector> = LazyLock::new(TraceCollector::new);
const COLLECTOR_BUFFER_SIZE: usize = 1024;
const WRITER_BUFFER_SIZE: usize = 128 * 1024 * 1024;
const WRITER_FLUSH_INTERVAL_SEC: u64 = 60;
const MAX_TRACE_COUNT: usize = 100000 * 60 * 30;

static UNIX_RISINGWAVE_DATE_SEC: u64 = 1_617_235_200;

/// [`UNIX_RISINGWAVE_DATE_EPOCH`] represents the risingwave date of the UNIX epoch:
/// 2021-04-01T00:00:00Z.
static UNIX_RISINGWAVE_DATE_EPOCH: LazyLock<SystemTime> =
    LazyLock::new(|| SystemTime::UNIX_EPOCH + Duration::from_secs(UNIX_RISINGWAVE_DATE_SEC));

pub enum CollectorMessage {
    TraceData(TraceData),
    Flush,
}

pub struct TraceData {
    table_id: String,
    data: Vec<u8>,
    timestamp: u64,
}

impl TraceData {
    pub fn new(table_id: String, data: Vec<u8>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(*UNIX_RISINGWAVE_DATE_EPOCH)
            .unwrap()
            .as_secs();
        Self {
            table_id,
            data,
            timestamp,
        }
    }
}

pub struct TraceSender {
    tx: tokio::sync::mpsc::Sender<CollectorMessage>,
}

pub struct TraceCollector {
    tx: tokio::sync::mpsc::Sender<CollectorMessage>,
}

impl TraceSender {
    pub async fn send(&self, d: TraceData) -> bool {
        self.tx.send(CollectorMessage::TraceData(d)).await.is_ok()
    }
}

pub fn new_trace_sender() -> TraceSender {
    GLOBAL_TRACE_COLLECTOR.trace_sender()
}

impl TraceCollector {
    fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(COLLECTOR_BUFFER_SIZE);
        let path = env::var("RW_TRACE_FILE_PATH").unwrap_or_else(|_| "./trace.csv".to_string());
        let flush_tx = tx.clone();
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        let runtime = builder.enable_all().worker_threads(2).build().unwrap();
        runtime.spawn(async move {
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(WRITER_FLUSH_INTERVAL_SEC)).await;
                    let _ = flush_tx.send(CollectorMessage::Flush).await;
                }
            });
            tokio::task::spawn_blocking(move || {
                start_collect(rx, &path);
            });
        });
        Box::leak(Box::new(runtime));
        Self { tx }
    }

    pub fn trace_sender(&self) -> TraceSender {
        TraceSender {
            tx: self.tx.clone(),
        }
    }
}

fn start_collect(mut rx: tokio::sync::mpsc::Receiver<CollectorMessage>, path: &str) {
    let mut writer = csv::WriterBuilder::new()
        .buffer_capacity(WRITER_BUFFER_SIZE)
        .from_path(path)
        .unwrap();
    let mut trace_count = 0;

    while let Some(m) = rx.blocking_recv() {
        match m {
            CollectorMessage::TraceData(trace) => {
                if trace_count + 1 > MAX_TRACE_COUNT {
                    break;
                }
                let encoded = base64::engine::general_purpose::STANDARD_NO_PAD.encode(trace.data);
                writer
                    .write_record(&[trace.table_id, encoded, trace.timestamp.to_string()])
                    .unwrap();
                trace_count += 1;
            }
            CollectorMessage::Flush => {
                writer.flush().unwrap();
            }
        }
    }
    writer.flush().unwrap();
    tracing::info!(trace_count, "trace collect exits");
}
