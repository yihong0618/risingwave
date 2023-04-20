use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::GetObjectError;
use aws_sdk_s3::output::GetObjectOutput;
use aws_smithy_http::result::SdkError;
use futures::future::try_join_all;
use clap::Parser;

const BUCKET: &str = "rw-bench-wcy-risingwave-hummock";
const PARTITION:u16 = 256;
const PATH_PREFIX: &str = "temp";

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long, default_value_t = 1)]
    parallelism: u64,
    #[clap(short, long, default_value_t = 1)]
    repeat: u64,
    #[clap(short, long, default_value_t = 0)]
    interval_ms: u64,
}

async fn create_objects(client: &Client) {
    for i in 0..5 {
        let body = aws_sdk_s3::types::ByteStream::from_path(Path::new(&format!("temp/{}", i))).await.unwrap();
        client
            .put_object()
            .bucket(BUCKET)
            .key(format!("{PATH_PREFIX}/{i}"))
            .body(body)
            .send()
            .await
            .unwrap();
    }
}

async fn read(client: &Client, path: &str) {
    match client.get_object().bucket(BUCKET).key(path).send().await {
        Ok(_) => {
            println!("read {} OK", path)
        }
        Err(e) => {
            eprintln!("read {} FAIL. {}", path, e);
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let shared_config = aws_config::load_from_env().await;
    let client = Arc::new(Client::new(&shared_config));
    create_objects(&client).await;
    println!("{:#?}", args);
    for repeat_i in 0..args.repeat {
        let mut futures = vec![];
        for i in 0..args.parallelism {
            let path = format!("{PATH_PREFIX}/{}", i % (PARTITION as u64));
            let client_ = client.clone();
            futures.push(tokio::spawn(async move {
                read(&client_, &path).await;
            }));
        }
        try_join_all(futures).await.unwrap();
        tokio::time::sleep(Duration::from_millis(args.interval_ms)).await;
    }
}