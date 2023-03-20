use std::fmt::Write;
use std::time::Duration;

use kafka::client::KafkaClient;
use kafka::producer::{Producer, Record, RequiredAcks};
use pcap::Capture;
use serde::Serialize;

#[derive(Serialize)]
struct Packet {
    data: Vec<u8>,
}

fn main() -> anyhow::Result<()> {
    let mut cap = Capture::from_file("c.pcap")?;

    // ./kafka-topics --topic tcp --create  --bootstrap-server localhost:9092
    let mut producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()
        .unwrap();
    while let Ok(packet) = cap.next_packet() {
        let buf = serde_json::to_vec(&Packet {
            data: packet.to_vec(),
        })
        .unwrap();
        producer.send(&Record::from_value("ether", buf)).unwrap();
    }
    Ok(())
}
