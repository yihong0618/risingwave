use std::fmt::Write;
use std::time::Duration;

use kafka::client::KafkaClient;
use kafka::producer::{Producer, Record, RequiredAcks};
use pcap::Capture;
use pnet::packet::ethernet::EthernetPacket;
use pnet::packet::ipv4::Ipv4Packet;
use pnet::packet::tcp::{TcpFlags, TcpPacket};
use pnet::packet::Packet;
use serde::Serialize;

#[derive(Serialize)]
struct MyTcpPacket {
    source_ip: String,
    destination_ip: String,
    source_port: u16,
    destination_port: u16,
    sequence_number: u32,
    acknowledgement_number: u32,
    window_size: u16,
    is_fin: bool,
    ts: u64,
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
        let ether = if let Some(ether) = EthernetPacket::new(&packet) {
            ether
        } else {
            continue;
        };
        let ipv4 = if let Some(ipv4) = Ipv4Packet::new(ether.payload()) {
            if ipv4.get_version() != 4 {
                continue;
            }
            ipv4
        } else {
            continue;
        };
        if ipv4.get_next_level_protocol() != pnet::packet::ip::IpNextHeaderProtocols::Tcp {
            continue;
        }
        let tcp = TcpPacket::new(ipv4.payload()).unwrap();
        let checksum =
            pnet::packet::tcp::ipv4_checksum(&tcp, &ipv4.get_source(), &ipv4.get_destination());
        if checksum != tcp.get_checksum() {
            println!("checksum not match");
        } else {
            let packet = MyTcpPacket {
                source_ip: ipv4.get_source().to_string(),
                destination_ip: ipv4.get_destination().to_string(),
                source_port: tcp.get_source(),
                destination_port: tcp.get_destination(),
                sequence_number: tcp.get_sequence(),
                acknowledgement_number: tcp.get_acknowledgement(),
                window_size: tcp.get_window(),
                is_fin: tcp.get_flags() & TcpFlags::FIN != 0,
                ts: packet.header.ts.tv_sec as u64,
            };
            let buf = serde_json::to_vec(&packet).unwrap();
            producer.send(&Record::from_value("tcp", buf)).unwrap();
        }
    }
    Ok(())
}
