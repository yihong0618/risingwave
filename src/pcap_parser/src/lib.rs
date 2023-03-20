use std::ffi::CString;
use std::fmt::Write;
use std::time::Duration;

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
}

#[no_mangle]
pub unsafe fn parse_ether_packet(buf: *const u8, len: usize) -> *mut i8 {
    let v = std::slice::from_raw_parts(buf, len);
    let r = parse(v);
    let cs = CString::new(r).unwrap();
    cs.into_raw()
}

pub fn parse(packet: &[u8]) -> String {
    let ether = if let Some(ether) = EthernetPacket::new(&packet) {
        ether
    } else {
        return String::new();
    };
    let ipv4 = if let Some(ipv4) = Ipv4Packet::new(ether.payload()) {
        if ipv4.get_version() != 4 {
            return String::new();
        }
        ipv4
    } else {
        return String::new();
    };
    if ipv4.get_next_level_protocol() != pnet::packet::ip::IpNextHeaderProtocols::Tcp {
        return String::new();
    }
    let tcp = TcpPacket::new(ipv4.payload()).unwrap();
    let checksum =
        pnet::packet::tcp::ipv4_checksum(&tcp, &ipv4.get_source(), &ipv4.get_destination());
    if checksum != tcp.get_checksum() {
        return String::new();
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
        };
        let buf = serde_json::to_string(&packet).unwrap();
        return buf;
    }
}
