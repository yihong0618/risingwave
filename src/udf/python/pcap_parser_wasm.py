from typing import Iterator
from risingwave.udf import udf, udtf, UdfServer
import random
from wasmer import Store, Memory, MemoryType, Buffer, engine,Module,Instance
from wasmer_compiler_cranelift import Compiler
from array import array
import json
#  python3.10 -m pip install wasmer wasmer_compiler_cranelift risingwave
with open('../../../target/wasm32-unknown-unknown/release/pcap_parser.wasm', 'rb') as file:
    wasm_bytes = file.read()


def parse_packet(packet):
    store = Store(engine.Universal(Compiler))
    module = Module(store, wasm_bytes)
    instance = Instance(module)
    memory = instance.exports.memory
    uint8 = memory.uint8_view()
    uint8[0:len(packet)] = packet
    start = instance.exports.parse_ether_packet(0,len(packet))
    end = start
    while uint8[end] != 0:
        end+=1
    if start == end:
        return {"source_ip": None,
        "source_port":None,
        "destination_ip":None,
        "destination_port":None,
        "sequence_number":None,
        "acknowledgement_number":None,
        "window_size":None,
        "is_fin":None,
        }
    data = json.loads(array('b',uint8[start:end]).tobytes().decode())
    return data
@udf(input_types=['BINARY'], result_type='VARCHAR')
def extract_source_ip(packet):
    return parse_packet(packet)["source_ip"]

@udf(input_types=['BINARY'], result_type='BIGINT')
def extract_source_port(packet):
    return parse_packet(packet)["source_port"]

@udf(input_types=['BINARY'], result_type='VARCHAR')
def extract_dst_ip(packet):
    return parse_packet(packet)["destination_ip"]

@udf(input_types=['BINARY'], result_type='BIGINT')
def extract_dst_port(packet):
    return parse_packet(packet)["destination_port"]

@udf(input_types=['BINARY'], result_type='BIGINT')
def extract_sequence_number(packet):
    return parse_packet(packet)["sequence_number"]


@udf(input_types=['BINARY'], result_type='BIGINT')
def extract_acknowledgement_number(packet):
    return parse_packet(packet)["acknowledgement_number"]

@udf(input_types=['BINARY'], result_type='BIGINT')
def extract_window_size(packet):
    return parse_packet(packet)["window_size"]

@udf(input_types=['BINARY'], result_type='BOOLEAN')
def extract_is_fin(packet):
    return parse_packet(packet)["is_fin"]




if __name__ == '__main__':
    server = UdfServer(location="0.0.0.0:8815")
    server.add_function(extract_source_ip)
    server.add_function(extract_source_port)
    server.add_function(extract_dst_ip)
    server.add_function(extract_dst_port)
    server.add_function(extract_sequence_number)
    server.add_function(extract_acknowledgement_number)
    server.add_function(extract_window_size)
    server.add_function(extract_is_fin)
    server.serve()
