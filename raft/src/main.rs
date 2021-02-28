use raft::ServerConfig;
use crate::grpc_channel::RaftRPCServerImpl;
use std::thread;

mod grpc_channel;

fn main() {
    println!("Hello, world!");
    let server_config_1=ServerConfig::new(1,35,80, 9090,vec![String::from("localhost:9091"),String::from("localhost:9092")]);
    let server_config_2=ServerConfig::new(2,35,80, 9091,vec![String::from("localhost:9090"),String::from("localhost:9092")]);
    let server_config_3=ServerConfig::new(1,35,80, 9092,vec![String::from("localhost:9090"),String::from("localhost:9091")]);

    let mut children = vec![];
/*
Se non raccolgo gli handle il programma finisce subito
 */
    children.push(thread::spawn(|| {
        RaftRPCServerImpl::start(server_config_1);
    }));
    children.push(thread::spawn(|| {
        RaftRPCServerImpl::start(server_config_2);
    }));
    children.push(thread::spawn(|| {
        RaftRPCServerImpl::start(server_config_3);
    }));

    for child in children {
        // Wait for the thread to finish. Returns a result.
        let _ = child.join();
    }
}
