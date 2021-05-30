use raft::ServerConfig;
use crate::grpc_channel::RaftRPCServerImpl;
use std::{thread, time};

mod grpc_channel;

fn main() {
    println!("Hello, world!");
    let server_config_1=ServerConfig::new(1,65,100, 9090,vec![String::from("http://localhost:9091"),String::from("http://localhost:9092")],String::from("c:\\temp\\1\\wal"),String::from("c:\\temp\\1\\sm"));
    let server_config_2=ServerConfig::new(2,65,100, 9091,vec![String::from("http://localhost:9090"),String::from("http://localhost:9092")],String::from("c:\\temp\\2\\wal"),String::from("c:\\temp\\2\\sm"));
    let server_config_3=ServerConfig::new(3,65,100, 9092,vec![String::from("http://localhost:9090"),String::from("http://localhost:9091")],String::from("c:\\temp\\3\\wal"),String::from("c:\\temp\\3\\sm"));

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
    let sleep_time = time::Duration::from_millis(3000);
    thread::sleep(sleep_time);
/*    server1.stop();
    server2.stop();
    server3.stop();*/
    /*for child in children {
        // Wait for the thread to finish. Returns a result.
        let _ = child.join();
    }*/
}
