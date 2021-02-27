use raft::ServerConfig;
use crate::grpc_channel::RaftRPCServerImpl;

mod grpc_channel;

fn main() {
    println!("Hello, world!");
    let server_config_1=ServerConfig::new(1,35,80, 9090,vec![String::from("localhost:9091"),String::from("localhost:9092")]);
    let server_config_2=ServerConfig::new(2,35,80, 9091,vec![String::from("localhost:9090"),String::from("localhost:9092")]);
    let server_config_3=ServerConfig::new(1,35,80, 9092,vec![String::from("localhost:9090"),String::from("localhost:9091")]);

    RaftRPCServerImpl::start(server_config_1) ;
    RaftRPCServerImpl::start(server_config_2) ;
    RaftRPCServerImpl::start(server_config_3) ;
}
