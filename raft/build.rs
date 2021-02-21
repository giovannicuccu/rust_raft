//TODO: capire il perchè di questo

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/raft_rpc.proto")?;
    Ok(())
}