/*
cerco qualcosa che sia chiaro e che leghi insieme i concetti di client e
server insieme a livello di programmazione
 */
/*
approfondire perchè server impl Fn()
 */
pub trait ServerChannel {
    fn handle_request_vote(&self, handler: impl Fn());
}

pub trait ClientChannel {
    fn send_request_vote(&self);
}
pub trait NetworkChannel<S:ServerChannel,C:ClientChannel> {
    fn server_channel(&self)->S;
    fn client_channel(&self,remote_address: &String)->C;
}