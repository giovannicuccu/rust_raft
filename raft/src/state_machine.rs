use sled::{Db, IVec, Error};
use crate::common::{StateMachineCommand, CommandType};

struct StateMachine {
    db: Db,
}


impl StateMachine {

    pub fn open(path: String) -> Self {
        let db=sled::open(path).unwrap();
        StateMachine {
            db
        }
    }

    pub fn apply_command(&self, command: StateMachineCommand) {
        match command.command_type() {
            CommandType::Put => {
                self.db.insert(command.key(), command.value().as_bytes());
            }
            CommandType::Delete => {
                self.db.remove(command.key());
            }
        }
    }

    pub fn get_value(&self, key: String) -> Option<String> {
        let get_result=&self.db.get(key);
        match get_result {
            Ok(value) => {
                match value {
                    None => { None}
                    Some(ivec) => {
                        Some(String::from_utf8(Vec::from(ivec.as_ref())).unwrap())
                    }
                }
            }
            Err(_) => { None}
        }
    }
}