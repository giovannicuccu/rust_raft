use sled::{Db, IVec, Error};
use crate::common::{StateMachineCommand};

pub struct StateMachine {
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
        match command {
            StateMachineCommand::Put { key, value } => {
                self.db.insert(key, value.as_bytes());
            }
            StateMachineCommand::Delete { key } => {
                self.db.remove(key);
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

#[cfg(test)]
mod tests {
    use std::env;
    use rand::Rng;
    use std::fs::{create_dir, remove_dir_all};
    use crate::state_machine::StateMachine;
    use crate::common::{StateMachineCommand};
    use crate::common::StateMachineCommand::{Put, Delete};

    fn create_test_dir() -> String {
        let dir = env::temp_dir();
        let mut rng = rand::thread_rng();
        let dir = format!("{}{}",dir.display(), rng.gen::<u32>());
        println!("dir={}",dir);
        create_dir(&dir).unwrap();
        dir
    }

    #[test]
    fn create_db_with_a_path() {
        let dir=create_test_dir();
        let state_machine=StateMachine::open(dir.clone());
        remove_dir_all(dir);
    }

    #[test]
    fn apply_put_command() {
        let dir=create_test_dir();
        let state_machine=StateMachine::open(dir.clone());
        let machine_command=Put {
            key: String::from("akey"),
            value: String::from("avalue")
        };
        state_machine.apply_command(machine_command);
        let value=state_machine.get_value(String::from("akey"));
        assert!(value.is_some());
        assert_eq!(value.unwrap(),String::from("avalue"));
        remove_dir_all(dir);
    }

    #[test]
    fn apply_delete_command() {
        let dir=create_test_dir();
        let state_machine=StateMachine::open(dir.clone());
        let put_command=Put {
            key: String::from("akey"),
            value: String::from("avalue")
        };
        state_machine.apply_command(put_command);
        let value=state_machine.get_value(String::from("akey"));
        assert!(value.is_some());
        assert_eq!(value.unwrap(),String::from("avalue"));
        let delete_command=Delete {
            key: String::from("akey")
        };
        state_machine.apply_command(delete_command);
        let value=state_machine.get_value(String::from("akey"));
        assert!(value.is_none());
        remove_dir_all(dir);
    }
}