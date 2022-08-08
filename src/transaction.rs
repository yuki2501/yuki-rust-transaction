use std::{path::Path, fs::File};

use anyhow::{anyhow, Context};
use serde::{Serialize,Deserialize};

use crate::{log::deserialize_log, db::DataBase};

#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
pub enum Command {
    Insert,
 //   Get,
    Remove,
}

#[derive(Debug,PartialEq,Serialize,Deserialize)]
pub enum TransactionStatus {
    Commit,
    Abort,
}

#[derive(Clone,Debug,PartialEq)]
pub struct OperationRecord {
    pub command: Command,
    pub key: String,
//    pub key_length: u32,
    pub value: String,
//    pub value_length: u32,
    pub value_hash: u32,
}

impl OperationRecord {
    pub fn consume_operation(self,db: &mut DataBase) {
        match self.command.clone() {
            Command::Insert => db.insert(&self.key, &self.value),
            Command::Remove => db.remove(&self.key),
        }
    }
}



pub struct Transaction {
    pub status: TransactionStatus,
    pub operations: Vec<OperationRecord>,
}

impl Transaction {
    fn new(db: &mut DataBase) -> Transaction{
        let mut log_file = File::open(Path::new("./data_test.log")).context("cannot open file").unwrap();
        let deserialized_log = deserialize_log(&mut log_file);
        match deserialized_log {
            Ok(deserialized_data) => {
                deserialized_data.to_operations_record()
                    .consume_transaction(db);
                return Transaction {
                    status: TransactionStatus::Abort,
                    operations: Vec::new(),
                }
            } 
           Err(_) => Transaction {
               status: TransactionStatus::Abort,
               operations: Vec::new(),
           }
        }
    }

    fn consume_transaction(self,db: &mut DataBase) {
       let status = self.status;
       
       if status == TransactionStatus::Abort {
           return; 
       }
       else { 
          let operations = self.operations;
          for operation in operations.into_iter() {
             operation.consume_operation(db);
          }
       }
    }

    fn add_operation(&mut self, operation: OperationRecord) -> Transaction {
        self.operations.push(operation);
        return Transaction{
            status: TransactionStatus::Abort,
            operations: self.operations.clone(),
        }
    }

    fn commit(self) -> Transaction {
        return Transaction {
            status: TransactionStatus::Commit,
            operations: self.operations,
        }
    }

    fn abort(self) -> Transaction {
        return Transaction {
            status: TransactionStatus::Abort,
            operations: self.operations,

        }
    }
}



