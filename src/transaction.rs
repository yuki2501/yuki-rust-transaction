use std::{path::Path, fs::{File, OpenOptions}};

use anyhow::{anyhow, Context};
use serde::{Serialize,Deserialize};

use crate::{log::{deserialize_transaction, deserialize_transaction_vector}, db::DataBase, io::delete_file};

type Result<T> = anyhow::Result<T>;

#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
pub enum Command {
    Insert,
    //Get,
    Remove,
}

#[derive(Debug,PartialEq,Serialize,Deserialize,Clone)]
pub enum TransactionStatus {
    Commit,
    Abort,
}

#[derive(Clone,Debug,PartialEq)]
pub struct OperationRecord {
    pub command: Command,
    pub key: String,
    pub value: String,
    //pub value_hash: u32,
}

impl OperationRecord {
    pub fn consume_operation(self,db: &mut DataBase) {
        match self.command.clone() {
            Command::Insert => db.insert(&self.key, &self.value),
            Command::Remove => db.remove(&self.key),
        }
    }
}



#[derive(Debug,PartialEq)]
pub struct Transaction {
    pub status: TransactionStatus,
    pub operations: Vec<OperationRecord>,
}

impl Transaction {
    pub fn new(db: &mut DataBase) -> Transaction{
        return Transaction {
               status: TransactionStatus::Abort,
               operations: Vec::new(),
        }
    }

    pub fn consume_transaction(self,db: &mut DataBase) {
       let status = self.status;
       
       if status == TransactionStatus::Abort {
           db.apply_abort(); 
       }
       else { 
          let operations = self.operations;
          for operation in operations.into_iter() {
             operation.consume_operation(db);
          }
          db.apply_commit();
       }
    }

    pub fn add_operation(&mut self, operation: OperationRecord) -> Transaction {
        self.operations.push(operation);
        return Transaction{
            status: TransactionStatus::Abort,
            operations: self.operations.clone(),
        }
    }

    pub fn commit(&self) -> Transaction {
        return Transaction {
            status: TransactionStatus::Commit,
            operations: self.operations.clone(),
        }
    }

    pub fn abort(&self) -> Transaction {
        return Transaction {
            status: TransactionStatus::Abort,
            operations: self.operations.clone(),

        }
    }
}

pub fn checkpointing(db:&mut DataBase) -> Result<()> {
    let mut wal_log_file = OpenOptions::new()
        .read(true)
        .open("./data_wal.log")
        .context("cannot open log file")?;
    for transaction in deserialize_transaction_vector(&mut wal_log_file) {
        Transaction::consume_transaction(transaction.to_operations_record(), db);
    };
    db.snapshot()
        .context("cannot take snapshot")?;
    delete_file(Path::new("./data_wal.log"))?;
    Ok(())
}



