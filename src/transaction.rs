use std::{path::Path, fs::OpenOptions};

use anyhow::Context;
use serde::{Serialize,Deserialize};

use crate::{log::deserialize_transaction_vector, db::DataBase, io::delete_file};

type Result<T> = anyhow::Result<T>;

#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
pub enum Command {
    Insert,
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
    pub fn execute_operation(self,db: &mut DataBase) {
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
    pub fn new() -> Transaction{
        return Transaction {
               status: TransactionStatus::Abort,
               operations: Vec::new(),
        }
    }

    pub fn execute_transaction(self,db: &mut DataBase) {
       let status = self.status;
       
       if status == TransactionStatus::Abort {
           db.apply_abort(); 
       }
       else { 
          let operations = self.operations;
          for operation in operations.into_iter() {
             operation.execute_operation(db);
          }
          db.apply_commit();
       }
    }

    pub fn add_operation(&mut self, operation: OperationRecord) {
        self.operations.push(operation);
    }

    pub fn set_comitted(&mut self) {
        self.status = TransactionStatus::Commit;
    }

    pub fn set_abortted(&mut self) {
        self.status = TransactionStatus::Abort;
    }
}

pub fn crash_recovery(db: &mut DataBase) -> Result<()> {
    let mut wal_log_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open("./data_wal.log")
        .context("cannot open log file")?;
    for transaction in deserialize_transaction_vector(&mut wal_log_file) {
        Transaction::execute_transaction(transaction.to_operations_record(), db);
    };
    Ok(())
}

pub fn checkpointing(db:&mut DataBase) -> Result<()> {
    db.snapshot()
        .context("cannot take snapshot")?;
    delete_file(Path::new("./data_wal.log"))?;
    Ok(())
}



