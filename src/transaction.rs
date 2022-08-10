use std::{collections::BTreeMap, fs::OpenOptions, path::Path};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::{db::DataBase, io::delete_file, log::deserialize_transaction_vector};

type Result<T> = anyhow::Result<T>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Insert,
    Remove,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum TransactionStatus {
    Commit,
    Abort,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OperationRecord {
    pub command: Command,
    pub key: String,
    pub value: String,
    //pub value_hash: u32,
}

impl OperationRecord {
    pub fn execute_operation(self, write_set: &mut BTreeMap<String, String>) {
        match self.command {
            Command::Insert => write_set.insert(self.key, self.value),
            Command::Remove => write_set.remove(&self.key),
        };
    }
}

#[derive(Debug, PartialEq)]
pub struct Transaction {
    pub status: TransactionStatus,
    pub operations: Vec<OperationRecord>,
}

impl Transaction {
    pub fn new() -> Transaction {
        return Transaction {
            status: TransactionStatus::Abort,
            operations: Vec::new(),
        };
    }

    pub fn execute_transaction(&mut self, db: &mut DataBase) {
        if self.status != TransactionStatus::Abort {
            db.apply_commit(self);
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
        self.operations = Vec::new();
    }

    pub fn tmp_write_set(&mut self, db: &DataBase) -> BTreeMap<String, String> {
        let mut write_set = db.values.clone();
        for operation in self.operations.clone().into_iter() {
            operation.execute_operation(&mut write_set);
        }
        return write_set;
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
        Transaction::execute_transaction(&mut transaction.to_operations_record(), db);
    }
    Ok(())
}

pub fn checkpointing(db: &mut DataBase) -> Result<()> {
    db.take_snapshot().context("cannot take snapshot")?;
    delete_file(Path::new("./data_wal.log"))?;
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::transaction::{Command, OperationRecord};

    use super::*;
    #[test]
    fn remove_works() {
        let mut test_db = DataBase::new().unwrap();
        let mut transaction = Transaction::new();
        transaction.add_operation(OperationRecord {
            command: Command::Insert,
            key: "testkey".to_string(),
            value: "testvalue".to_string(),
        });
        transaction.add_operation(OperationRecord {
            command: Command::Insert,
            key: "testkey2".to_string(),
            value: "testvalue2".to_string(),
        });
        transaction.add_operation(OperationRecord {
            command: Command::Remove,
            key: "testkey".to_string(),
            value: "".to_string(),
        });
        transaction.set_comitted();
        transaction.execute_transaction(&mut test_db);
        assert_eq!(test_db.get("testkey", &mut transaction), None);
        assert_eq!(
            test_db.get("testkey2", &mut transaction),
            Some("testvalue".to_string())
        );
    }
}
