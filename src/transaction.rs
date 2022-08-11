use std::{collections::BTreeMap, fs::OpenOptions, path::Path};

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::{db::DataBase, io::delete_file, log::deserialize_transaction_vector};

type Result<T> = anyhow::Result<T>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Insert,
    Remove,
    Get,
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

pub enum OperationResult {
    DoneInsert,
    DoneRemove,
    DoneGet(Option<String>),
}

impl OperationRecord {
    pub fn execute_operation_for_writeset(
        self,
        db: &mut DataBase,
        write_set: &mut BTreeMap<String, Option<String>>,
    ) -> OperationResult {
        match self.command {
            Command::Insert => {
                write_set.insert(self.key, Some(self.value));
                return OperationResult::DoneInsert;
            }
            Command::Remove => {
                write_set.insert(self.key, None);
                return OperationResult::DoneRemove;
            }
            Command::Get => match (db.get(&self.key), write_set.get(&self.key)) {
                (_, Some(Some(x))) => OperationResult::DoneGet(Some(x.to_string())),
                (Some(x), None) => OperationResult::DoneGet(Some(x.to_string())),
                (Some(_), Some(None)) => OperationResult::DoneGet(None),
                (None, Some(None)) => OperationResult::DoneGet(None),
                (None, None) => OperationResult::DoneGet(None),
            },
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Transaction {
    pub status: TransactionStatus,
    pub operations: Vec<OperationRecord>,
    pub write_set: BTreeMap<String, Option<String>>,
}

impl Transaction {
    pub fn new() -> Transaction {
        let write_set = BTreeMap::new();
        Transaction {
            status: TransactionStatus::Abort,
            operations: Vec::new(),
            write_set,
        }
    }

    pub fn execute_transaction(&mut self, db: &mut DataBase) {
        if self.status != TransactionStatus::Abort {
            self.apply_commit(db);
        }
    }

    pub fn add_operation_to_transaction(
        &mut self,
        db: &mut DataBase,
        operation: &OperationRecord,
    ) -> OperationResult {
        self.operations.push(operation.clone());
        OperationRecord::execute_operation_for_writeset(operation.clone(), db, &mut self.write_set)
    }

    pub fn set_comitted(&mut self) {
        self.status = TransactionStatus::Commit;
    }

    pub fn set_abortted(&mut self) {
        self.status = TransactionStatus::Abort;
        self.operations = Vec::new();
    }

    pub fn apply_commit(&mut self, db: &mut DataBase) {
        let iter = self.write_set.iter();
        for record in iter {
            match record {
                (key, Some(value)) => {
                    db.values.insert(key.to_string(), value.to_string());
                }
                (key, None) => {
                    db.values.remove(key);
                }
            }
        }
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
        transaction.add_operation_to_transaction(
            &mut test_db,
            &OperationRecord {
                command: Command::Insert,
                key: "testkey".to_string(),
                value: "testvalue".to_string(),
            },
        );
        transaction.add_operation_to_transaction(
            &mut test_db,
            &OperationRecord {
                command: Command::Insert,
                key: "testkey2".to_string(),
                value: "testvalue2".to_string(),
            },
        );
        transaction.add_operation_to_transaction(
            &mut test_db,
            &OperationRecord {
                command: Command::Remove,
                key: "testkey".to_string(),
                value: "".to_string(),
            },
        );
        transaction.set_comitted();
        transaction.execute_transaction(&mut test_db);
        assert_eq!(test_db.get("testkey"), None);
        assert_eq!(test_db.get("testkey2"), Some(&("testvalue".to_string())));
    }
}
