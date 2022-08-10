use std::{
    fs::OpenOptions,
    io::{stdin, stdout, Write},
};

use crate::{
    db::DataBase,
    transaction::{checkpointing, crash_recovery, OperationRecord, Transaction, OperationResult},
};

mod db;
mod io;
mod log;
mod transaction;
fn main() {
    println!("Hello, world!");
    let mut db = DataBase::new().unwrap();
    crash_recovery(&mut db).unwrap();
    checkpointing(&mut db).unwrap();
    let mut wal_log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("./data_wal.log")
        .unwrap();
    let mut transaction = Transaction::new();
    loop {
        print!("> ");
        stdout().flush().unwrap();
        let mut input_buf = String::new();
        stdin().read_line(&mut input_buf).unwrap();
        let mut splited_input = input_buf.split_whitespace();
        let operation = splited_input.next().unwrap();
        match operation {
            "get" => {
                let key = splited_input.next().unwrap();
                let result = transaction.add_operation_to_transaction(&mut db,&OperationRecord {
                    command: transaction::Command::Get,
                    key: key.to_string(),
                    value: "".to_string(),
                });
                let value = match result {
                    OperationResult::DoneGet(x) => {
                        match x {
                            Some(value) => value,
                            None => "not found".to_string(),
                        }
                    },
                    _ => "error".to_string(),
                };
                println!(
                    "{}",value
                );
            }

            "insert" => {
                let key = splited_input.next().unwrap();
                let value = splited_input.next().unwrap();
                transaction.add_operation_to_transaction(&mut db,&OperationRecord {
                    command: transaction::Command::Insert,
                    key: key.to_string(),
                    value: value.to_string(),
                });
            }

            "remove" => {
                let key = splited_input.next().unwrap();
                transaction.add_operation_to_transaction(&mut db,&OperationRecord {
                    command: transaction::Command::Remove,
                    key: key.to_string(),
                    value: "".to_string(),
                });
            }

            "commit" => {
                transaction.set_comitted();
                transaction
                    .to_serializable()
                    .atomic_log_write(&mut wal_log_file)
                    .unwrap();
                transaction.apply_commit(&mut db);
                db.take_snapshot().unwrap();
                transaction = Transaction::new();
            }

            "abort" => {
                transaction.set_abortted();
                transaction
                    .to_serializable()
                    .atomic_log_write(&mut wal_log_file)
                    .unwrap();
                transaction = Transaction::new();
            }

            _ => println!("error"),
        }
    }
}
