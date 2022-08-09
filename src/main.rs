use std::{io::{stdout, Write, stdin}, fs::OpenOptions};

use crate::{db::DataBase, transaction::{checkpointing, Transaction, OperationRecord}};

mod io;
mod db;
mod log;
mod transaction;
fn main() {
    println!("Hello, world!");
    let mut db = DataBase::new().unwrap();
    checkpointing(&mut db).unwrap();
    let mut wal_log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("./data_wal.log").unwrap();
    loop {
        print!("> ");
        stdout().flush().unwrap();
        let mut input_buf = String::new();
        stdin().read_line(&mut input_buf).unwrap();
        let mut splited_input = input_buf.split_whitespace();
        let operation = splited_input.next().unwrap();
        let mut transaction = Transaction::new(&mut db);
        match operation {
            "get" => {
                let key = splited_input.next().unwrap();
                println!("{}",db.get(key).unwrap_or("Not Found Value".to_string()));
            },
            "insert" => {
                let key = splited_input.next().unwrap();
                let value = splited_input.next().unwrap();
                transaction.add_operation(OperationRecord { command: transaction::Command::Insert, key:key.to_string(), value:value.to_string()});
                db.insert(key,value);
            },
            "remove" => {
                let key = splited_input.next().unwrap();
                transaction.add_operation(OperationRecord { command: transaction::Command::Remove, key:key.to_string(), value:"".to_string()});
                db.remove(key);
            },
            "commit" => {
                transaction.commit().to_serializable()
                    .atomic_log_write(&mut wal_log_file).unwrap();
                db.apply_commit();
                transaction = Transaction::new(&mut db);
            },
            "abort" => {
                transaction.abort().to_serializable()
                    .atomic_log_write(&mut wal_log_file).unwrap();
                db.apply_abort();
                transaction = Transaction::new(&mut db);
            },
            _ => println!("error"),

        }

    }
}
