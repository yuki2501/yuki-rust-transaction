use std::{io::{self, Read, Seek, SeekFrom}, fs::{File,OpenOptions}, path::Path};
use crate::io::delete_file;
use super::{transaction};
use anyhow::Context;
use serde::{Serialize, Deserialize};
use crc32fast::*;
use thiserror::Error;

type Result<T> = anyhow::Result<T>;

type OperationRecord = transaction::OperationRecord;
impl OperationRecord {
     fn to_serializable(self) -> LogRecord {
        let command = self.command;
        let key = self.key.clone().into_bytes();
//        let key_length = self.key_length.to_be_bytes().to_vec();
        let value = self.value.clone().into_bytes();
//       let value_length = self.value_hash.to_be_bytes().to_vec();
     //   let value_hash = self.value_hash;
        return LogRecord {
            command,
            key,
//            key_length,
            value,
//            value_length,
      //      value_hash
        }
    }
}


type Transaction = transaction::Transaction;
impl Transaction {
    pub fn to_serializable(self) -> TransactionLog {
        let status = self.status;
        let operations = self.operations
            .into_iter()
            .map(OperationRecord::to_serializable)
            .collect();
        return TransactionLog {
            status,
            operations,
        }
    }
}


type Command = transaction::Command;
type Status = transaction::TransactionStatus;

#[derive(Debug, PartialEq, Deserialize, Serialize,Clone)]
pub struct LogRecord {
  command: Command,
  #[serde(with = "serde_bytes")]
  key: Vec<u8>,
  //#[serde(with = "serde_bytes")]
  //key_length: Vec<u8>,
  #[serde(with = "serde_bytes")]
  value: Vec<u8>,
  //#[serde(with = "serde_bytes")] 
  //value_length: Vec<u8>,
  //value_hash: u32,
}

impl LogRecord {
     pub fn to_operations(self) -> OperationRecord{
         let command = self.command.clone();
         let key = String::from_utf8(self.key.clone()).unwrap();
         let value = String::from_utf8(self.value.clone()).unwrap();
//         let value_hash = self.value_hash;
         return 
             OperationRecord {
                 command,
                 key,
                 value,
                 //value_hash,
             }
         
     }
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct TransactionLog {
    pub status: Status,
    pub operations: Vec<LogRecord>,
}
//TODO Fix Chechpointing time
impl TransactionLog {

    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        if self.status == Status::Commit {
            let mut transaction_bytes = bincode::serialize(self).unwrap();
            let len = transaction_bytes
                .len()
                .to_le_bytes()
                .to_vec();
            let mut crc32hash_byte = hash(&transaction_bytes)
                .to_le_bytes()
                .to_vec();
            let mut log_bytes = len;
            log_bytes.append(&mut crc32hash_byte);
            log_bytes.append(&mut transaction_bytes);
            Some(log_bytes)
        } else {
            None
        }
    }

    pub fn atomic_log_write(&self,file:&mut File) -> Result<()>{
        let byte_for_log = TransactionLog::to_bytes(self);
        match byte_for_log {
            Some(x) => {
                super::io::write(&file,&x)
            },
            None => Ok(()), 
        }
    }

    pub fn to_operations_record(self) -> Transaction { 
        let operations = self.operations
            .into_iter()
            .map(LogRecord::to_operations)
            .collect();
        let status = self.status;
        return
            Transaction{
                status,
                operations,
            }
    }

}

#[derive(Debug,Error,PartialEq)]
pub enum DeserializeError {
    #[error("UnexpectedEof")]
    Eof,
    #[error("checksum don't match")]
    ChecksumUnmatch,
    #[error("other error")]
    OtherError,
}


pub fn deserialize_transaction(file: &mut File,position: u64)-> std::result::Result<(TransactionLog,u64),DeserializeError> {
// len -> 8byte, checksum -> 4byte, rest -> (len) byte
    file.rewind().map_err(|err| if err.kind() == std::io::ErrorKind::UnexpectedEof {DeserializeError::Eof} else {DeserializeError::OtherError})?;
    file.seek(SeekFrom::Start(position)).map_err(|err| if err.kind() == std::io::ErrorKind::UnexpectedEof {DeserializeError::Eof} else {DeserializeError::OtherError})?;
    let mut buffer = [0; 8];
    file.read_exact(&mut buffer).map_err(|err| if err.kind() == std::io::ErrorKind::UnexpectedEof {DeserializeError::Eof} else {DeserializeError::OtherError})?; 
    let data_len = u64::from_le_bytes(buffer);
    let mut buffer = [0; 4];
    file.read_exact(&mut buffer)
        .map_err(|err| if err.kind() == std::io::ErrorKind::UnexpectedEof {DeserializeError::Eof} 
                 else {DeserializeError::OtherError})?; 
    let checksum = u32::from_le_bytes(buffer);
    let mut buffer = Vec::new();
    let mut handle = file.take(data_len);
    handle.read_to_end(&mut buffer)
        .map_err(|err| if err.kind() == std::io::ErrorKind::UnexpectedEof {DeserializeError::Eof} 
                 else {DeserializeError::OtherError})?;
    if crc32fast::hash(&buffer) != (checksum) {
        return Err(DeserializeError::ChecksumUnmatch);
    } 

    let log_content = bincode::deserialize(&buffer).map_err(|_| DeserializeError::OtherError)?;
    return Ok((log_content,file.stream_position().unwrap()));
}

pub fn deserialize_transaction_vector(file: &mut File) -> Vec<TransactionLog> {
    let mut transaction_vec: Vec<TransactionLog> = Vec::new();
    let mut position = 0;
    loop {
        match deserialize_transaction(file,position) {
            Ok((x,n)) => {
                transaction_vec.push(x);
                position = n;
                println!("InOk: {:?}\n",position);
                continue;
            },
            Err(x) => {
                if x == DeserializeError::Eof {
                    break;
                } else {
                    position += 1;
                    continue;
                }
            },  
        }
    }
    return transaction_vec;
}



#[cfg(test)]
mod test{
  use anyhow::Context;


  use super::*;
  #[test]
  fn serialize_test(){
      let log_record = vec![OperationRecord {
        command: Command::Insert,
        key:"test".to_string(),
        //key_length: 4,
        value: "test".to_string(),
        //value_length: 4,
        //value_hash: 100,
      }.to_serializable()];
      let status = Status::Commit;
      let commit_records = TransactionLog {
          status,
          operations:log_record.clone(),
      };
      let abort_records = TransactionLog {
          status: Status::Abort,
          operations:log_record.clone(),
      };

      let mut log_file = OpenOptions::new()
          .create(true)
          .append(true)
          .write(true)
          .open("./data_test.log").context("cannot open file").unwrap();
      commit_records.atomic_log_write(&mut log_file).unwrap();
      delete_file(Path::new("./data_test.log")).unwrap();
  }
  #[test]
  fn deserialize_test(){
    let operation_records = vec![OperationRecord {
        command: Command::Insert,
        key:"test".to_string(),
        //key_length: 4,
        value: "test".to_string(),
        //value_length: 4,
        //value_hash: 100,
      }];
    let log_records = operation_records.clone()
        .into_iter()
        .map(OperationRecord::to_serializable)
        .collect::<Vec<LogRecord>>();
      let status = Status::Commit;
      let transactions = vec![
          Transaction{
              status: status.clone(),
              operations: operation_records,
          }
      ];
      let commit_records = vec![TransactionLog {
          status: status.clone(),
          operations:log_records,
      }];
      let mut log_file = OpenOptions::new()
          .append(true)
          .create(true)
          .open(Path::new("./data_test.log")).context("cannot open file").unwrap();
      for transaction_log in commit_records.iter(){
        transaction_log.atomic_log_write(&mut log_file).unwrap();
      }
      let mut log_file = OpenOptions::new()
          .read(true)
          .open(Path::new("./data_test.log")).context("cannot open file").unwrap();
      let deserialized_log_content:Vec<Transaction> = deserialize_transaction_vector(&mut log_file)
          .into_iter()
          .map(TransactionLog::to_operations_record)
          .collect::<Vec<Transaction>>();
      assert_eq!(transactions,deserialized_log_content);
      delete_file(Path::new("./data_test.log")).unwrap();
  }
}
