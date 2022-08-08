use std::{io::{self, Read}, fs::File, path::Path};
use crate::io::delete_file;

use super::transaction;
use anyhow::Context;
use serde::{Serialize, Deserialize, de::IntoDeserializer};

type Result<T> = anyhow::Result<T>;

type OperationRecord = transaction::OperationRecord;
impl OperationRecord {
     fn to_serializable(self) -> LogRecord {
        let command = self.command;
        let key = self.key.clone().into_bytes();
//        let key_length = self.key_length.to_be_bytes().to_vec();
        let value = self.value.clone().into_bytes();
//       let value_length = self.value_hash.to_be_bytes().to_vec();
        let value_hash = self.value_hash;
        return LogRecord {
            command,
            key,
//            key_length,
            value,
//            value_length,
            value_hash
        }
    }
}


type Transaction = transaction::Transaction;
impl Transaction {
    fn to_serializable(self) -> LogRecords {
        let status = self.status;
        let operations = self.operations
            .into_iter()
            .map(OperationRecord::to_serializable)
            .collect();
        return LogRecords {
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
  value_hash: u32,
}

impl LogRecord {
     pub fn to_operations(self) -> OperationRecord{
         let command = self.command.clone();
         let key = String::from_utf8(self.key.clone()).unwrap();
         let value = String::from_utf8(self.value.clone()).unwrap();
         let value_hash = self.value_hash;
         return 
             OperationRecord {
                 command,
                 key,
                 value,
                 value_hash,
             }
         
     }
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct LogRecords {
    pub status: Status,
    pub operations: Vec<LogRecord>,
}

impl LogRecords {

    pub fn to_bytes(self) -> Option<Vec<u8>> {
        if self.status == Status::Commit {
            Some(bincode::serialize(&self).unwrap())
        } else {
            None
        }
    }

    pub fn atomic_log_write(self,file:&File) -> Result<()>{
        let byte_for_log = self.to_bytes();
        match byte_for_log {
            Some(x) => super::io::write(file,&x),
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

pub fn deserialize_log(file: &mut File)-> Result<LogRecords>{
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    let log_content = bincode::deserialize(&buffer).context("cannot deserialize")?;
    delete_file(Path::new("./data_test.log"))?;  
    return Ok(log_content);
}


#[cfg(test)]
mod test{
  use anyhow::Context;

  use crate::transaction::OperationRecord;


use super::*;
  #[test]
  fn serialize_test(){
      let log_record = vec![OperationRecord {
        command: Command::Insert,
        key:"test".to_string(),
        //key_length: 4,
        value: "test".to_string(),
        //value_length: 4,
        value_hash: 100,
      }.to_serializable()];
      let status = Status::Commit;
      let commit_records = LogRecords {
          status,
          operations:log_record.clone(),
      };
      let abort_records = LogRecords {
          status: Status::Abort,
          operations:log_record.clone(),
      };

      let log_file = File::create("./data_test.log").context("cannot open file").unwrap();
      commit_records.atomic_log_write(&log_file).unwrap();
      abort_records.atomic_log_write(&log_file).unwrap();
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
        value_hash: 100,
      }];
    let log_records = operation_records.clone()
        .into_iter()
        .map(OperationRecord::to_serializable)
        .collect::<Vec<LogRecord>>();
      let status = Status::Commit;
      let commit_records = LogRecords {
          status,
          operations:log_records.clone(),
      };
      let log_file = File::create(Path::new("./data_test.log")).context("cannot open file").unwrap();
      commit_records.atomic_log_write(&log_file).unwrap();
      let mut log_file = File::open(Path::new("./data_test.log")).context("cannot open file").unwrap();
      let deserialized_log_content:Vec<OperationRecord> = deserialize_log(&mut log_file)
          .unwrap()
          .operations
          .into_iter()
          .clone()
          .map(LogRecord::to_operations)
          .collect::<Vec<OperationRecord>>();
      assert_eq!(operation_records,deserialized_log_content);
  }
}
