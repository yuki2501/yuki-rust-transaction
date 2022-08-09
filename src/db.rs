use std::{collections::BTreeMap, fs::{File, OpenOptions}, path::Path, io::Read};
use serde::{Serialize,Deserialize};
use anyhow::{Context, ensure};

type Key = String;
type Value = String;
type DataBaseValue = BTreeMap<Key, Value>;
type DataBaseWriteSetValue = BTreeMap<Key, Option<Value>>;
type Result<T> = anyhow::Result<T>;
pub struct DataBase {
   wal_log_file: File,
   values: DataBaseValue,
   write_set: DataBaseWriteSetValue,
}

impl DataBase {
    pub fn new () -> anyhow::Result<Self> {
        let mut log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open("./data_wal.log")
            .context("failed to open log")?;
        let db_values = match (deserialize_snapshot()) {
            Ok(snapshot) => snapshot,
            _ => BTreeMap::new(),
        };
        let db_writeset : DataBaseWriteSetValue = BTreeMap::new();
        Ok(DataBase {
        wal_log_file: log_file,
        values: db_values,
        write_set: db_writeset,
        })
    }

    pub fn get (&self, key: &str) -> Option<String> {
        let value = self.write_set
            .get(key)
            .map( |v| v.clone())?;
        value
    }

    pub fn insert(&mut self, key: &str, value: &str) { 
        self.write_set
            .insert(key.to_string(), Some(value.to_string())); 
    }

    pub fn remove(&mut self, key: &str) {
        self.write_set
            .insert(key.to_string(), None);
    }

    pub fn apply_commit(&mut self)  {
        let mut new_values: BTreeMap<Key,Value> = BTreeMap::new();
        for (k,v) in self.write_set.iter_mut() {
            if v.clone() != None {
                new_values.insert(k.to_string(), v.clone().unwrap().to_string());
            }
        }
        self.values = new_values;
    } 

    pub fn apply_abort(&mut self){
        self.write_set = BTreeMap::new();
    }

    pub fn snapshot(&self) -> Result<()>{
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open("./data.log")
            .context("failed to open log")?;
        let mut values_byte= bincode::serialize(&self.values)
            .context("cannot serialize")?;
        let mut values_len = values_byte
            .len()
            .to_le_bytes()
            .to_vec();
        let mut hash = crc32fast::hash(&values_byte)
            .to_le_bytes()
            .to_vec();
        let mut snapshot_log = Vec::new();
        snapshot_log.append(&mut values_len);
        snapshot_log.append(&mut hash);
        snapshot_log.append(&mut values_byte);
        super::io::write(&file,&snapshot_log)
    }
}

pub fn deserialize_snapshot() -> Result<DataBaseValue> {
    let mut file = OpenOptions::new()
        .read(true)
        .open("./data.log")
        .context("failed to open log")?;
    let mut buffer = [0;8];
    file.read_exact(&mut buffer).context("cannot read len")?;
    let data_len = u64::from_le_bytes(buffer);
    let mut buffer = [0;4];
    file.read_exact(&mut buffer).context("cannot read checksum")?;
    let checksum = u32::from_le_bytes(buffer);
    let mut buffer = Vec::new();
    let mut handler = file.take(data_len);
    handler.read_to_end(&mut buffer).context("cannot read snapshot")?;
    ensure!(crc32fast::hash(&mut buffer) == checksum,"checksum don't match");
    let snapshot_content = bincode::deserialize(&mut buffer).context("cannot deserialize snapshot")?;
    Ok(snapshot_content)
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn remove_works() {
        let mut test_db = DataBase::new().unwrap();
        test_db.insert("testkey", "testvalue");
        test_db.remove("testkey");
        assert_eq!(test_db.get("testkey"), None);
    }
    #[test]
    #[should_panic]
    fn abort_works() {
        let mut test_db = DataBase::new().unwrap();
        test_db.insert("testkey", "testvalue");
        test_db.apply_abort();
        test_db.get("testkey").unwrap();
    }

    #[test]
    fn snapshot_test() {
        let mut db = DataBase::new().unwrap();
        db.insert("snapshot_test","snapshot_test_value");
        db.insert("snapshot_test2","snapshot_test_value2");
        db.remove("snapshot_test2");
        db.apply_commit();
        db.snapshot().unwrap();
        let deserialized_db = deserialize_snapshot().unwrap();
        assert_eq!(db.values,deserialized_db);
    }
}
