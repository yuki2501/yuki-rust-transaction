use std::{collections::BTreeMap, fs::{File, OpenOptions} };

use anyhow::Context;

type Key = String;
type Value = String;
type DataBaseValue = BTreeMap<Key, Value>;
type DataBaseWriteSetValue = BTreeMap<Key, Option<Value>>;

pub struct DataBase {
   wal_log_file: File,
   values: DataBaseValue,
   write_set: DataBaseWriteSetValue,
}

impl DataBase {
    pub fn new () -> anyhow::Result<Self> {
        let log_file = OpenOptions::new()
            .append(true)
            .open("./data.log")
            .context("failed to open log")?;
        let db_values : DataBaseValue = BTreeMap::new();
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
}
