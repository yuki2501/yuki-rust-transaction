use std::{collections::BTreeMap, fs::OpenOptions,  io::Read};
use anyhow::{Context, ensure};

type Key = String;
type Value = String;
type DataBaseValue = BTreeMap<Key, Value>;
type DataBaseWriteSetValue = BTreeMap<Key, Option<Value>>;
type Result<T> = anyhow::Result<T>;
pub struct DataBase {
   values: DataBaseValue,
   write_set: DataBaseWriteSetValue,
}

impl DataBase {
    pub fn new () -> anyhow::Result<Self> {
        let db_values = match deserialize_snapshot() {
            Ok(snapshot) => {
                println!("deserialize_snapshot: {:?}",snapshot);
                snapshot
            },
            Err(_) => {
                BTreeMap::new()
            },
        };
        let db_writeset : DataBaseWriteSetValue = db_values.clone()
            .into_iter()
            .map(|x| (x.0,Some(x.1)))
            .collect();
        Ok(DataBase {
        values: db_values,
        write_set: db_writeset,
        })
    }

    pub fn get (&self, key: &str) -> Option<String> {
        let value_in_db = self.values.get(key);
        let value_in_write_set = self.write_set.get(key);
        match (value_in_db,value_in_write_set) {
            (_, Some(x)) => x.clone(),
            (Some(x),None) => Some(x.to_string()),
            (None,None) => None,
        }
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
        let mut new_values:DataBaseValue = BTreeMap::new();
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
        let file = OpenOptions::new()
            .write(true)
            .create(true)
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
        test_db.insert("testkey2","testvalue");
        test_db.remove("testkey");
        test_db.apply_commit();
        assert_eq!(test_db.get("testkey"), None);
        assert_eq!(test_db.get("testkey2"),Some("testvalue".to_string()));
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
        assert_eq!(db.get("snapshot_test").unwrap().to_string(),"snapshot_test_value");
        assert_eq!(db.get("test1").unwrap().to_string(),"test");
    }
}
