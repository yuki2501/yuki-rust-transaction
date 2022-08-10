use anyhow::{ensure, Context};
use std::{collections::BTreeMap, fs::OpenOptions, io::Read};

use crate::{transaction::Transaction, io::write};

type Key = String;
type Value = String;
type DataBaseValue = BTreeMap<Key, Value>;
type Result<T> = anyhow::Result<T>;
pub struct DataBase {
    pub values: DataBaseValue,
}

impl DataBase {
    pub fn new() -> anyhow::Result<Self> {
        let db_values = match deserialize_snapshot() {
            Ok(snapshot) => snapshot,
            Err(_) => BTreeMap::new(),
        };
        Ok(DataBase { values: db_values })
    }

    pub fn get(&mut self, key: &str) -> Option<&String> {
        self.values.get(key)
    }

    // pub fn apply_commit(&mut self, transaction: &mut Transaction) {
    //     self.values = transaction.write_set;
    // }

    pub fn take_snapshot(&self) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open("./data.log")
            .context("failed to open log")?;

        let mut values_byte = bincode::serialize(&self.values).context("cannot serialize")?;

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

        write(&file, &snapshot_log)
    }
}

pub fn deserialize_snapshot() -> Result<DataBaseValue> {
    let mut file = OpenOptions::new()
        .read(true)
        .open("./data.log")
        .context("failed to open log")?;

    let mut buffer = [0; 8];
    file.read_exact(&mut buffer).context("cannot read len")?;
    let data_len = u64::from_le_bytes(buffer);

    let mut buffer = [0; 4];
    file.read_exact(&mut buffer)
        .context("cannot read checksum")?;
    let checksum = u32::from_le_bytes(buffer);

    let mut buffer = Vec::new();
    let mut handler = file.take(data_len);
    handler
        .read_to_end(&mut buffer)
        .context("cannot read snapshot")?;
    ensure!(
        crc32fast::hash(&mut buffer) == checksum,
        "checksum don't match"
    );

    let snapshot_content =
        bincode::deserialize(&mut buffer).context("cannot deserialize snapshot")?;

    Ok(snapshot_content)
}
