use std::io::{self, BufReader, BufWriter};
use std::io::prelude::*;
use std::fs::{File, remove_file};
use std::path::{Path, self};
use anyhow::{anyhow, Context};

type Result<T> = anyhow::Result<T>;

pub fn open(path: &str) -> Result<File>{
   let file = File::open(path).context("Cannot open file")?;
   Ok(file) 
}

fn fsync(file: &File) -> Result<()> {
   file.sync_all()?;
   Ok(())
}



pub fn write(file: &File, value:&Vec<u8>) -> Result<()> {
    let mut writer = BufWriter::new(file);
    writer.write_all(value).context("Cannot write")?;
    writer.flush()?;
    fsync(file)?;
    Ok(())
}

pub fn delete_file(path: &Path) -> Result<()> {
    remove_file(path).context("cannot remove")?;
    Ok(())
}
