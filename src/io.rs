use std::io::BufWriter;
use std::io::prelude::*;
use std::fs::{File, remove_file};
use std::path::Path;
use anyhow::Context;

type Result<T> = anyhow::Result<T>;

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
