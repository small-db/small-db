use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    sync::{Mutex, MutexGuard},
};

use crate::{
    btree::page::BTreePage, error::SmallError, types::SmallResult,
};

pub struct SmallFile {
    file: Mutex<File>,
}

impl SmallFile {
    pub fn new(file_path: &str) -> Self {
        File::create(file_path).expect("io error");

        let f = Mutex::new(
            OpenOptions::new()
                .write(true)
                .read(true)
                .open(file_path)
                .unwrap(),
        );

        Self { file: f }
    }

    pub fn get_file(&self) -> MutexGuard<'_, File> {
        self.file.lock().unwrap()
    }

    pub fn read_u8(&self) -> Result<u8, SmallError> {
        let mut buf = [0u8; 1];
        self.get_file().read_exact(&mut buf).unwrap();
        Ok(buf[0])
    }

    pub fn write_u8(&self, v: u8) -> SmallResult {
        self.write(&[v])
    }

    pub fn write_u64(&self, v: u64) -> SmallResult {
        self.write(&v.to_le_bytes())
    }

    pub fn write(&self, buf: &[u8]) -> SmallResult {
        match self.get_file().write(buf) {
            Ok(_) => Ok(()),
            Err(e) => Err(SmallError::new(&e.to_string())),
        }
    }
}

pub struct SmallReader<'a> {
    buf: &'a Vec<u8>,
    cursor: usize,
}

impl<'a> SmallReader<'a> {
    pub fn new(buf: &'a Vec<u8>) -> Self {
        Self { buf, cursor: 0 }
    }

    pub fn read_exact(&mut self, bytes_count: usize) -> &'_ [u8] {
        let start = self.cursor;
        let end = self.cursor + bytes_count;

        // boundary check
        if end > self.buf.len() {
            panic!("read out of boundary");
        }

        return &self.buf[start..end];
    }
}

pub struct SmallWriter {
    buf: Vec<u8>,
    cap: usize,
}

impl SmallWriter {
    pub fn new(cap: usize) -> Self {
        let buf = Vec::new();
        Self { buf, cap }
    }

    pub fn write(&mut self, buf: &[u8]) {
        // boundary check
        if self.buf.len() + buf.len() > self.cap {
            panic!("write out of boundary");
        }

        self.buf.extend_from_slice(buf);
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        // boundary check
        if self.buf.len() > self.cap {
            panic!("write out of boundary");
        }

        let mut result = self.buf.clone();
        result.resize(self.cap, 0);
        result
    }
}
