use std::{
    convert::TryInto,
    fs::{File, OpenOptions},
    io::{Read, Write},
    sync::{Mutex, MutexGuard},
};

use bit_vec::BitVec;

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

        self.cursor += bytes_count;

        return &self.buf[start..end];
    }
}

pub struct SmallWriter {
    buf: Vec<u8>,
}

impl SmallWriter {
    pub fn new() -> Self {
        let buf = Vec::new();
        Self { buf }
    }

    pub fn write<T: Condensable>(&mut self, obj: &T) {
        self.buf.extend_from_slice(obj.to_bytes().as_slice());
    }

    pub fn to_padded_bytes(&self, size: usize) -> Vec<u8> {
        let mut buf = self.buf.clone();

        if buf.len() > size {
            panic!(
                "buffer size is larger than the given size: {} > {}",
                buf.len(),
                size
            );
        }

        buf.resize(size, 0);
        buf
    }
}

pub trait Condensable {
    fn to_bytes(&self) -> Vec<u8>;
}

pub trait Vaporizable {
    fn read_from(reader: &mut SmallReader) -> Self;
}

pub trait Serializable: Condensable + Vaporizable {}

/// # Format
///
/// - 2 bytes: bytes size (range: 0 - 65535) (65535 * 8 = 524280 bits)
/// - n bytes: bit vector
impl Condensable for BitVec {
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        let payload = self.to_bytes();

        // write size
        let len = payload.len() as u16;
        buf.extend_from_slice(&len.to_le_bytes());

        // write payload
        buf.extend_from_slice(&payload);

        buf
    }
}

impl Vaporizable for BitVec {
    fn read_from(reader: &mut SmallReader) -> Self {
        // read size
        let size = u16::from_le_bytes(
            reader.read_exact(2).try_into().unwrap(),
        );

        // read payload
        let buf = reader.read_exact(size as usize);

        BitVec::from_bytes(buf)
    }
}

impl Condensable for u32 {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl Vaporizable for u32 {
    fn read_from(reader: &mut SmallReader) -> Self {
        let buf = reader.read_exact(4);
        u32::from_le_bytes(buf.try_into().unwrap())
    }
}
