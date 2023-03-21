use std::{
    convert::TryInto,
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    mem::size_of,
    path::Path,
    sync::{Mutex, MutexGuard},
};

use bit_vec::BitVec;
use log::error;

use crate::{
    btree::page::BTreePage, error::SmallError, types::SmallResult,
};

pub struct SmallFile {
    file: Mutex<File>,
}

impl SmallFile {
    pub fn new<P: AsRef<Path>>(file_path: P) -> Self {
        let f = Mutex::new(
            OpenOptions::new()
                .write(true)
                .read(true)
                .create(true)
                .open(file_path)
                .unwrap(),
        );

        Self { file: f }
    }

    pub fn get_file(&self) -> MutexGuard<'_, File> {
        self.file.lock().unwrap()
    }

    pub fn read_page(&self) -> Result<Vec<u8>, SmallError> {
        let page_size = self.read::<usize>()?;

        let mut buf: Vec<u8> = vec![0; page_size];
        self.get_file()
            .read_exact(&mut buf)
            .or(Err(SmallError::new("io error")))?;
        Ok(buf)
    }

    pub fn write<T: Encodeable>(&self, obj: &T) -> SmallResult {
        match self.get_file().write(&obj.to_bytes()) {
            Ok(_) => Ok(()),
            Err(e) => Err(SmallError::new(&e.to_string())),
        }
    }

    pub fn read<T: Decodeable>(&self) -> Result<T, SmallError> {
        let mut buf = vec![0u8; size_of::<T>()];
        self.get_file()
            .read_exact(&mut buf)
            .or(Err(SmallError::new("io error")))?;
        let mut reader = SmallReader::new(&buf);
        Ok(T::read_from(&mut reader))
    }

    pub fn get_size(&self) -> Result<u64, SmallError> {
        let metadata = self
            .get_file()
            .metadata()
            .or(Err(SmallError::new("io error")))?;
        Ok(metadata.len())
    }

    pub fn get_current_position(&self) -> Result<u64, SmallError> {
        let offset = self
            .get_file()
            .seek(std::io::SeekFrom::Current(0))
            .or(Err(SmallError::new("io error")))?;
        Ok(offset)
    }

    pub fn seek(&self, offset: u64) -> SmallResult {
        self.get_file()
            .seek(std::io::SeekFrom::Start(offset))
            .or(Err(SmallError::new("io error")))?;
        Ok(())
    }
}

pub struct SmallReader<'a> {
    buf: &'a [u8],
    cursor: usize,
}

impl<'a> SmallReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, cursor: 0 }
    }

    // TODO: remove this api
    pub fn read_exact(&mut self, bytes_count: usize) -> &'_ [u8] {
        let start = self.cursor;
        let end = self.cursor + bytes_count;

        // boundary check
        if end > self.buf.len() {
            error!(
                "read out of boundary: {} > {}",
                end,
                self.buf.len()
            );
            panic!("read out of boundary");
        }

        self.cursor += bytes_count;

        return &self.buf[start..end];
    }

    pub fn read<T: Decodeable>(&mut self) -> T {
        T::read_from(self)
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

    pub fn write<T: Encodeable>(&mut self, obj: &T) {
        self.buf.extend_from_slice(obj.to_bytes().as_slice());
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.buf.clone()
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

pub trait Encodeable {
    fn to_bytes(&self) -> Vec<u8>;
}

pub trait Decodeable {
    fn read_from(reader: &mut SmallReader) -> Self;

    fn from_bytes(buf: Vec<u8>) -> Self
    where
        Self: Sized,
    {
        let mut reader = SmallReader::new(&buf);
        Self::read_from(&mut reader)
    }
}

/// # Format
///
/// - 2 bytes: bytes size (range: 0 - 65535) (65535 * 8 = 524280 bits)
/// - n bytes: bit vector
impl Encodeable for BitVec {
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

impl Decodeable for BitVec {
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

/// # Format
///
/// - 1 byte (0 for false, 1 for true)
impl Encodeable for bool {
    fn to_bytes(&self) -> Vec<u8> {
        vec![*self as u8]
    }
}

impl Decodeable for bool {
    fn read_from(reader: &mut SmallReader) -> Self {
        reader.read::<u8>() == 1
    }
}

// # Format
//
// - 1 byte: size of the string (range: 0 - 255)
// - n bytes: string
impl Encodeable for String {
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        let payload = self.as_bytes();

        // write size
        let len = payload.len() as u8;
        buf.extend_from_slice(&len.to_le_bytes());

        // write payload
        buf.extend_from_slice(&payload);

        buf
    }
}

impl Decodeable for String {
    fn read_from(reader: &mut SmallReader) -> Self {
        // read size
        let size = u8::from_le_bytes(
            reader.read_exact(1).try_into().unwrap(),
        );

        // read payload
        let buf = reader.read_exact(size as usize);

        String::from_utf8(buf.to_vec()).unwrap()
    }
}

impl Encodeable for &[u8] {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_vec()
    }
}

impl Encodeable for Vec<u8> {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_vec()
    }
}

macro_rules! impl_serialization {
    (for $($t:ty),+) => {
        $(
            impl Encodeable for $t {
                fn to_bytes(&self) -> Vec<u8> {
                    self.to_le_bytes().to_vec()
                }
            }

            impl Decodeable for $t {
                fn read_from(reader: &mut SmallReader) -> Self {
                    let buf = reader.read_exact(size_of::<Self>());
                    Self::from_le_bytes(buf.try_into().unwrap())
                }
            }
        )*
    }
}

impl_serialization!(for u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, isize, usize, f32, f64);
