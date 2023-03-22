use std::{
    convert::TryInto,
    fs::{File, OpenOptions},
    io::{Cursor, Read, Seek, Write},
    mem::size_of,
    path::Path,
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
        match self.get_file().write(&obj.encode()) {
            Ok(_) => Ok(()),
            Err(e) => Err(SmallError::new(&e.to_string())),
        }
    }

    pub fn read<T: Decodeable>(&self) -> Result<T, SmallError> {
        let mut bytes = vec![0u8; size_of::<T>()];
        self.get_file()
            .read_exact(&mut bytes)
            .or(Err(SmallError::new("io error")))?;
        let mut reader = Cursor::new(bytes);
        Ok(T::decode(&mut reader))
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

pub fn read_into<T: Decodeable, R: std::io::Read>(
    reader: &mut R,
) -> T {
    T::decode(reader)
}

pub fn read_exact<R: std::io::Read>(
    reader: &mut R,
    bytes_count: usize,
) -> Vec<u8> {
    let mut buffer = vec![0u8; bytes_count];
    reader
        .read_exact(&mut buffer)
        .expect(&format!("io error, expect {}", bytes_count));
    buffer
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
        self.buf.extend_from_slice(obj.encode().as_slice());
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.buf.clone()
    }

    pub fn size(&self) -> usize {
        self.buf.len()
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
    fn encode(&self) -> Vec<u8>;
}

pub trait Decodeable {
    fn decode<R: std::io::Read>(reader: &mut R) -> Self;
}

/// # Format
///
/// - 2 bytes: bytes size (range: 0 - 65535) (65535 * 8 = 524280 bits)
/// - n bytes: bit vector
impl Encodeable for BitVec {
    fn encode(&self) -> Vec<u8> {
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
    fn decode<R: std::io::Read>(reader: &mut R) -> Self {
        // read size
        // let buffer = [0u8; 2];
        // reader.read_exact(&mut buffer).unwrap();
        let size = u16::from_le_bytes(
            read_exact(reader, 2).try_into().unwrap(),
        );

        // read payload
        let buf = read_exact(reader, size as usize);

        BitVec::from_bytes(&buf)
    }
}

/// # Format
///
/// - 1 byte (0 for false, 1 for true)
impl Encodeable for bool {
    fn encode(&self) -> Vec<u8> {
        vec![*self as u8]
    }
}

impl Decodeable for bool {
    fn decode<R: std::io::Read>(reader: &mut R) -> Self {
        u8::decode(reader) == 1
    }
}

impl Decodeable for String {
    fn decode<R: std::io::Read>(reader: &mut R) -> Self {
        // read size
        let size = u8::from_le_bytes(
            read_exact(reader, 1).try_into().unwrap(),
        );

        // read payload
        let bytes = read_exact(reader, size as usize);
        String::from_utf8(bytes).unwrap()
    }
}

impl Encodeable for &[u8] {
    fn encode(&self) -> Vec<u8> {
        self.to_vec()
    }
}

// impl Encodeable for Vec<u8> {
//     fn encode(&self) -> Vec<u8> {
//         self.to_vec()
//     }
// }

// # Format

// - 1 byte: size of the string (range: 0 - 255)
// - n bytes: string
impl Encodeable for Vec<u8> {
    fn encode(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        // write size
        let len = self.len() as u8;
        buffer.extend_from_slice(&len.to_le_bytes());

        // write payload
        buffer.extend_from_slice(&self);

        buffer
    }
}

macro_rules! impl_serialization {
    (for $($t:ty),+) => {
        $(
            impl Encodeable for $t {
                fn encode(&self) -> Vec<u8> {
                    self.to_le_bytes().to_vec()
                }
            }

            impl Decodeable for $t {
                fn decode<R: std::io::Read>(reader: &mut R) -> Self {
                    let bytes = read_exact(reader, size_of::<Self>());
                    Self::from_le_bytes(bytes.try_into().unwrap())
                }
            }
        )*
    }
}

impl_serialization!(for u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, isize, usize, f32, f64);
