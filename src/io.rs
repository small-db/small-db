use std::{
    convert::TryInto,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    mem::size_of,
    path::Path,
};

use bit_vec::BitVec;

use crate::{btree::page::BTreePage, error::SmallError, types::SmallResult};

const MAX_BYTES_SIZE: usize = u16::MAX as usize;

pub struct SmallFile {
    file: File,
}

impl SmallFile {
    /// Create a new `SmallFile` from the given file path and open it
    /// with read and write mode.
    ///
    /// If the file doesn't exist, it will be created.
    pub fn new<P: AsRef<Path>>(file_path: P) -> Self {
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(file_path)
            .unwrap();

        Self { file }
    }

    pub fn write<T: Encodeable>(&mut self, obj: &T) -> SmallResult {
        let mut writer = SmallWriter::new();
        obj.encode(&mut writer);
        writer.write_to(&mut self.file);
        Ok(())

        // match self.file.write(&obj.to_bytes()) {
        //     Ok(_) => Ok(()),
        //     Err(e) => Err(SmallError::new(&e.to_string())),
        // }
    }

    pub fn get_size(&self) -> Result<u64, SmallError> {
        let metadata = self.file.metadata().or(Err(SmallError::new("io error")))?;
        Ok(metadata.len())
    }

    pub fn get_current_position(&mut self) -> Result<u64, SmallError> {
        let offset = self
            .file
            .seek(std::io::SeekFrom::Current(0))
            .or(Err(SmallError::new("io error")))?;
        Ok(offset)
    }

    pub fn set_len(&self, len: u64) -> SmallResult {
        self.file
            .set_len(len)
            .or(Err(SmallError::new("io error")))?;
        Ok(())
    }

    pub fn seek(&mut self, pos: SeekFrom) -> Result<u64, SmallError> {
        self.file.seek(pos).or(Err(SmallError::new("io error")))
    }

    pub fn flush(&mut self) -> SmallResult {
        self.file.flush().or(Err(SmallError::new("io error")))?;
        Ok(())
    }
}

impl std::io::Read for SmallFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

/// A wrapper for `std::io::Read` to read a `Decodeable` object.
///
/// The advantage of this wrapper is doesn't require explicit type
/// annotation when type inference is possible. This makes some code
/// more concise.
///
/// TODO: rename
pub fn read_into<T: Decodeable, R: std::io::Read>(reader: &mut R) -> T {
    T::decode_from(reader)
}

pub fn read_exact<R: std::io::Read>(reader: &mut R, bytes_count: usize) -> Vec<u8> {
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
    /// Create a new `SmallWriter` with an empty buffer.
    pub fn new() -> Self {
        let buf = Vec::new();
        Self { buf }
    }

    /// Create a new `SmallWriter` with a buffer of the given capacity.
    pub fn new_reserved(cap: usize) -> Self {
        let mut buf = Vec::new();
        buf.reserve(cap);
        Self { buf }
    }

    pub fn write<T: Encodeable>(&mut self, obj: &T) {
        obj.encode(self);
    }

    pub fn write_bytes(&mut self, obj: &[u8]) {
        self.buf.write_all(obj).unwrap();
    }

    // TODO: move instead of clone
    pub fn to_bytes(&self) -> Vec<u8> {
        self.buf.clone()
    }

    pub fn write_to(&self, w: &mut dyn std::io::Write) {
        w.write_all(&self.buf).unwrap();
    }

    /// Pad the buffer to the given size. Note that the writer is
    /// cleared after this operation.
    ///
    /// TODO: move instead of clone
    pub fn to_padded_bytes(&mut self, size: usize) -> Vec<u8> {
        // let mut buf = self.buf.clone();

        if self.buf.len() > size {
            panic!(
                "buffer size is larger than the given size: {} > {}",
                self.buf.len(),
                size
            );
        }

        self.buf.resize(size, 0);
        std::mem::take(&mut self.buf)
    }
}

pub trait Encodeable {
    fn encode(&self, writer: &mut SmallWriter);

    /// TODO: remove this api
    fn to_bytes(&self) -> Vec<u8> {
        let mut writer = SmallWriter::new();
        self.encode(&mut writer);
        writer.to_bytes()
    }
}

pub trait Decodeable {
    fn decode_from<R: std::io::Read>(reader: &mut R) -> Self;
}

impl Encodeable for BitVec {
    fn encode(&self, writer: &mut SmallWriter) {
        writer.write(&self.to_bytes());
    }
}

impl Decodeable for BitVec {
    fn decode_from<R: std::io::Read>(reader: &mut R) -> Self {
        let buffer: Vec<u8> = read_into(reader);
        BitVec::from_bytes(&buffer)
    }
}

/// # Format
///
/// - 1 byte (0 for false, 1 for true)
impl Encodeable for bool {
    fn encode(&self, writer: &mut SmallWriter) {
        writer.write(&(*self as u8));
    }
}

impl Decodeable for bool {
    fn decode_from<R: std::io::Read>(reader: &mut R) -> Self {
        u8::decode_from(reader) == 1
    }
}

impl Decodeable for String {
    fn decode_from<R: std::io::Read>(reader: &mut R) -> Self {
        // read size
        let size = u8::from_le_bytes(read_exact(reader, 1).try_into().unwrap());

        // read payload
        let bytes = read_exact(reader, size as usize);
        String::from_utf8(bytes).unwrap()
    }
}

/// # Format
/// - 2 byte: size of the string (range: 0 - 64 KB)
/// - n bytes: string
impl Encodeable for Vec<u8> {
    fn encode(&self, writer: &mut SmallWriter) {
        // write size
        let size = self.len() as u16;
        writer.write(&size);

        // write payload
        writer.write_bytes(self);
    }
}

impl Decodeable for Vec<u8> {
    fn decode_from<R: std::io::Read>(reader: &mut R) -> Self {
        // read size
        let size: u16 = read_into(reader);

        // read payload
        read_exact(reader, size as usize)
    }
}

macro_rules! impl_serialization {
    (for $($t:ty),+) => {
        $(
            impl Encodeable for $t {
                fn encode(&self, writer: &mut SmallWriter) {
                    writer.write_bytes(&self.to_le_bytes());
                }
            }

            impl Decodeable for $t {
                fn decode_from<R: std::io::Read>(reader: &mut R) -> Self {
                    let bytes = read_exact(reader, size_of::<Self>());
                    Self::from_le_bytes(bytes.try_into().unwrap())
                }
            }
        )*
    }
}

impl_serialization!(for u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, isize, usize, f32, f64);
