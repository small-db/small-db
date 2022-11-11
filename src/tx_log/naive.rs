use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Seek, Write},
    sync::{Mutex, MutexGuard},
};

use crate::{error::SmallError, transaction::Transaction, types::SmallResult};

/// see:
/// https://users.rust-lang.org/t/mapping-enum-u8/23400
///
/// TODO: add docs for `repr(u8)`
#[repr(u8)]
enum RecordType {
    ABORT,
    COMMIT,
    UPDATE,
    START,
    CHECKPOINT,
}

pub struct LogManager {
    /// Record the start position of each transaction.
    ///
    /// The position is the byte position of the last byte of
    /// BEGIN_RECORD. (Why?)
    tx_start_position: HashMap<Transaction, u64>,

    file: SmallFile,

    /// The absolute position of the file descriptor cursor.
    current_offset: u64,
}

impl LogManager {
    pub fn new(file_path: &str) -> Self {
        Self {
            tx_start_position: HashMap::new(),
            file: SmallFile::new(file_path),
            current_offset: 0,
        }
    }

    // pub fn records_count(&self) -> usize {
    //     unimplemented!()
    // }

    fn get_file(&self) -> MutexGuard<'_, File> {
        self.file.get_file()
    }

    pub fn log_start(&mut self, tx: &Transaction) -> SmallResult {
        self.file.write_u8(RecordType::START as u8)?;
        self.file.write_u64(tx.get_id())?;
        self.file.write_u64(self.current_offset)?;

        self.tx_start_position.insert(*tx, self.current_offset);
        let current_offset =
            self.get_file().seek(std::io::SeekFrom::Current(0)).unwrap();
        self.current_offset = current_offset;

        Ok(())
    }

    /// Write an abort record to the log for the specified tid, force the log to
    /// disk, and perform a rollback
    pub fn log_abort(&mut self, tx: &Transaction) -> SmallResult {
        self.file.write_u8(RecordType::START as u8)?;
        self.file.write_u64(tx.get_id())?;
        self.file.write_u64(self.current_offset)?;

        let current_offset =
            self.get_file().seek(std::io::SeekFrom::Current(0)).unwrap();
        self.current_offset = current_offset;

        self.tx_start_position.remove(tx);

        Ok(())
    }

    /// Rollback the specified transaction, setting the state of any
    /// of pages it updated to their pre-updated state.  To preserve
    /// transaction semantics, this should not be called on
    /// transactions that have already committed (though this may not
    /// be enforced by this method.)
    fn rollback() {}
}

struct SmallFile {
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

    fn get_file(&self) -> MutexGuard<'_, File> {
        self.file.lock().unwrap()
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
