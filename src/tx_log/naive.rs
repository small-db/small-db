use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, Write},
    sync::{Arc, MutexGuard, RwLock},
};

use log::debug;

use crate::{
    btree::page::BTreePage,
    error::SmallError,
    io::{Condensable, SmallFile, SmallReader, Vaporizable},
    transaction::Transaction,
    types::SmallResult,
    Unique,
};

static START_RECORD_LEN: u64 = 17;

/// see:
/// https://users.rust-lang.org/t/mapping-enum-u8/23400
///
/// TODO: add docs for `repr(u8)`
/// #[repr(u8)]
#[derive(Debug)]
enum RecordType {
    ABORT,
    COMMIT,
    UPDATE,
    START,
    CHECKPOINT,
}

impl RecordType {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => RecordType::ABORT,
            1 => RecordType::COMMIT,
            2 => RecordType::UPDATE,
            3 => RecordType::START,
            4 => RecordType::CHECKPOINT,
            _ => panic!("invalid record type: {}", value),
        }
    }
}

impl Condensable for RecordType {
    fn to_bytes(&self) -> Vec<u8> {
        vec![*self as u8]
    }
}

impl Vaporizable for RecordType {
    fn read_from(reader: &mut SmallReader) -> Self {
        let value = reader.read_exact(1);
        RecordType::from_u8(value[0])
    }
}

/// Migrated from java version.
///
/// TODO: Figure out what this is used for, and if it's needed.
static NO_CHECKPOINT_ID: i64 = -1;

pub struct LogManager {
    /// Record the start position of each transaction.
    ///
    /// The position is the byte position of the last byte of
    /// BEGIN_RECORD. (Why?)
    tx_start_position: HashMap<Transaction, u64>,

    file: SmallFile,

    /// The absolute position of the file descriptor cursor.
    current_offset: u64,

    /// Migrated from java version.
    ///
    /// TODO: Figure out what this is used for, and if it's needed.
    total_records: usize,

    /// Migrated from java version.
    ///
    /// no call to recover() and no append to log
    ///
    /// TODO: Figure out what this is used for, and if it's needed.
    recovery_undecided: bool,

    file_path: String,
}

impl LogManager {
    pub fn new(file_path: &str) -> Self {
        Self {
            tx_start_position: HashMap::new(),
            file: SmallFile::new(file_path),
            current_offset: 0,
            total_records: 0,
            recovery_undecided: true,
            file_path: file_path.to_string(),
        }
    }

    pub fn reset(&mut self) {
        self.file = SmallFile::new(&self.file_path);
        self.tx_start_position.clear();
        self.current_offset = 0;
        self.total_records = 0;
        self.recovery_undecided = true;
    }

    pub fn records_count(&self) -> usize {
        self.total_records
    }

    fn get_file(&self) -> MutexGuard<'_, File> {
        self.file.get_file()
    }

    pub fn log_start(&mut self, tx: &Transaction) -> SmallResult {
        self.pre_append()?;

        self.file.write_u8(RecordType::START as u8)?;
        self.file.write_u64(tx.get_id())?;
        self.file.write_u64(self.current_offset)?;

        self.tx_start_position.insert(*tx, self.current_offset);
        let current_offset = self
            .get_file()
            .seek(std::io::SeekFrom::Current(0))
            .unwrap();
        self.current_offset = current_offset;

        Ok(())
    }

    // /// Write an UPDATE record to disk for the specified tid and
    // page /// (with provided         before and after images.)
    // pub fn log_update(&mut self, _tx: &Transaction) -> SmallResult
    // {     todo!()
    // }

    // pub fn log_abort(&mut self, tx: &Transaction) -> SmallResult {
    // }

    /// Write an abort record to the log for the specified tid, force
    /// the log to disk, and perform a rollback
    pub fn log_abort(&mut self, tx: &Transaction) -> SmallResult {
        self.rollback(tx)?;

        self.file.write_u8(RecordType::START as u8)?;
        self.file.write_u64(tx.get_id())?;
        self.file.write_u64(self.current_offset)?;

        let current_offset = self
            .get_file()
            .seek(std::io::SeekFrom::Current(0))
            .unwrap();
        self.current_offset = current_offset;

        self.tx_start_position.remove(tx);

        Ok(())
    }

    /// Write an UPDATE record to disk for the specified tid and page
    /// (with provided before and after images.)
    pub fn log_update(
        &mut self,
        tx: &Transaction,
        before: &[u8],
        after: &[u8],
    ) -> SmallResult {
        self.pre_append()?;

        // update record conists of
        // record type
        // transaction id
        // before page data (see writePageData)
        // after page data
        // start offset
        // 4 + 8 + before page + after page + 8

        self.file.write_u8(RecordType::UPDATE as u8)?;
        self.file.write_u64(tx.get_id())?;
        self.file.write_bytes(before)?;
        self.file.write_bytes(after)?;
        self.file.write_u64(self.current_offset)?;

        let current_offset = self
            .get_file()
            .seek(std::io::SeekFrom::Current(0))
            .unwrap();
        self.current_offset = current_offset;

        return Ok(());
    }

    pub fn log_checkpoint(&mut self) -> SmallResult {
        self.pre_append()?;

        self.get_file().flush().unwrap();

        Unique::buffer_pool().flush_all_pages();

        self.file.write(&RecordType::CHECKPOINT)?;

        // no tid , but leave space for convenience
        //
        // TODO: Figure out what this is used for, and if it's needed.
        self.file.write(&NO_CHECKPOINT_ID)?;

        // write list of outstanding transactions
        self.file.write(&self.tx_start_position.len())?;
        for (tx, start_position) in &self.tx_start_position {
            self.file.write(&tx.get_id())?;
            self.file.write(start_position)?;
        }

        // once the CP is written, make sure the CP location at the
        // beginning of the log file is updated
        let checkpoint_end_position =
            self.file.get_current_position()?;
        self.get_file().seek(std::io::SeekFrom::Start(0)).unwrap();
        self.file.write(&checkpoint_end_position)?;

        // TODO: Figure out what this is used for, and if it's needed.
        self.get_file()
            .seek(std::io::SeekFrom::Start(checkpoint_end_position))
            .unwrap();
        self.file.write(&checkpoint_end_position)?;

        self.current_offset = self.file.get_current_position()?;

        return Ok(());
    }

    /// Rollback the specified transaction, setting the state of any
    /// of pages it updated to their pre-updated state.
    ///
    /// To preserve transaction semantics, this should not be called
    /// on transactions that have already committed (though this
    /// may not be enforced by this method).
    fn rollback(&mut self, tx: &Transaction) -> SmallResult {
        // Unique::buffer_pool().tx_complete(tx, false);
        return Ok(());

        todo!();

        let start = *self.tx_start_position.get(tx).unwrap();
        // seek to the start position of the transaction, skip the
        // START_RECORD
        let offset = self
            .get_file()
            .seek(std::io::SeekFrom::Start(start + START_RECORD_LEN))
            .unwrap();

        let file_size = self.file.get_size()?;

        debug!(
            "start: {}, offset: {}, file_size: {}",
            start, offset, file_size
        );

        let record_type = RecordType::from_u8(self.file.read_u8()?);
        debug!("record_type: {:?}", record_type);

        match record_type {
            RecordType::UPDATE => {
                let before_page_rc = self.read_page().unwrap();
                let before_page = before_page_rc.read().unwrap();
                Unique::buffer_pool()
                    .discard_page(&before_page.get_pid());

                todo!()
            }
            _ => panic!("invalid record type"),
        }

        todo!()
    }

    fn read_page(
        &mut self,
    ) -> Result<Arc<RwLock<dyn BTreePage>>, SmallError> {
        todo!()
    }

    // We're about to append a log record. If we weren't sure whether
    // the DB wants to do recovery, we're sure now -- it didn't.
    // So truncate the log.
    fn pre_append(&mut self) -> SmallResult {
        self.total_records += 1;

        if self.recovery_undecided {
            self.recovery_undecided = false;
            self.get_file()
                .set_len(0)
                .or(Err(SmallError::new("set_len failed")))?;
            self.get_file()
                .seek(std::io::SeekFrom::Start(0))
                .or(Err(SmallError::new("seek failed")))?;
            self.file.write_i64(NO_CHECKPOINT_ID)?;
            self.get_file()
                .seek(std::io::SeekFrom::End(0))
                .or(Err(SmallError::new("seek failed")))?;
            let new_offset = self
                .get_file()
                .seek(std::io::SeekFrom::Current(0))
                .or(Err(SmallError::new("seek failed")))?;
            self.current_offset = new_offset;
        }

        return Ok(());
    }

    pub fn show_log_contents(&self) {
        let mut depiction = String::new();

        {
            let mut file = self.get_file();
            file.seek(std::io::SeekFrom::Start(0)).unwrap();
        }

        let last_checkpoint = self.file.read_i64().unwrap();

        if last_checkpoint != NO_CHECKPOINT_ID {
            depiction.push_str(&format!(
                "├── [8 bytes] last checkpoint: {}\n",
                last_checkpoint,
            ));
        } else {
            depiction
                .push_str(&format!("├── [8 bytes] no checkpoint\n",));
        }

        let offset = 0;
        let mut record_id = -1;
        while offset < self.current_offset {
            record_id += 1;

            // if self.file.reach_end() {
            //     break;
            // }

            let record_type: RecordType;

            if let Ok(byte) = self.file.read_u8() {
                match byte {
                    0..=4 => {
                        record_type = RecordType::from_u8(byte);
                    }
                    _ => {
                        debug!("invalid record type: {}", byte);
                        break;
                    }
                }
            } else {
                break;
            }

            // let record_type =
            //     RecordType::from_u8(self.file.read_u8().unwrap());

            depiction.push_str(&format!(
                "├── [record {}]-{:?}\n",
                record_id, record_type,
            ));

            match record_type {
                RecordType::START => {
                    depiction.push_str(&format!(
                        "│   ├── [1 byte] record type: {:?}\n",
                        record_type,
                    ));

                    let tid = self.file.read_u64().unwrap();
                    depiction.push_str(&format!(
                        "│   ├── [8 bytes] tid: {}\n",
                        tid,
                    ));

                    let start_offset = self.file.read_u64().unwrap();
                    depiction.push_str(&format!(
                        "│   └── [8 bytes] start offset: {}\n",
                        start_offset,
                    ));
                }
                RecordType::UPDATE => {
                    depiction.push_str(&format!(
                        "│   ├── [1 byte] record type: {:?}\n",
                        record_type,
                    ));

                    let tid = self.file.read_u64().unwrap();
                    depiction.push_str(&format!(
                        "│   ├── [8 bytes] tid: {}\n",
                        tid,
                    ));

                    let before_page = self.file.read_page().unwrap();
                    depiction.push_str(&format!(
                        "│   ├── [{} bytes] before page: {:?}\n",
                        before_page.len(),
                        &before_page[0..16],
                    ));

                    let after_page = self.file.read_page().unwrap();
                    depiction.push_str(&format!(
                        "│   ├── [{} bytes] after page: {:?}\n",
                        after_page.len(),
                        &after_page[0..16],
                    ));

                    let start_offset = self.file.read_u64().unwrap();
                    depiction.push_str(&format!(
                        "│   └── [8 bytes] start offset: {}\n",
                        start_offset,
                    ));
                }
                RecordType::ABORT => {
                    depiction.push_str(&format!(
                        "│   ├── [1 byte] record type: {:?}\n",
                        record_type,
                    ));

                    let tid = self.file.read_u64().unwrap();
                    depiction.push_str(&format!(
                        "│   ├── [8 bytes] tid: {}\n",
                        tid,
                    ));

                    let start_offset = self.file.read_u64().unwrap();
                    depiction.push_str(&format!(
                        "│   └── [8 bytes] start offset: {}\n",
                        start_offset,
                    ));
                }
                RecordType::CHECKPOINT => {
                    depiction.push_str(&format!(
                        "│   ├── [1 byte] record type: {:?}\n",
                        record_type,
                    ));

                    let checkpoint_id = self.file.read_i64().unwrap();
                    depiction.push_str(&format!(
                        "│   ├── [8 bytes] checkpoint id: {}\n",
                        checkpoint_id,
                    ));

                    let start_offset = self.file.read_u64().unwrap();
                    depiction.push_str(&format!(
                        "│   └── [8 bytes] start offset: {}\n",
                        start_offset,
                    ));
                }
                _ => {
                    debug!("invalid record type: {:?}", record_type);
                    break;
                }
            }
        }

        debug!("log content: \n{}", depiction);
    }
}
