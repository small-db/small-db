use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, Write},
    sync::{Arc, MutexGuard, RwLock},
};

use log::{debug, error};

use crate::{
    btree::{
        page::{BTreeLeafPage, BTreePage, BTreePageID, PageCategory},
        tuple::small_int_schema,
    },
    error::SmallError,
    io::{Condensable, SmallFile, SmallReader, Vaporizable},
    transaction::Transaction,
    types::SmallResult,
    utils::HandyRwLock,
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

        self.file.write(&RecordType::START)?;
        self.file.write(&tx.get_id())?;
        self.file.write(&self.current_offset)?;

        self.tx_start_position.insert(*tx, self.current_offset);
        self.current_offset = self.file.get_current_position()?;

        Ok(())
    }

    /// Write an abort record to the log for the specified tid, force
    /// the log to disk, and perform a rollback
    pub fn log_abort(&mut self, tx: &Transaction) -> SmallResult {
        // must have page cache lock before proceeding, since this calls rollback
        // let cache = Unique::mut_page_cache();

        self.rollback(tx)?;

        self.file.write(&RecordType::ABORT)?;
        self.file.write(&tx.get_id())?;
        self.file.write(&self.current_offset)?;

        self.current_offset = self.file.get_current_position()?;
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

        self.file.write(&RecordType::UPDATE)?;
        self.file.write(&tx.get_id())?;
        self.file.write(&before)?;
        self.file.write(&after)?;
        self.file.write(&self.current_offset)?;

        let current_offset = self
            .get_file()
            .seek(std::io::SeekFrom::Current(0))
            .unwrap();
        self.current_offset = current_offset;

        return Ok(());
    }

    pub fn log_checkpoint(&mut self) -> SmallResult {
        // make sure we have buffer pool lock before proceeding
        let cache = Unique::mut_page_cache();

        self.pre_append()?;

        self.get_file().flush().unwrap();

        // Unique::mut_buffer_pool().flush_all_pages();
        // Unique::buffer_pool_pod().wl().flush_all_pages();
        cache.flush_all_pages(self);

        let checkpoint_start_position =
            self.file.get_current_position()?;

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

        let checkpoint_end_position =
            self.file.get_current_position()?;

        // once the CP is written, make sure the CP location at the
        // beginning of the log file is updated
        self.get_file().seek(std::io::SeekFrom::Start(0)).unwrap();
        self.file.write(&checkpoint_start_position)?;

        // TODO: Figure out what this is used for, and if it's needed.
        self.get_file()
            .seek(std::io::SeekFrom::Start(checkpoint_end_position))
            .unwrap();
        // TODO: why write self.current_offset instead of checkpoint_end_position?
        self.file.write(&self.current_offset)?;

        self.current_offset = checkpoint_end_position;

        return Ok(());
    }

    pub fn log_commit(&mut self, tx: &Transaction) -> SmallResult {
        self.pre_append()?;

        self.file.write(&RecordType::COMMIT)?;
        self.file.write(&tx.get_id())?;
        self.file.write(&self.current_offset)?;

        self.current_offset = self.file.get_current_position()?;
        self.tx_start_position.remove(tx);
        Ok(())
    }

    /// Rollback the specified transaction, setting the state of any
    /// of pages it updated to their pre-updated state.
    ///
    /// To preserve transaction semantics, this should not be called
    /// on transactions that have already committed (though this
    /// may not be enforced by this method).
    fn rollback(&mut self, tx: &Transaction) -> SmallResult {
        let start = *self.tx_start_position.get(tx).unwrap();
        // seek to the start position of the transaction, skip the
        // START_RECORD
        let offset = self
            .get_file()
            .seek(std::io::SeekFrom::Start(start + START_RECORD_LEN))
            .unwrap();

        let file_size = self.file.get_size()?;

        debug!(
            "start: {}, offset: {}, file_size: {}, tid: {}",
            start,
            offset,
            file_size,
            tx.get_id()
        );

        let record_type = RecordType::from_u8(self.file.read_u8()?);
        debug!("record_type: {:?}", record_type);

        self.show_log_contents();

        match record_type {
            RecordType::UPDATE => {
                let before_page_rc = self.read_page().unwrap();
                let before_page = before_page_rc.read().unwrap();
                Unique::mut_page_cache()
                    .discard_page(&before_page.get_pid());

                todo!()
            }
            _ => {
                error!("invalid record type: {:?}", record_type);
                panic!("invalid record type");
            }
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
            self.file.write(&NO_CHECKPOINT_ID)?;
            self.get_file()
                .seek(std::io::SeekFrom::End(0))
                .or(Err(SmallError::new("seek failed")))?;
            self.current_offset = self.file.get_current_position()?;
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

                    let tid = self.file.read::<u64>().unwrap();
                    depiction.push_str(&format!(
                        "│   ├── [8 bytes] tid: {}\n",
                        tid,
                    ));

                    let start_offset =
                        self.file.read::<u64>().unwrap();
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

                    let tid = self.file.read::<u64>().unwrap();
                    depiction.push_str(&format!(
                        "│   ├── [8 bytes] tid: {}\n",
                        tid,
                    ));

                    let before_page = self.file.read_page().unwrap();
                    depiction.push_str(&format!(
                        "│   ├── {}\n",
                        self.parsed_page_content(&before_page),
                    ));

                    let after_page = self.file.read_page().unwrap();
                    depiction.push_str(&format!(
                        "│   ├── {}\n",
                        self.parsed_page_content(&after_page),
                    ));

                    let start_offset =
                        self.file.read::<u64>().unwrap();
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

                    let tid = self.file.read::<u64>().unwrap();
                    depiction.push_str(&format!(
                        "│   ├── [8 bytes] tid: {}\n",
                        tid,
                    ));

                    let start_offset =
                        self.file.read::<u64>().unwrap();
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

                    // read list of outstanding(active) transactions
                    let tx_count: usize = self.file.read().unwrap();
                    depiction.push_str(&format!(
                        "│   ├── [{} bytes] active tx count: {}\n",
                        std::mem::size_of::<usize>(),
                        tx_count,
                    ));
                    for _ in 0..tx_count {
                        let tx_id: u64 = self.file.read().unwrap();
                        depiction.push_str(&format!(
                            "│   │   ├── [8 bytes] tx id: {}\n",
                            tx_id,
                        ));
                        let tx_start_offset: u64 =
                            self.file.read().unwrap();
                        depiction.push_str(&format!(
                            "│   │   └── [8 bytes] tx start offset: {}\n",
                            tx_start_offset,
                        ));
                    }

                    let checkpoint_end_position: u64 =
                        self.file.read().unwrap();
                    depiction.push_str(&format!(
                        "│   └── [8 bytes] weird position: {}\n",
                        checkpoint_end_position,
                    ));
                }
                RecordType::COMMIT => {
                    depiction.push_str(&format!(
                        "│   ├── [1 byte] record type: {:?}\n",
                        record_type,
                    ));

                    let tid = self.file.read::<u64>().unwrap();
                    depiction.push_str(&format!(
                        "│   ├── [8 bytes] tid: {}\n",
                        tid,
                    ));

                    let start_offset =
                        self.file.read::<u64>().unwrap();
                    depiction.push_str(&format!(
                        "│   └── [8 bytes] start offset: {}\n",
                        start_offset,
                    ));
                }
            }
        }

        debug!("log content: \n{}", depiction);
    }

    fn parsed_page_content(&self, bytes: &[u8]) -> String {
        let page_category =
            PageCategory::read_from(&mut SmallReader::new(&bytes));

        match page_category {
            PageCategory::Leaf => {
                // TODO: use real value for schema, key_field and pid
                let schema = small_int_schema(2, "");
                let key_field = 0;
                let pid = BTreePageID::new(page_category, 0, 0);

                let page = BTreeLeafPage::new(
                    &pid, bytes, &schema, key_field,
                );
                let iter = page.iter();
                // let content = iter.take(3).collect::<Vec<_>>();
                let content = iter
                    .take(3)
                    .map(|x| x.fields[0].to_string())
                    .collect::<Vec<_>>();

                return format!(
                    "[{} bytes] before page: {:?}, content: {:?}...",
                    bytes.len(),
                    page_category,
                    content,
                );
            }
            _ => {
                return format!(
                    "[{} bytes] before page: {:?}",
                    bytes.len(),
                    &bytes[0..16],
                );
            }
        }
    }
}
