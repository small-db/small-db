use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, Write},
    sync::{Arc, MutexGuard, RwLock},
};

use log::{debug, error};

use crate::{
    btree::{
        page::{
            BTreeHeaderPage, BTreeInternalPage, BTreeLeafPage,
            BTreePage, BTreePageID, BTreeRootPointerPage,
            PageCategory,
        },
        page_cache::PageCache,
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
#[derive(Debug, PartialEq)]
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
static NO_CHECKPOINT: u64 = 0;

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
    pub fn log_abort(
        &mut self,
        tx: &Transaction,
        page_cache: &PageCache,
    ) -> SmallResult {
        // must have page cache lock before proceeding, since this
        // calls rollback let cache =
        // Unique::mut_page_cache();

        self.rollback(tx, page_cache)?;

        self.file.write(&RecordType::ABORT)?;
        self.file.write(&tx.get_id())?;
        self.file.write(&self.current_offset)?;

        self.current_offset = self.file.get_current_position()?;
        self.tx_start_position.remove(tx);
        Ok(())
    }

    /// Write an UPDATE record to disk for the specified tid and page
    /// (with provided before and after images.)
    pub fn log_update<PAGE: BTreePage>(
        &mut self,
        tx: &Transaction,
        page_pod: Arc<RwLock<PAGE>>,
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
        self.write_page(page_pod)?;
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
        self.file.write(&NO_CHECKPOINT)?;

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
        // TODO: why write self.current_offset instead of
        // checkpoint_end_position?
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
    fn rollback(
        &mut self,
        tx: &Transaction,
        page_cache: &PageCache,
    ) -> SmallResult {
        // step 1: get the position of last checkpoint
        // TODO: what if there is no checkpoint?
        self.file.seek(0)?;
        let last_checkpoint_position = self.file.read::<u64>()?;
        if last_checkpoint_position == NO_CHECKPOINT {
            panic!("no checkpoint found");
        }

        // step 2: seek to the start position of the checkpoint
        self.file.seek(last_checkpoint_position)?;

        // step 3: read checkpoint, get the position of the specific
        // tx
        let record_type = self.file.read::<RecordType>()?;
        if record_type != RecordType::CHECKPOINT {
            panic!("invalid checkpoint");
        }
        // checkpoint id
        let _ = self.file.read::<i64>().unwrap();
        // read list of outstanding(active) transactions
        let tx_count = self.file.read::<usize>()?;
        let mut tx_start_position = 0;
        for _ in 0..tx_count {
            let tx_id = self.file.read::<u64>()?;
            if tx_id == tx.get_id() {
                tx_start_position = self.file.read::<u64>()?;
                break;
            } else {
                // skip the start position
                let _ = self.file.read::<u64>()?;
            }
        }
        if tx_start_position == 0 {
            panic!("no such transaction");
        }

        // step 4: seek to the start position of the transaction
        self.show_log_contents();
        self.file.seek(tx_start_position)?;

        // step 5: read the log records of the transaction, stop when
        // we encounter the EOF
        let file_size = self.file.get_size()?;
        while self.file.get_current_position()? < file_size {
            let record_type = self.file.read::<RecordType>()?;
            debug!("record_type: {:?}", record_type);

            match record_type {
                RecordType::START => {
                    // skip the transaction id
                    let _ = self.file.read::<u64>()?;

                    // skip the start position
                    let _ = self.file.read::<u64>()?;
                }
                RecordType::UPDATE => {
                    // skip the transaction id
                    let _ = self.file.read::<u64>()?;

                    let pid = self.file.read::<BTreePageID>()?;
                    page_cache.discard_page(&pid);

                    // skip the before page
                    self.recover_page(&pid, page_cache)?;
                    // let before_page = self.read_page(&pid)?;
                    // page_cache.insert_page(&before_page);

                    // skip the after page
                    let _ = self.read_page(&pid)?;

                    // skip the start position
                    let _ = self.file.read::<u64>()?;
                }
                RecordType::CHECKPOINT => {
                    // skip the checkpoint id
                    let _ = self.file.read::<i64>()?;

                    // skip the list of outstanding transactions
                    let tx_count = self.file.read::<usize>()?;
                    for _ in 0..tx_count {
                        // skip the transaction id
                        let _ = self.file.read::<u64>()?;

                        // skip the start position
                        let _ = self.file.read::<u64>()?;
                    }

                    // skip the current offset
                    let _ = self.file.read::<u64>()?;
                }
                RecordType::COMMIT => {
                    // skip the transaction id
                    let _ = self.file.read::<u64>()?;

                    // skip the start position
                    let _ = self.file.read::<u64>()?;
                }
                RecordType::ABORT => {
                    // skip the transaction id
                    let _ = self.file.read::<u64>()?;

                    // skip the start position
                    let _ = self.file.read::<u64>()?;
                }
            }
        }

        return Ok(());
    }

    fn write_page<PAGE: BTreePage>(
        &mut self,
        page_pod: Arc<RwLock<PAGE>>,
    ) -> SmallResult {
        let page = page_pod.read().unwrap();
        self.file.write(&page.get_pid())?;

        let before_data = page.get_before_image();
        self.file.write(&before_data.len())?;
        self.file.write(&before_data)?;

        // let page = page_pod.read().unwrap();
        // self.file.write(&page.get_pid())?;

        let after_data = page.get_page_data();
        self.file.write(&after_data.len())?;
        self.file.write(&after_data)?;

        return Ok(());
    }

    fn recover_page(
        &mut self,
        pid: &BTreePageID,
        page_cache: &PageCache,
    ) -> SmallResult {
        let data = self.file.read_page()?;

        let catalog = Unique::catalog();
        let table_pod = catalog.get_table(&pid.table_id).unwrap();
        let table = table_pod.rl();

        let schema = table.get_tuple_scheme();
        let key_field = table.key_field;

        match pid.category {
            PageCategory::Leaf => {
                let page = BTreeLeafPage::new(
                    &pid, &data, &schema, key_field,
                );
                // page_cache.insert_page(&Arc::new(RwLock::new(page)));
                page_cache.recover_page(&pid);
                todo!()
            }
            _ => {
                todo!()
            } // PageCategory::RootPointer => {
              //     let page = BTreeRootPointerPage::new(
              //         &pid, &data, &schema, key_field,
              //     );
              //     return Ok(Arc::new(RwLock::new(page)));
              // }
              // PageCategory::Internal => {
              //     let page = BTreeInternalPage::new(
              //         &pid, &data, &schema, key_field,
              //     );
              //     return Ok(Arc::new(RwLock::new(page)));
              // }
              // PageCategory::Header => {
              //     let page = BTreeHeaderPage::new(&pid, &data);
              //     return Ok(Arc::new(RwLock::new(page)));
              // }
        }
    }

    fn read_page(
        &mut self,
        pid: &BTreePageID,
    ) -> Result<Arc<RwLock<dyn BTreePage>>, SmallError> {
        // let pid = self.file.read::<BTreePageID>()?;

        let data = self.file.read_page()?;

        let catalog = Unique::catalog();
        let table_pod = catalog.get_table(&pid.table_id).unwrap();
        let table = table_pod.rl();

        let schema = table.get_tuple_scheme();
        let key_field = table.key_field;

        match pid.category {
            PageCategory::Leaf => {
                let page = BTreeLeafPage::new(
                    &pid, &data, &schema, key_field,
                );
                return Ok(Arc::new(RwLock::new(page)));
            }
            PageCategory::RootPointer => {
                let page = BTreeRootPointerPage::new(
                    &pid, &data, &schema, key_field,
                );
                return Ok(Arc::new(RwLock::new(page)));
            }
            PageCategory::Internal => {
                let page = BTreeInternalPage::new(
                    &pid, &data, &schema, key_field,
                );
                return Ok(Arc::new(RwLock::new(page)));
            }
            PageCategory::Header => {
                let page = BTreeHeaderPage::new(&pid, &data);
                return Ok(Arc::new(RwLock::new(page)));
            }
        }
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
            self.file.write(&NO_CHECKPOINT)?;
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

        let last_checkpoint = self.file.read::<u64>().unwrap();

        if last_checkpoint != NO_CHECKPOINT {
            depiction.push_str(&format!(
                "├── [8 bytes] last checkpoint: {}\n",
                last_checkpoint,
            ));
        } else {
            depiction
                .push_str(&format!("├── [8 bytes] no checkpoint\n",));
        }

        let mut offset = 0;
        let mut record_id = -1;
        while offset < self.current_offset {
            record_id += 1;

            offset = self.file.get_current_position().unwrap();

            let record_type: RecordType;

            if let Ok(byte) = self.file.read() {
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
                "├── {:?}-[pos {}]-[record {}]\n",
                record_type, offset, record_id,
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

                    let pid =
                        self.file.read::<BTreePageID>().unwrap();
                    depiction.push_str(&format!(
                        "│   ├── [8 bytes] pid: {:?}\n",
                        pid,
                    ));

                    let before_page = self.file.read_page().unwrap();
                    depiction.push_str(&format!(
                        "│   ├── [{} bytes] before page: {}\n",
                        before_page.len(),
                        self.parsed_page_content(&before_page),
                    ));

                    let after_page = self.file.read_page().unwrap();
                    depiction.push_str(&format!(
                        "│   ├── [{} bytes] after page: {}\n",
                        after_page.len(),
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

                    let checkpoint_id =
                        self.file.read::<i64>().unwrap();
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
                    "{:?}, content: {:?}...",
                    page_category, content,
                );
            }
            _ => {
                return format!("{:?}", &bytes[0..16],);
            }
        }
    }
}
