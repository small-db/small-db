use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{Read, Seek, Write},
    mem::size_of,
    path::{Path, PathBuf},
    sync::{Arc, MutexGuard, RwLock},
};

use log::debug;

use crate::{
    btree::{
        page::{
            BTreeHeaderPage, BTreeInternalPage, BTreeLeafPage,
            BTreePage, BTreePageID, BTreeRootPointerPage,
            PageCategory,
        },
        page_cache::PageCache,
    },
    error::SmallError,
    io::{Decodeable, Encodeable, SmallFile, SmallReader},
    storage::schema::small_int_schema,
    transaction::Transaction,
    types::SmallResult,
    utils::HandyRwLock,
    Database,
};

static START_RECORD_LEN: u64 = 17;

/// see:
/// https://users.rust-lang.org/t/mapping-enum-u8/23400
///
/// TODO: add docs for `repr(u8)`
/// #[repr(u8)]
#[derive(Debug, PartialEq, Clone, Copy)]
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

impl Encodeable for RecordType {
    fn to_bytes(&self) -> Vec<u8> {
        vec![*self as u8]
    }
}

impl Decodeable for RecordType {
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

    /// no call to recover() and no append to log
    recovery_undecided: bool,

    file_path: PathBuf,
}

impl LogManager {
    /// Constructor.
    ///
    /// Initialize and back the log file with the specified file.
    ///
    /// We're not sure yet whether the caller is creating a brand new
    /// DB, in which case we should ignore the log file, or
    /// whether the caller will eventually want to recover (after
    /// populating the Catalog).
    ///
    /// So we make this decision lazily: if someone calls recover(),
    /// then do it, while if someone starts adding log file
    /// entries, then first throw out the initial log file
    /// contents.
    pub fn new<P: AsRef<Path> + Clone>(file_path: P) -> Self {
        Self {
            tx_start_position: HashMap::new(),
            file: SmallFile::new(file_path),
            current_offset: 0,
            total_records: 0,
            recovery_undecided: true,
            file_path: todo!(),
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

    /// Recover the database system by ensuring that the updates of
    /// committed transactions are installed and that the
    /// updates of uncommitted transactions are not installed.
    ///
    /// When the database system restarts after the crash, recovery
    /// proceeds in three phases:
    ///
    /// 1. The analysis phase identifies dirty pages in the page cache
    /// and transactions that were in progress at the time of a
    /// crash. Information about dirty pages is used to identify
    /// the starting point for the redo phase. A list of
    /// in-progress transactions is used during the undo phase to
    /// roll back incomplete transactions.
    ///
    /// 2. The redo phase repeats the history up to the point of a
    /// crash and restores the database to the previous state.
    /// This phase is done for incomplete transactions as well as
    /// ones that were committed but whose contents weren’t
    /// flushed to persistent storage.
    ///
    /// 3. The undo phase rolls back all incomplete transactions and
    /// restores the database to the last consistent state. All
    /// operations are rolled back in reverse chronological order.
    /// In case the database crashes again during recovery,
    /// operations that undo transactions are logged as well to
    /// avoid repeating them.
    pub fn recover(&mut self) -> SmallResult {
        self.recovery_undecided = false;

        // undo phase

        // get all incomplete transactions (transactions that have
        // started but not committed or aborted at the time of the
        // crash)
        let incomplete_transactions =
            self.get_incomplete_transactions()?;
        debug!(
            "incomplete transactions: {:?}",
            incomplete_transactions
        );

        self.get_file()
            .seek(std::io::SeekFrom::End(0))
            .or(Err(SmallError::new("io error")))?;

        debug!(
            "current offset: {}",
            self.file.get_current_position()?
        );

        while self.file.get_current_position()? >= START_RECORD_LEN {
            let word_size = size_of::<u64>() as i64;
            self.get_file()
                .seek(std::io::SeekFrom::Current(-word_size))
                .or(Err(SmallError::new("io error")))?;

            let record_start_pos = self.file.read::<u64>()?;
            self.file.seek(record_start_pos)?;
            // debug!("record start pos: {}", record_start_pos);
            let record_type = self.file.read::<RecordType>()?;

            match record_type {
                RecordType::START => {
                    // skip the transaction id
                    let _ = self.file.read::<u64>()?;

                    // skip the start position
                    let _ = self.file.read::<u64>()?;
                }
                RecordType::UPDATE => {
                    let tid = self.file.read::<u64>()?;

                    if incomplete_transactions.contains(&tid) {
                        // skip the page id
                        let pid = self.file.read::<BTreePageID>()?;

                        // skip the before page
                        let before_page = self.file.read_page()?;

                        // TODO: construct a new page from the before
                        // page
                        let catalog = Database::catalog();
                        let table_pod =
                            catalog.get_table(&pid.table_id).ok_or(
                                SmallError::new("table not found"),
                            )?;
                        let table = table_pod.rl();
                        table.write_page_to_disk(&pid, &before_page);

                        // skip the after page
                        let _ = self.file.read_page()?;

                        // skip the start position
                        let _ = self.file.read::<u64>()?;
                    } else {
                        // skip the page id
                        let _ = self.file.read::<BTreePageID>()?;

                        // skip the before page
                        let _ = self.file.read_page()?;

                        // skip the after page
                        let _ = self.file.read_page()?;

                        // skip the start position
                        let _ = self.file.read::<u64>()?;
                    }
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

            // in the end, seek to the start of the record
            self.file.seek(record_start_pos)?;
        }

        Ok(())
    }

    fn get_incomplete_transactions(
        &self,
    ) -> Result<HashSet<u64>, SmallError> {
        self.file.seek(0)?;
        let last_checkpoint_position = self.file.read::<u64>()?;

        let mut incomplete_transactions = HashSet::new();

        if last_checkpoint_position != NO_CHECKPOINT {
            self.show_log_contents();
            self.file.seek(last_checkpoint_position)?;

            // check the record type
            let record_type = self.file.read::<RecordType>()?;
            if record_type != RecordType::CHECKPOINT {
                return Err(SmallError::new(
                    "invalid checkpoint record type",
                ));
            }

            // skip the checkpoint id
            let _ = self.file.read::<i64>()?;

            // read the list of outstanding transactions
            let tx_count = self.file.read::<usize>()?;
            for _ in 0..tx_count {
                let tid = self.file.read::<u64>()?;
                incomplete_transactions.insert(tid);

                // skip the start position
                let _ = self.file.read::<u64>()?;
            }

            // skip the start position
            let _ = self.file.read::<u64>()?;
        }

        // step 5: read the log records, stop when we encounter the
        // EOF
        let file_size = self.file.get_size()?;
        while self.file.get_current_position()? < file_size {
            let record_type = self.file.read::<RecordType>()?;

            match record_type {
                RecordType::START => {
                    let tid = self.file.read::<u64>()?;
                    incomplete_transactions.insert(tid);

                    // skip the start position
                    let _ = self.file.read::<u64>()?;
                }
                RecordType::UPDATE => {
                    // skip the transaction id
                    let _ = self.file.read::<u64>()?;

                    // skip the page id
                    let _ = self.file.read::<BTreePageID>()?;

                    // skip the before page
                    let _ = self.file.read_page()?;

                    // skip the after page
                    let _ = self.file.read_page()?;

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

                    // skip the start position
                    let _ = self.file.read::<u64>()?;
                }
                RecordType::COMMIT => {
                    let tid = self.file.read::<u64>()?;
                    incomplete_transactions.remove(&tid);

                    // skip the start position
                    let _ = self.file.read::<u64>()?;
                }
                RecordType::ABORT => {
                    let tid = self.file.read::<u64>()?;
                    incomplete_transactions.remove(&tid);

                    self.show_log_contents();

                    // skip the start position
                    let _ = self.file.read::<u64>().unwrap();
                }
            }
        }

        Ok(incomplete_transactions)
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
        let cache = Database::mut_page_cache();

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

        self.get_file()
            .seek(std::io::SeekFrom::Start(checkpoint_end_position))
            .unwrap();

        // write start position of this record
        self.file.write(&checkpoint_start_position)?;
        self.current_offset = self.file.get_current_position()?;

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
            // page_cache.discard_page(pid)
            let hold_pages = Database::concurrent_status()
                .hold_pages
                .get_inner_rl();
            let pids = hold_pages.get(tx).unwrap();

            for pid in pids {
                page_cache.discard_page(pid);
            }

            return Ok(());
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
        self.file.seek(tx_start_position)?;

        // step 5: read the log records of the transaction, stop when
        // we encounter the EOF
        let file_size = self.file.get_size()?;
        while self.file.get_current_position()? < file_size {
            let record_type = self.file.read::<RecordType>()?;
            // debug!("record_type: {:?}", record_type);

            match record_type {
                RecordType::START => {
                    // skip the transaction id
                    let _ = self.file.read::<u64>()?;

                    // skip the start position
                    let _ = self.file.read::<u64>()?;
                }
                RecordType::UPDATE => {
                    let tid = self.file.read::<u64>()?;
                    if tid == tx.get_id() {
                        let pid = self.file.read::<BTreePageID>()?;

                        // skip the before page
                        let before_image = self.file.read_page()?;
                        self.recover_page(
                            &pid,
                            &before_image,
                            page_cache,
                        )?;

                        // skip the after page
                        let _ = self.read_page(&pid)?;

                        // skip the start position
                        let _ = self.file.read::<u64>()?;
                    } else {
                        // skip this record

                        // skip the page id
                        let _ = self.file.read::<BTreePageID>()?;

                        // skip the before page
                        let _ = self.file.read_page()?;

                        // skip the after page
                        let _ = self.file.read_page()?;

                        // skip the start position
                        let _ = self.file.read::<u64>()?;
                    }
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

        let after_data = page.get_page_data();
        self.file.write(&after_data.len())?;
        self.file.write(&after_data)?;

        return Ok(());
    }

    fn recover_page(
        &mut self,
        pid: &BTreePageID,
        before_image: &Vec<u8>,
        page_cache: &PageCache,
    ) -> SmallResult {
        let catalog = Database::catalog();
        let table_pod = catalog.get_table(&pid.table_id).unwrap();
        let table = table_pod.rl();

        let schema = table.get_tuple_scheme();
        let key_field = table.key_field;

        match pid.category {
            PageCategory::Leaf => {
                let page = BTreeLeafPage::new(
                    &pid,
                    &before_image,
                    &schema,
                    key_field,
                );
                page_cache.recover_page(
                    &pid,
                    page,
                    &page_cache.leaf_buffer,
                );
            }
            PageCategory::RootPointer => {
                let page = BTreeRootPointerPage::new(
                    &pid,
                    &before_image,
                    &schema,
                    key_field,
                );
                page_cache.recover_page(
                    &pid,
                    page,
                    &page_cache.root_pointer_buffer,
                );
            }
            PageCategory::Internal => {
                let page = BTreeInternalPage::new(
                    &pid,
                    &before_image,
                    &schema,
                    key_field,
                );
                page_cache.recover_page(
                    &pid,
                    page,
                    &page_cache.internal_buffer,
                );
            }
            PageCategory::Header => {
                let page = BTreeHeaderPage::new(&pid, &before_image);
                page_cache.recover_page(
                    &pid,
                    page,
                    &page_cache.header_buffer,
                );
            }
        }

        Ok(())
    }

    fn read_page(
        &mut self,
        pid: &BTreePageID,
    ) -> Result<Arc<RwLock<dyn BTreePage>>, SmallError> {
        // let pid = self.file.read::<BTreePageID>()?;

        let data = self.file.read_page()?;

        let catalog = Database::catalog();
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
            self.file.seek(0)?;
            self.file.write(&NO_CHECKPOINT)?;
            self.current_offset = self.file.get_current_position()?;
        }

        return Ok(());
    }

    pub fn show_log_contents(&self) {
        let original_offset =
            self.file.get_current_position().unwrap();

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
                        "│   └── [8 bytes] start position: {}\n",
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

        self.file.seek(original_offset).unwrap();
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
                let content = iter
                    .take(5)
                    .map(|x| x.get_cell(0).to_bytes())
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
