use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek},
    sync::{Arc, MutexGuard, RwLock},
};

use log::debug;

use crate::{
    btree::page::BTreePage, error::SmallError, io::SmallFile,
    transaction::Transaction, types::SmallResult, Unique,
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
            _ => panic!("invalid record type"),
        }
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
    /// no call to recover() and no append to log
    ///
    /// TODO: Figure out what this is used for, and if it's needed.
    recovery_undecided: bool,
}

impl LogManager {
    pub fn new(file_path: &str) -> Self {
        Self {
            tx_start_position: HashMap::new(),
            file: SmallFile::new(file_path),
            current_offset: 0,
            total_records: 0,
            recovery_undecided: true,
        }
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

    /// Write an UPDATE record to disk for the specified tid and page
    /// (with provided         before and after images.)
    pub fn log_update(&mut self, _tx: &Transaction) -> SmallResult {
        todo!()
    }

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

    /**
     * Write an UPDATE record to disk for the specified tid and page
     * (with provided         before and after images.)
     *
     * @param tid    The transaction performing the write
     * @param before The before image of the page
     * @param after  The after image of the page
     * @see simpledb.Page#getBeforeImage
     */
    // public synchronized void logWrite(TransactionId tid, Page before,
    //     Page after)

    pub fn log_write(
        &self,
        tx: &Transaction,
        before: &[u8],
        after: &[u8],
    ) {
        unimplemented!()
    }

    /// Rollback the specified transaction, setting the state of any
    /// of pages it updated to their pre-updated state.  To preserve
    /// transaction semantics, this should not be called on
    /// transactions that have already committed (though this may not
    /// be enforced by this method.)
    fn rollback(&mut self, tx: &Transaction) -> SmallResult {
        let start = self.tx_start_position.get(tx).unwrap();
        // seek to the start position of the transaction, skip the
        // START_RECORD
        self.get_file()
            .seek(std::io::SeekFrom::Start(*start + START_RECORD_LEN))
            .unwrap();

        let record_type =
            RecordType::from_u8(self.file.read_u8().unwrap());
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

    // We're about to append a log record. If we weren't sure whether the
    // DB wants to do recovery, we're sure now -- it didn't. So truncate
    // the log.
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

    // void preAppend() throws IOException {
    //     logger.debug("preAppend start, offsets = " + raf.getFilePointer());
    //     print();

    //     totalRecords++;
    //     if (recoveryUndecided) {
    //         recoveryUndecided = false;
    //         raf.seek(0);
    //         raf.setLength(0);
    //         raf.writeLong(NO_CHECKPOINT_ID);
    //         raf.seek(raf.length());
    //         currentOffset = raf.getFilePointer();
    //     }

    //     print();
    //     logger.debug("preAppend end, offsets = " + raf.getFilePointer());
    // }
}
