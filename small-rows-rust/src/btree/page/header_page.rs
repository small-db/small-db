use std::{
    cmp,
    io::Cursor,
    sync::{atomic::Ordering, Arc, RwLock},
};

use bit_vec::BitVec;

use super::{BTreePage, BTreePageID, BTreePageInit, PageCategory, PageIndex};
use crate::{
    btree::buffer_pool::BufferPool,
    io::{Serializeable, SmallWriter},
    storage::table_schema::TableSchema,
    transaction::{Permission, Transaction},
    utils::HandyRwLock,
    BTreeTable, Database,
};

/// # Binary Layout
///
/// - 4 bytes: page category
/// - 4 bytes: page id of the next header page
/// - n bytes: header
pub struct BTreeHeaderPage {
    pid: BTreePageID,

    next_pid: Option<BTreePageID>,

    // indicate slots' status: true means occupied, false means empty
    header: BitVec<u32>,

    slot_count: usize,

    old_data: Vec<u8>,
}

impl BTreeHeaderPage {
    pub fn new(pid: &BTreePageID, bytes: &[u8], table_schema: &TableSchema) -> Self {
        let mut instance: Self;

        let mut reader = Cursor::new(bytes);

        // read page category
        let page_category = PageCategory::decode(&mut reader, &());
        if page_category != PageCategory::Header {
            panic!("invalid page category: {:?}", page_category);
        }

        // read header
        let header = BitVec::decode(&mut reader, &());

        let slot_count = header.len();

        instance = BTreeHeaderPage {
            pid: pid.clone(),
            next_pid: None,
            header,
            slot_count,
            old_data: Vec::new(),
        };

        instance.set_before_image(table_schema);
        return instance;
    }

    fn set_next_pid(&mut self, pid: &BTreePageID) {
        self.next_pid = Some(pid.clone());
    }

    fn get_next_pid(&self) -> Option<BTreePageID> {
        self.next_pid.clone()
    }

    // mark the slot as empty/filled.
    pub(crate) fn mark_slot_status(&mut self, slot_index: usize, used: bool) {
        self.header.set(slot_index, used);
    }

    pub(crate) fn get_empty_slot(&self) -> Option<u32> {
        for i in 0..self.slot_count {
            if !self.header[i] {
                return Some(i as u32);
            }
        }
        None
    }

    pub(crate) fn calc_slots_count() -> usize {
        // extraBytes:
        // - 4 bytes: page category
        // - 4 bytes: page id of the next header page
        let extra_bytes = 4 + 4;
        (BufferPool::get_page_size() - extra_bytes) * 8
    }
}

impl BTreePageInit for BTreeHeaderPage {
    fn new_empty_page(pid: &BTreePageID, table_schema: &TableSchema) -> Self {
        let slot_count = Self::calc_slots_count();

        let mut header = BitVec::new();
        header.grow(slot_count, false);

        Self {
            pid: pid.clone(),
            next_pid: None,
            header,
            slot_count,
            old_data: Vec::new(),
        }
    }
}

impl BTreePage for BTreeHeaderPage {
    fn new(pid: &BTreePageID, bytes: &[u8], table_schema: &TableSchema) -> Self {
        Self::new(pid, bytes, table_schema)
    }

    fn get_pid(&self) -> BTreePageID {
        self.pid.clone()
    }

    fn get_page_data(&self, _table_schema: &TableSchema) -> Vec<u8> {
        let mut writer = SmallWriter::new_reserved(BufferPool::get_page_size());

        // write page category
        self.get_pid().category.encode(&mut writer, &());

        // write header
        self.header.encode(&mut writer, &());

        return writer.to_padded_bytes(BufferPool::get_page_size());
    }

    fn set_before_image(&mut self, table_schema: &TableSchema) {
        self.old_data = self.get_page_data(table_schema);
    }

    fn get_before_image(&self, _table_schema: &TableSchema) -> Vec<u8> {
        if self.old_data.is_empty() {
            panic!("before image is not set");
        }
        return self.old_data.clone();
    }
}

pub(crate) struct HeaderPages {
    header_pages: Vec<Arc<RwLock<BTreeHeaderPage>>>,
    tx: Transaction,
}

impl HeaderPages {
    pub(crate) fn new(table: &BTreeTable, tx: &Transaction) -> Self {
        let header_pid = BTreePageID::get_header_pid(table.get_id());

        let mut header_pages = Vec::new();
        let mut pid = header_pid;
        loop {
            let page = BufferPool::get_header_page(tx, Permission::ReadWrite, &pid).unwrap();
            header_pages.push(page.clone());

            let next_pid = page.rl().get_next_pid();
            if next_pid.is_none() {
                break;
            }
            pid = next_pid.unwrap();
        }

        Self {
            header_pages,
            tx: tx.clone(),
        }
    }

    /// Get the page id of the first header page.
    pub(crate) fn get_head_pid(&self) -> BTreePageID {
        self.header_pages[0].rl().get_pid()
    }

    pub(crate) fn init_header_pages(table: &BTreeTable, tx: &Transaction) -> Self {
        let mut header_pids = Vec::new();
        let slots_per_page = BTreeHeaderPage::calc_slots_count();

        let mut filled_slots = 0;
        loop {
            if filled_slots >= table.page_index.load(Ordering::Relaxed) as usize {
                break;
            }

            let page_index = table.page_index.fetch_add(1, Ordering::Relaxed) + 1;
            let page_id = BTreePageID::new(PageCategory::Header, table.get_id(), page_index);
            let mut page = BTreeHeaderPage::new_empty_page(&page_id, &table.schema);

            // Q: what if the process crashes before the writing finished?
            // A: I don't know.
            //
            // step 1: write the header pages to disk
            //
            // The disk write action should comes before the buffer pool read action.
            // Otherwise, the buffer pool read will fail.
            table.write_page_to_disk(&page_id, &page.get_page_data(&table.schema));

            let current_slots = cmp::min(
                slots_per_page,
                table.page_index.load(Ordering::Relaxed) as usize - filled_slots,
            );
            for i in 0..current_slots {
                page.mark_slot_status(i as usize, true);
            }

            filled_slots += current_slots;
            header_pids.push(page_id);

            let page_rc = Arc::new(RwLock::new(page));
            Database::mut_buffer_pool()
                .header_buffer
                .insert(page_id, page_rc.clone());
        }

        // step 2: get the header pages from the buffer pool
        let mut header_pages = Vec::new();
        for pid in header_pids.iter() {
            let page = BufferPool::get_header_page(tx, Permission::ReadWrite, pid).unwrap();
            header_pages.push(page);
        }

        // step 3: link the header pages
        for i in 0..header_pages.len() - 1 {
            let mut page = header_pages[i].wl();
            let next_pid = header_pages[i + 1].rl().get_pid();
            page.set_next_pid(&next_pid);
        }

        Self {
            header_pages,
            tx: tx.clone(),
        }
    }

    /// Get the page index of the first empty slot in the header pages.
    pub(crate) fn get_empty_page_index(&self) -> PageIndex {
        let slots_per_page = BTreeHeaderPage::calc_slots_count();

        for (i, page_rc) in self.header_pages.iter().enumerate() {
            let mut page = page_rc.wl();
            let empty_slot = page.get_empty_slot();
            if let Some(empty_slot) = empty_slot {
                page.mark_slot_status(empty_slot as usize, true);
                return empty_slot + (i * slots_per_page) as u32;
            }
        }

        panic!("no empty slot in the header pages");
    }

    pub(crate) fn mark_page(&self, pid: &BTreePageID, used: bool) {
        let header_index = pid.page_index / BTreeHeaderPage::calc_slots_count() as u32;
        let slot_index = pid.page_index as usize % BTreeHeaderPage::calc_slots_count();
        self.header_pages[header_index as usize]
            .wl()
            .mark_slot_status(slot_index, used);
    }

    pub(crate) fn release_latches(&self) {
        for page in self.header_pages.iter() {
            let pid = page.rl().get_pid();
            Database::mut_concurrent_status()
                .release_latch(&self.tx, &pid)
                .unwrap();
        }
    }
}
