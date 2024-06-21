use small_db::{
    btree::{
        buffer_pool::BufferPool,
        page::{BTreeInternalPage, BTreeInternalPageIterator, BTreeLeafPage},
    },
    transaction::Permission,
    storage::tuple::{Cell, Tuple},
    transaction::Transaction,
    types::Pod,
    utils::HandyRwLock,
    BTreeTable, TableSchema,
};

pub fn leaf_records_cap() -> usize {
    let schema = TableSchema::small_int_schema(2);
    BTreeLeafPage::calc_children_cap(&schema)
}

pub fn internal_children_cap() -> usize {
    let schema = TableSchema::small_int_schema(2);
    BTreeInternalPage::get_children_cap(&schema)
}

pub fn get_internal_page(table: &BTreeTable, level: usize, index: usize) -> Pod<BTreeInternalPage> {
    let tx = Transaction::new();
    let root_pid = table.get_root_pid(&tx);
    let root_pod = BufferPool::get_internal_page(&tx, Permission::ReadOnly, &root_pid).unwrap();

    match level {
        0 => {
            tx.commit().unwrap();
            return root_pod;
        }
        1 => match index {
            0 => {
                let e = BTreeInternalPageIterator::new(&root_pod.rl())
                    .next()
                    .unwrap();
                let left_child_rc =
                    BufferPool::get_internal_page(&tx, Permission::ReadOnly, &e.get_left_child())
                        .unwrap();
                tx.commit().unwrap();
                return left_child_rc;
            }
            _ => {
                let e = BTreeInternalPageIterator::new(&root_pod.rl())
                    .skip(index - 1)
                    .next()
                    .unwrap();
                let left_child_rc =
                    BufferPool::get_internal_page(&tx, Permission::ReadOnly, &e.get_right_child())
                        .unwrap();
                tx.commit().unwrap();
                return left_child_rc;
            }
        },
        _ => todo!(),
    }
}

pub fn get_leaf_page(table: &BTreeTable, level: usize, index: usize) -> Pod<BTreeLeafPage> {
    match level {
        0 => {
            let tx = Transaction::new();
            let root_pid = table.get_root_pid(&tx);
            let root_pod = BufferPool::get_leaf_page(&tx, Permission::ReadOnly, &root_pid).unwrap();
            tx.commit().unwrap();
            return root_pod;
        }
        _ => {
            let internal_pod = get_internal_page(table, level - 1, index);
            let tx = Transaction::new();
            let e = BTreeInternalPageIterator::new(&internal_pod.rl())
                .next()
                .unwrap();
            let leaf_pod =
                BufferPool::get_leaf_page(&tx, Permission::ReadOnly, &e.get_left_child()).unwrap();
            tx.commit().unwrap();
            return leaf_pod;
        }
    }
}

pub fn new_int_tuples(value: i64, width: usize, tx: &Transaction) -> Tuple {
    let mut cells: Vec<Cell> = Vec::new();
    for _ in 0..width {
        cells.push(Cell::Int64(value));
    }
    Tuple::new(&cells, tx.get_id())
}
