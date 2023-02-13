



use small_db::{
    btree::{
        page::{
            BTreeInternalPage, BTreeInternalPageIterator,
            BTreeLeafPage, BTreePage,
        },
    },
    concurrent_status::Permission,
    transaction::Transaction,
    types::Pod,
    utils::{small_int_schema, HandyRwLock},
    *,
};

pub fn leaf_records_cap() -> usize {
    let scheme = small_int_schema(2, "");
    BTreeLeafPage::calculate_slots_count(&scheme)
}

pub fn internal_children_cap() -> usize {
    BTreeInternalPage::get_children_cap(4) + 1
}

pub fn get_internal_page(
    table: &BTreeTable,
    level: usize,
    index: usize,
) -> Pod<BTreeInternalPage> {
    let tx = Transaction::new();
    let root_pid = table.get_root_pid(&tx);
    let root_pod = Database::mut_page_cache()
        .get_internal_page(&tx, Permission::ReadOnly, &root_pid)
        .unwrap();

    match level {
        0 => {
            tx.commit().unwrap();
            return root_pod;
        }
        1 => match index {
            0 => {
                let e =
                    BTreeInternalPageIterator::new(&root_pod.rl())
                        .next()
                        .unwrap();
                let left_child_rc = Database::mut_page_cache()
                    .get_internal_page(
                        &tx,
                        Permission::ReadOnly,
                        &e.get_left_child(),
                    )
                    .unwrap();
                tx.commit().unwrap();
                return left_child_rc;
            }
            _ => {
                let e =
                    BTreeInternalPageIterator::new(&root_pod.rl())
                        .skip(index - 1)
                        .next()
                        .unwrap();
                let left_child_rc = Database::mut_page_cache()
                    .get_internal_page(
                        &tx,
                        Permission::ReadOnly,
                        &e.get_right_child(),
                    )
                    .unwrap();
                tx.commit().unwrap();
                return left_child_rc;
            }
        },
        _ => todo!(),
    }
}

pub fn get_leaf_page(
    table: &BTreeTable,
    level: usize,
    index: usize,
) -> Pod<BTreeLeafPage> {
    match level {
        0 => {
            let tx = Transaction::new();
            let root_pid = table.get_root_pid(&tx);
            let root_pod = Database::mut_page_cache()
                .get_leaf_page(&tx, Permission::ReadOnly, &root_pid)
                .unwrap();
            tx.commit().unwrap();
            return root_pod;
        }
        _ => {
            let internal_pod =
                get_internal_page(table, level - 1, index);
            let tx = Transaction::new();
            let e =
                BTreeInternalPageIterator::new(&internal_pod.rl())
                    .next()
                    .unwrap();
            let leaf_pod = Database::mut_page_cache()
                .get_leaf_page(
                    &tx,
                    Permission::ReadOnly,
                    &e.get_left_child(),
                )
                .unwrap();
            tx.commit().unwrap();
            return leaf_pod;
        }
    }
}
