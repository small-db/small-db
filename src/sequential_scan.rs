use crate::transaction_id::TransactionID;
use crate::row::Row;
use crate::database::Database;
//use crate::database::db;

pub struct SequentialScan {
    pub tid: TransactionID,
    pub table_id: i32,
    pub table_alias: String,
}

impl SequentialScan {
    pub fn new(tid: TransactionID, table_id: i32, table_alias: &str) -> SequentialScan {
        SequentialScan {
            tid,
            table_id,
            table_alias: table_alias.to_string(),
        }
    }

    pub fn open() {

    }

    pub fn next() {
//        get pages
//        let page =
//        print!("{:?}", db.get_catalog());
//        db.g
    }
}
