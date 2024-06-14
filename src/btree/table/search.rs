use crate::BTreeTable;

impl BTreeTable {
    // pub(super) fn get_pages(
    //     &self,
    //     tx: &Transaction,
    //     predicate: &Predicate,
    // ) -> Result<Vec<Arc<RwLock<BTreeLeafPage>>>, SmallError> {
    //     if predicate.field_index == self.key_field {
    //         match predicate.op {
    //             Op::Equals | Op::GreaterThan | Op::GreaterThanOrEq => {
    //                 start_page_rc = self.find_leaf_page(
    //                     tx,
    //                     Permission::ReadOnly,
    //                     root_pid,
    //                     &SearchFor::Target(predicate.cell.clone()),
    //                 )
    //             }
    //             Op::LessThan | Op::LessThanOrEq => {
    //                 start_page_rc = table.find_leaf_page(
    //                     &tx,
    //                     Permission::ReadOnly,
    //                     root_pid,
    //                     &SearchFor::LeftMost,
    //                 )
    //             }
    //             Op::Like => todo!(),
    //             Op::NotEquals => todo!(),
    //         }
    //     } else {
    //         log::error!("Not implemented yet");
    //         todo!()
    //     }
    //     todo!()
    // }
}
