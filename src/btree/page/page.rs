use super::BTreePageID;
use crate::storage::schema::Schema;

pub trait BTreePage {
    fn new(pid: &BTreePageID, bytes: &[u8], schema: &Schema) -> Self
    where
        Self: Sized;

    fn get_pid(&self) -> BTreePageID;

    fn get_parent_pid(&self) -> BTreePageID;
    fn set_parent_pid(&mut self, pid: &BTreePageID);

    /// Generates a byte array representing the contents of this page.
    /// Used to serialize this page to disk.
    ///
    /// The invariant here is that it should be possible to pass the
    /// byte array generated by get_page_data to the BTreePage
    /// constructor and have it produce an identical BTreeLeafPage
    /// object.
    ///
    /// # Returns
    /// A byte array representing the contents of this page.
    fn get_page_data(&self) -> Vec<u8>;

    fn set_before_image(&mut self);

    /// Provide a representation of this page before any modifications
    /// were made to it. Used by recovery.
    fn get_before_image(&self) -> Vec<u8>;

    fn peek(&self);
}
