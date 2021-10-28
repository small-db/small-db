use super::BTreePageID;

pub trait BTreePage {
    fn get_pid(&self) -> BTreePageID;

    fn get_parent_pid(&self) -> BTreePageID;
    fn set_parent_pid(&mut self, pid: &BTreePageID);
}
