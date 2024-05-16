pub mod buffer_pool;
pub mod consts;
pub mod entry;
pub mod page;
pub mod table;

// questions about mysql:
// - what is "bufferfixed"?
// - what is "fsp latch"?

// The simplified version of the B+ tree latch strategy is as follows:
// - no tree latch
// - when accessing a node (either leaf or internal), all ancestor nodes of the node
//  must be latched (why? if not latched, two directions of tree-traversal may happen
//  at the same time, and lead to a deadlock)

// The imitate-mysql version of the B+ tree latch strategy is as follows:
// - there is a tree latch