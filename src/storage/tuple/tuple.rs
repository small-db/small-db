use std::{
    fmt::{self},
    usize,
};

use crate::{
    btree::page::BTreePageID,
    io::{Serializeable, SmallWriter},
    storage::{table_schema::TableSchema, tuple::Cell},
    transaction::{TransactionID, TransactionStatus},
    Database,
};

#[derive(Clone)]
/// Tuple is only visible to transaction that has an id between xmin and xmax
pub struct Tuple {
    /// The transaction that created this tuple.
    xmin: TransactionID,

    /// The transaction that deleted or updated this tuple.
    ///
    /// (The update is treated as deletion of the older tuple and insertion of
    /// the new tuple.)
    xmax: TransactionID,

    cells: Vec<Cell>,
}

// constructors
impl Tuple {
    pub fn new(cells: &Vec<Cell>, tx_id: TransactionID) -> Self {
        Self {
            xmin: tx_id,
            xmax: TransactionID::MAX,

            cells: cells.to_vec(),
        }
    }

    pub(crate) fn new_x(xmin: TransactionID, xmax: TransactionID, cells: &Vec<Cell>) -> Self {
        Self {
            xmin,
            xmax,

            cells: cells.to_vec(),
        }
    }

    pub(crate) fn clone(&self) -> Self {
        Self::new_x(self.xmin, self.xmax, &self.cells.clone())
    }
}

impl Tuple {
    pub fn get_cell(&self, i: usize) -> Cell {
        self.cells[i].clone()
    }

    pub fn get_cells(&self) -> Vec<Cell> {
        self.cells.clone()
    }

    /// Determines whether the tuple is visible to the transaction with the
    /// specified ID. This function is only relevant for isolation levels at
    /// or more strict than "Read Committed."
    pub(crate) fn visible_to(&self, tid: TransactionID) -> bool {
        let transaction_status = Database::concurrent_status().transaction_status.clone();

        // Invisible case 1:
        // The tuple is created by a transaction starts later than transaction "tid", so
        // it's not visible to "tid".
        if tid < self.xmin {
            return false;
        }

        // Invisible case 2:
        // The transaction that created the tuple started earlier than "tid", but it has
        // not committed yet, so the tuple is not visible to the "tid".
        if self.xmin < tid {
            if let Some(status) = transaction_status.get(&self.xmin) {
                if *status != TransactionStatus::Committed {
                    return false;
                }
            }

            // If unable to find the status of the transaction, the transaction
            // was committed in a previous database instance.
            //
            // Q: When we cannot find teh status of the transaction "xmain", why
            // the "xmin" must represent a committed transaction rather than an
            // aborted one from a previous database instance?
            // A: If the transaction hadn't been committed, the page would have
            // been recovered during the recovery process, and we wouldn't see
            // the tuples created by the aborted transaction.
        }

        // Invisible case 3:
        // The tuple was deleted by "tid" itself, so it is not visible to "tid".
        if tid == self.xmax {
            return false;
        }

        // Invisible case 4:
        // The tuple was deleted by a transaction that started earlier than "tid", and
        // the deleter has committed, so the tuple is not visible to "tid".
        if self.xmax < tid {
            let v = Database::concurrent_status().transaction_status.clone();
            if let Some(status) = v.get(&self.xmin) {
                if *status == TransactionStatus::Committed {
                    return false;
                }
            } else {
                // Cannot find the status of the transaction, means the deleter has been
                // committed.
                return false;
            }
        }

        // In all other cases, the tuple is visible to "tid".
        return true;
    }
}

impl Serializeable for Tuple {
    type Reference = TableSchema;

    fn encode(&self, writer: &mut SmallWriter, reference: &Self::Reference) {
        self.xmin.encode(writer, &());
        self.xmax.encode(writer, &());

        for i in 0..self.cells.len() {
            let cell = &self.cells[i];
            let t = reference.get_fields()[i].get_type();
            cell.encode(writer, &t);
        }
    }

    fn decode<R: std::io::Read>(reader: &mut R, reference: &Self::Reference) -> Self {
        let xmin = TransactionID::decode(reader, &());
        let xmax = TransactionID::decode(reader, &());

        let mut cells: Vec<Cell> = Vec::new();
        for field in reference.get_fields() {
            let cell = Cell::decode(reader, &field.get_type());
            cells.push(cell);
        }
        Self::new_x(xmin, xmax, &cells)
    }
}

impl PartialEq for Tuple {
    fn eq(&self, other: &Self) -> bool {
        for (i, field) in self.cells.iter().enumerate() {
            if field != &other.cells[i] {
                return false;
            }
        }

        return true;
    }
}

impl Eq for Tuple {}

impl fmt::Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut content: String = "{".to_owned();

        // xmin
        content.push_str(&format!("xmin: {:?}, ", self.xmin));

        // xmax
        content.push_str(&format!("xmax: {:?}, ", self.xmax));

        // cells
        for cell in &self.cells {
            let cell_str = format!("{:?}, ", cell);
            content.push_str(&cell_str);
        }
        content = content[..content.len() - 2].to_string();

        content.push_str(&"}");
        write!(f, "{}", content,)
    }
}

impl fmt::Debug for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

// TODO: move this to `btree` module, or remove it
#[derive(PartialEq)]
pub struct WrappedTuple {
    internal: Tuple,
    slot_number: usize,
    pid: BTreePageID,
}

impl std::ops::Deref for WrappedTuple {
    type Target = Tuple;
    fn deref(&self) -> &Self::Target {
        &self.internal
    }
}

impl std::ops::DerefMut for WrappedTuple {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.internal
    }
}

impl WrappedTuple {
    pub fn new(internal: &Tuple, slot_number: usize, pid: BTreePageID) -> WrappedTuple {
        WrappedTuple {
            internal: internal.clone(),
            slot_number,
            pid,
        }
    }

    pub fn get_slot_number(&self) -> usize {
        self.slot_number
    }

    pub fn get_pid(&self) -> BTreePageID {
        self.pid
    }

    pub(crate) fn get_tuple(&self) -> &Tuple {
        &self.internal
    }
}

impl Eq for WrappedTuple {}

impl fmt::Display for WrappedTuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.get_tuple())
    }
}

impl fmt::Debug for WrappedTuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}
