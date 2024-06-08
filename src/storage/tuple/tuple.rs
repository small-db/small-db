use std::{
    fmt::{self},
    usize,
};

use log::{debug, error};

use crate::{
    btree::page::BTreePageID,
    io::{Decodeable, Encodeable, Serializeable, SmallWriter},
    storage::{table_schema::TableSchema, tuple::Cell},
    transaction::{TransactionID, TransactionStatus, TRANSACTION_ID_BYTES},
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

    // pub(crate) fn read_from<R: std::io::Read>(reader: &mut R, schema: &TableSchema) -> Self {
    //     let xmin = TransactionID::decode_from(reader);
    //     let xmax = TransactionID::decode_from(reader);

    //     let mut cells: Vec<Cell> = Vec::new();
    //     for field in schema.get_fields() {
    //         let cell = Cell::read_from(reader, &field.get_type());
    //         cells.push(cell);
    //     }
    //     Self::new_x(xmin, xmax, &cells)
    // }

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

    pub(crate) fn visible_to(&self, tid: TransactionID) -> bool {
        // out of the range [xmin, xmax), not visible
        if tid < self.xmin || self.xmax <= tid {
            return false;
        }

        if tid == self.xmin {
            // the tuple is visible to the transaction that created it
            return true;
        }

        // tid in the range (xmin, xmax), the tuple is visible if the transaction that
        // created it has committed
        // debug!("status: {:?}", Database::concurrent_status().transaction_status);
        // Database::concurrent_status().transaction_status;
        if let Some(status) = Database::concurrent_status()
            .transaction_status
            .get(&self.xmin)
        {
            if *status == TransactionStatus::Committed {
                // it is visible only if the transaction that created it has committed
                return true;
            } else {
                // debug!("status: {:?}", status);
                return false;
            }
        } else {
            // cannot find the status of the transaction that created this tuple
            error!("txn not found: {}", self.xmin);
            return false;
        }
    }
}

impl Serializeable for Tuple {
    type Reference = TableSchema;

    fn encode_memory(&self, writer: &mut SmallWriter) {
        self.xmin.encode_memory(writer);
        self.xmax.encode_memory(writer);

        for cell in &self.cells {
            cell.encode_memory(writer);
        }
    }

    fn decode_memory<R: std::io::Read>(reader: &mut R) -> Self {
        let xmin = TransactionID::decode_from(reader);
        let xmax = TransactionID::decode_from(reader);

        let mut cells: Vec<Cell> = Vec::new();
        for field in reference.get_fields() {
            let cell = Cell::decode_from(reader, &field.get_type());
            cells.push(cell);
        }
        Self::new_x(xmin, xmax, &cells)
    }

    fn encode_disk(&self, writer: &mut SmallWriter, reference: &Self::Reference) {
        self.xmin.encode_disk(writer, &());
        self.xmax.encode_disk(writer, &());

        for i in 0..self.cells.len() {
            let cell = &self.cells[i];
            let t = reference.get_fields()[i].get_type();
            cell.encode_disk(writer, &t);
        }
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
