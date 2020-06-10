use rand::Rng;

pub struct TransactionID {
    pub id: i32,
}

impl TransactionID {
    pub fn new() -> TransactionID {
        let id = rand::thread_rng().gen_range(1, 100);
        TransactionID { id }
    }
}
