use std::collections::{HashMap, HashSet};

use super::TransactionID;

pub(crate) struct WaitForGraph {
    // key: transaction id, value: the transactions that the key transaction is waiting for
    graph: HashMap<TransactionID, HashSet<TransactionID>>,
}

impl WaitForGraph {
    pub(crate) fn new() -> Self {
        Self {
            graph: HashMap::new(),
        }
    }

    pub(crate) fn add_edge(&mut self, from: TransactionID, to: TransactionID) {
        self.graph
            .entry(from)
            .or_insert_with(HashSet::new)
            .insert(to);
    }

    pub(crate) fn remove_edge(&mut self, from: TransactionID, to: TransactionID) {
        if let Some(transactions) = self.graph.get_mut(&from) {
            transactions.remove(&to);
        }
    }

    pub(crate) fn get_waiting_transactions(
        &self,
        tid: TransactionID,
    ) -> Option<&HashSet<TransactionID>> {
        self.graph.get(&tid)
    }

    pub(crate) fn remove_transaction(&mut self, tid: TransactionID) {
        self.graph.remove(&tid);
    }

    /// Check if there is a cycle in the wait-for graph.
    pub(crate) fn exists_cycle(&self) -> bool {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        for &tid in self.graph.keys() {
            if self.is_cyclic(tid, &mut visited, &mut rec_stack) {
                return true;
            }
        }

        false
    }

    fn is_cyclic(
        &self,
        tid: TransactionID,
        visited: &mut HashSet<TransactionID>,
        rec_stack: &mut HashSet<TransactionID>,
    ) -> bool {
        if !visited.contains(&tid) {
            visited.insert(tid);
            rec_stack.insert(tid);

            if let Some(transactions) = self.graph.get(&tid) {
                for &t in transactions {
                    if !visited.contains(&t) && self.is_cyclic(t, visited, rec_stack) {
                        return true;
                    } else if rec_stack.contains(&t) {
                        return true;
                    }
                }
            }
        }

        rec_stack.remove(&tid);
        false
    }
}
