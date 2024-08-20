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

    pub(crate) fn remove_waiter(&mut self, tid: TransactionID) {
        self.graph.remove(&tid);
    }

    /// Check if there is a cycle in the wait-for graph.
    pub(crate) fn find_cycle(&self) -> Option<Vec<TransactionID>> {
        let mut visited = HashSet::new();
        let mut stack = Vec::new();

        for &tid in self.graph.keys() {
            if let Some(cycle) = self.dfs(&tid, &mut visited, &mut stack) {
                return Some(cycle);
            }
        }

        return None;
    }

    fn dfs(
        &self,
        tid: &TransactionID,
        visited: &mut HashSet<TransactionID>,
        stack: &mut Vec<TransactionID>,
    ) -> Option<Vec<TransactionID>> {
        if stack.contains(tid) {
            stack.push(*tid);
            return Some(stack.clone());
        }

        if !visited.contains(tid) {
            return None;
        }

        visited.insert(*tid);

        stack.push(*tid);

        if let Some(transactions) = self.graph.get(&tid) {
            for &t in transactions {
                if let Some(cycle) = self.dfs(&t, visited, stack) {
                    return Some(cycle);
                }
            }
        }

        stack.pop();
        return None;
    }
}
