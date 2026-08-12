use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    time::Instant,
};

#[derive(Eq, PartialEq)]
struct Ttl {
    key: Vec<u8>,
    expiration: Instant,
}

// Compare by expiration time
impl Ord for Ttl {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.expiration.cmp(&other.expiration)
    }
}

impl PartialOrd for Ttl {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct TtlManager {
    heap: BinaryHeap<Reverse<Ttl>>,
}

impl TtlManager {
    fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
        }
    }

    fn set_ttl(&mut self, key: Vec<u8>, expiration: Instant) {
        let ttl = Ttl {
            key,
            expiration,
        };

        self.heap.push(Reverse(ttl));
    }

    fn next_expiration(&self) -> Option<&Ttl> {
        self.heap.peek().map(|x| &x.0)
    }

    fn pop_expired(&mut self) -> Option<Ttl> {
        if let Some(Reverse(ttl)) = self.heap.peek() {
            if ttl.expiration <= Instant::now() {
                return self.heap.pop().map(|x| x.0);
            }
        }

        None
    }
}









