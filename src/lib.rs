mod murmurhash2;
mod arena;

const INITIAL_TABLE_SIZE: usize = 1_024;

use std::mem;
use std::hash::{Hasher, BuildHasher};
use std::collections::hash_map::RandomState;
use std::marker::PhantomData;
use arena::{Addr, Handler, Arena};


pub type HashKey = u64;

#[derive(Clone, Copy, Debug)]
enum Bucket {
    Vacant,
    Occupied(HashKey, Addr)
}

pub struct HashMap<V, S=RandomState> {
    hash_builder: S,
    key_addr: Vec<Bucket>,
    arena: Arena,
    data: PhantomData<V>,
    max_probe_dist: usize,
}

impl<V, S> HashMap<V, S>
    where
        V: Copy,
        S: Default {

    fn new() -> HashMap<V, S> {
        HashMap::with_hash_builder(S::default())
    }
}

impl<V, S> HashMap<V, S> {
    fn with_hash_builder(hash_builder: S) -> HashMap<V, S> {
        HashMap {
            hash_builder,
            key_addr: vec![Bucket::Vacant; INITIAL_TABLE_SIZE],
            arena: Arena::new(),
            data: PhantomData,
            max_probe_dist: usize::max_value(), //< TODO fix me.
        }
    }
}


struct Probe {
    dist: usize,
    bucket_addr: usize,
    mask: usize
}

impl Probe {

    fn new(hash: u64, bucket_len: usize) -> Probe {
        let mask = bucket_len - 1;
        Probe {
            dist: 0,
            bucket_addr: (hash as usize) & mask,
            mask
        }
    }

    fn advance_by(&mut self, dist: usize) -> usize {
        self.dist += dist;
        self.bucket_addr = (self.bucket_addr + dist) & self.mask;
        self.bucket_addr
    }

    fn advance(&mut self) -> usize {
        self.dist += 1;
        self.bucket_addr = (self.bucket_addr + 1) & self.mask;
        self.bucket_addr
    }
}

pub struct NotPresentEntry<'a, V: Copy> {
    bucket_id: usize,
    buckets: &'a mut [Bucket],
    inner: NotPresentEntryCases,
    key: &'a [u8],
    hash: u64,
    heap: &'a mut Arena,
    _type: PhantomData<V>
}

impl<'a, V:Copy> NotPresentEntry<'a, V> {
    fn new(bucket_id: usize, buckets: &'a mut [Bucket], hash: u64, key: &'a [u8], heap: &'a mut Arena, cases: NotPresentEntryCases) -> Self {
        NotPresentEntry {
            bucket_id,
            buckets,
            inner: cases,
            key,
            hash,
            heap,
            _type: PhantomData
        }
    }

    fn insert(mut self, value: V)  -> Handler<'a, V> {
        let addr = self.heap.save_slice(self.key);
        self.buckets[self.bucket_id] = Bucket::Occupied(self.hash, addr);
        if let NotPresentEntryCases::KickableEntry { probing_distance, hash, addr } = self.inner {
            self.kick(probing_distance + 1, hash, addr);
        }
        let handler: Handler<'a, V> = unsafe { self.heap.set_new_handler(value) };
        handler
    }

    fn kick(&mut self, probing_distance: usize, hash: u64, addr: Addr) {
        let mut occupied_bucket = Bucket::Occupied(hash, addr);
        let mut d = probing_distance;
        let mut probe = Probe::new(hash, self.buckets.len());
        let mut bucket_id = probe.advance_by(probing_distance);
        loop {
            match self.buckets[bucket_id] {
                Bucket::Vacant => {
                    self.buckets[bucket_id] = occupied_bucket;
                    return;
                },
                Bucket::Occupied(in_place_hash, _) => {
                    let in_place_probing_distance = dist(bucket_id, in_place_hash, self.buckets.len() - 1);
                    if in_place_probing_distance < d {
                        mem::swap(&mut occupied_bucket, &mut self.buckets[bucket_id]);
                        d = in_place_probing_distance;
                    }
                }
            }
            bucket_id = probe.advance();
            d += 1;
        }
    }
}


fn dist(bucket_id: usize, hash: u64, mask: usize) -> usize {
    let target_pos: usize = (hash as usize) & mask;
    if bucket_id >= target_pos {
        bucket_id - target_pos
    } else {
        mask + 1 + bucket_id - target_pos
    }
}


enum NotPresentEntryCases {
    VacantEntry,
    KickableEntry { probing_distance: usize, hash: HashKey, addr: Addr }
}

pub enum Entry<'a, V:'a> where V: Copy {
    NotPresent(NotPresentEntry<'a, V>),
    Present(Handler<'a, V>)
}

impl<'a, V:'a> Entry<'a, V> where V: Copy {
    fn or_insert(self, default_value: V) -> Handler<'a, V> {
        match self {
            Entry::Present(handler) => handler,
            Entry::NotPresent(entry) => {
                entry.insert(default_value)
            }
        }
    }
}

impl<V, S> HashMap<V, S>
    where S: BuildHasher, V: Copy {

    fn capacity(&self) -> usize {
        self.key_addr.len()
    }

    fn reserve(&mut self, _additional_capacity: usize) {
        unimplemented!();
    }

    fn make_hash(&self, key: &[u8]) -> HashKey {
        let mut hasher: S::Hasher = self.hash_builder.build_hasher();
        hasher.write(key);
        hasher.finish()
    }

    #[inline(always)]
    fn entry<'a>(&'a mut self, key: &'a [u8]) -> Entry<'a, V> {
        let hash = self.make_hash(key);
        let mut probe = Probe::new(hash, self.key_addr.len());
        for probing_distance in 0.. {
            let bucket_addr = probe.advance();
            match self.key_addr[bucket_addr] {
                Bucket::Vacant => {
                    let vacant_entry = NotPresentEntryCases::VacantEntry;
                    return Entry::NotPresent(NotPresentEntry::new(bucket_addr, &mut self.key_addr, hash, key, &mut self.arena, vacant_entry));
                }
                Bucket::Occupied(in_place_hash, addr) => {
                    if in_place_hash == hash {
                        let bucket_value_offset_opt = {
                            // ugly scope to drop the borrow on `self.arena`
                            let (bucket_key, bucket_value_offset) = unsafe { self.arena.read_slice(addr) };
                            if key == bucket_key {
                                Some(bucket_value_offset)
                            } else {
                                None
                            }
                        };
                        if let Some(bucket_value_offset) = bucket_value_offset_opt {
                            let handler = unsafe { self.arena.get_handler::<V>(bucket_value_offset) };
                            return Entry::Present(handler);
                        } else {
                            // full-hash collision.
                        }
                    }
                    let in_place_probing_distance = dist(bucket_addr, in_place_hash, self.key_addr.len() - 1);
                    if in_place_probing_distance <= probing_distance {
                        // the entry at the bucket is at a lower distance.
                        // We can kick it and takes its place.
                        let kickable_entry = NotPresentEntryCases::KickableEntry {
                            probing_distance: in_place_probing_distance,
                            hash: in_place_hash,
                            addr
                        };
                        let e = NotPresentEntry::new(
                            bucket_addr,
                            &mut self.key_addr,
                            hash,
                            key,
                            &mut self.arena,
                            kickable_entry);
                        return Entry::NotPresent(e);
                    } else {
                        // keep on probing...
                    }
                }
            }
        }
        unreachable!();
    }

    pub fn get(&self, key: &[u8]) -> Option<V> {
        let hash = self.make_hash(key);
        let mut probe = Probe::new(hash, self.key_addr.len());
        for _ in 0..self.max_probe_dist {
            let bucket_addr = probe.advance();
            match self.key_addr[bucket_addr] {
                Bucket::Vacant => {
                    return None;
                }
                Bucket::Occupied(in_place_hash_key, addr) => {
                    if in_place_hash_key == hash {
                        // ugly scope to drop the borrow on `self.arena`
                        let (bucket_key, bucket_value_offset) = unsafe { self.arena.read_slice(addr) };
                        if key == bucket_key {
                            let value: V = unsafe { self.arena.read(bucket_value_offset) };
                            return Some(value);
                        }
                    }
                }
            }
        }
        None
    }

}


#[cfg(test)]
mod test {

    use super::HashMap;
    use std::collections::hash_map::RandomState;

    #[test]
    fn test_insert_one() {
        let mut hash_map: HashMap<u32, RandomState> = HashMap::new();
        assert_eq!(hash_map.get(b"coucou"), None);
        {
            let mut handler = hash_map.entry(b"coucou").or_insert(0);
            *handler += 1;
        }
        assert_eq!(hash_map.get(b"coucou"), Some(1));
    }

    #[test]
    fn test_insert_and_update() {
        let mut hash_map: HashMap<u32, RandomState> = HashMap::new();
        assert_eq!(hash_map.get(b"coucou"), None);
        {
            let mut handler = hash_map.entry(b"coucou").or_insert(0);
            *handler += 1;
        }
        {
            let mut handler = hash_map.entry(b"coucou").or_insert(0);
            *handler += 1;
        }
        assert_eq!(hash_map.get(b"coucou"), Some(2));
    }

    #[test]
    fn test_insert_two_keys() {
        let mut hash_map: HashMap<u32, RandomState> = HashMap::new();
        assert_eq!(hash_map.get(b"coucou"), None);
        {
            let mut handler = hash_map.entry(b"coucou").or_insert(0);
            *handler += 1;
        }
        assert_eq!(hash_map.get(b"coucou2"), None);
        {
            let mut handler = hash_map.entry(b"coucou2").or_insert(0);
            *handler += 1;
        }
        {
            let mut handler = hash_map.entry(b"coucou3").or_insert(0);
            *handler += 1;
        }
        {
            let mut handler = hash_map.entry(b"coucou4").or_insert(0);
            *handler += 1;
        }
        assert_eq!(hash_map.get(b"coucou"), Some(1));
        assert_eq!(hash_map.get(b"coucou2"), Some(1));
    }


    #[test]
    fn test_insert_thousand() {
        let mut hash_map: HashMap<u32, RandomState> = HashMap::new();
        for i in 0..800 {
            let key = format!("key{}", i);
            let key_bytes = key.as_bytes();
            assert_eq!(hash_map.get(key_bytes), None);
            {
                let mut handler = hash_map.entry(key_bytes).or_insert(0);
                *handler += 1;
            }
        }
        for i in 0..800 {
            let key = format!("key{}", i);
            let key_bytes = key.as_bytes();
            assert_eq!(hash_map.get(key_bytes), Some(1));
            {
                let mut handler = hash_map.entry(key_bytes).or_insert(0);
                *handler += 1;
            }
        }
        for i in 0..800 {
            let key = format!("key{}", i);
            let key_bytes = key.as_bytes();
            assert_eq!(hash_map.get(key_bytes), Some(2));
        }
    }

}
