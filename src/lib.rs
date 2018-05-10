mod murmurhash2;
mod arena;

const INITIAL_TABLE_SIZE: usize = 1_024 * 1_024 * 8;

use std::cmp;
use std::mem;
use std::hash::{Hasher, BuildHasher};
use std::collections::hash_map::RandomState;
use std::marker::PhantomData;
use arena::{Addr, Handler, Arena};


pub type HashKey = u32;

#[derive(Clone, Copy, Debug)]
enum Bucket {
    Vacant,
    Occupied(HashKey, Addr)
}

pub struct HashMap<V> {
    key_addr: Vec<Bucket>,
    arena: Arena,
    data: PhantomData<V>,
    max_probe_dist: usize,
    len: usize
}

impl<V> HashMap<V>
    where
        V: Copy {

    pub fn new() -> HashMap<V> {
        HashMap {
            key_addr: vec![Bucket::Vacant; INITIAL_TABLE_SIZE],
            arena: Arena::new(),
            data: PhantomData,
            max_probe_dist: 0, //< TODO fix me.
            len: 0
        }
    }

}

struct Probe {
    dist: usize,
    bucket_addr: usize,
    mask: usize
}

impl Probe {

    fn new(hash: HashKey, bucket_len: usize) -> Probe {
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

pub struct NotPresentEntry<'a, V: Copy + 'a> {
    hash_map: &'a mut HashMap<V>,
    probing_distance: usize,
    bucket_id: usize,
    inner: NotPresentEntryCases,
    key: &'a [u8],
    hash: HashKey,
    _type: PhantomData<V>
}

impl<'a, V:Copy + 'a> NotPresentEntry<'a, V>
{
    fn new(hash_map: &'a mut HashMap<V>,
           probing_distance: usize,
           bucket_id: usize,
           hash: HashKey,
           key: &'a [u8],
           cases: NotPresentEntryCases) -> Self {
        NotPresentEntry {
            hash_map,
            probing_distance,
            bucket_id,
            inner: cases,
            key,
            hash,
            _type: PhantomData
        }
    }

    fn insert(mut self, value: V)  -> Handler<'a, V> {
        self.hash_map.len += 1;
        let addr = self.hash_map.arena.save_slice(self.key);
        self.hash_map.max_probe_dist = cmp::max(self.hash_map.max_probe_dist, self.probing_distance);
        self.hash_map.key_addr[self.bucket_id] = Bucket::Occupied(self.hash, addr);
        if let NotPresentEntryCases::KickableEntry { probing_distance, hash, addr } = self.inner {
            self.kick(probing_distance + 1, hash, addr);
        }
        unsafe { self.hash_map.arena.set_new_handler(value) }
    }

    fn kick(&mut self, probing_distance: usize, hash: HashKey, addr: Addr) {
        let mut occupied_bucket = Bucket::Occupied(hash, addr);
        let mut d = probing_distance;
        let mut probe = Probe::new(self.hash, self.hash_map.key_addr.len());
        let mut bucket_id = probe.advance_by(probing_distance);
        let buckets = &mut self.hash_map.key_addr[..];
        loop {
            match buckets[bucket_id] {
                Bucket::Vacant => {
                    buckets[bucket_id] = occupied_bucket;
                    return;
                },
                Bucket::Occupied(in_place_hash, _) => {
                    let in_place_probing_distance = dist(bucket_id, in_place_hash, buckets.len() - 1);
                    if in_place_probing_distance < d {
                        mem::swap(&mut occupied_bucket, &mut buckets[bucket_id]);
                        d = in_place_probing_distance;
                    }
                }
            }
            bucket_id = probe.advance();
            d += 1;
        }
    }
}


fn dist(bucket_id: usize, hash: HashKey, mask: usize) -> usize {
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
    #[inline(always)]
    pub fn or_insert(self, default_value: V) -> Handler<'a, V> {
        match self {
            Entry::Present(handler) => handler,
            Entry::NotPresent(entry) => {
                entry.insert(default_value)
            }
        }
    }
}

impl<V> HashMap<V>
    where V: Copy {

    pub fn capacity(&self) -> usize {
        self.key_addr.len()
    }

    fn reserve(&mut self, _additional_capacity: usize) {
        unimplemented!();
    }

    fn make_hash(&self, key: &[u8]) -> HashKey {
        murmurhash2::murmurhash2(key)
    }

    pub fn entry<'a>(&'a mut self, key: &'a [u8]) -> Entry<'a, V> {
        let hash = self.make_hash(key);
        let mut probe = Probe::new(hash, self.key_addr.len());
        for probing_distance in 0.. {
            let bucket_addr = probe.advance();
            match self.key_addr[bucket_addr] {
                Bucket::Vacant => {
                    let vacant_entry = NotPresentEntryCases::VacantEntry;
                    return Entry::NotPresent(NotPresentEntry::new(self,
                                                                  probing_distance,
                                                                  bucket_addr,
                                                                  hash,
                                                                  key,
                                                                  vacant_entry));
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
                            self,
                            probing_distance,
                            bucket_addr,
                            hash,
                            key,
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

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn max_probe_dist(&self) -> usize {
        self.max_probe_dist
    }

    pub fn get(&self, key: &[u8]) -> Option<V> {
        let hash = self.make_hash(key);
        let mut probe = Probe::new(hash, self.key_addr.len());
        for _ in 0..(self.max_probe_dist + 1) {
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
        let mut hash_map: HashMap<u32> = HashMap::new();
        assert_eq!(hash_map.get(b"coucou"), None);
        {
            let mut handler = hash_map.entry(b"coucou").or_insert(0);
            *handler += 1;
        }
        assert_eq!(hash_map.len(), 1);
        assert_eq!(hash_map.get(b"coucou"), Some(1));
    }


    #[test]
    fn test_insert_same_el_twice() {
        let mut hash_map: HashMap<u32> = HashMap::new();
        assert_eq!(hash_map.get(b"coucou"), None);
        {
            let mut handler = hash_map.entry(b"coucou").or_insert(0);
            *handler += 1;
        }
        assert_eq!(hash_map.len(), 1);
        {
            let mut handler = hash_map.entry(b"coucou").or_insert(0);
            *handler += 1;
        }
        assert_eq!(hash_map.len(), 1);
        assert_eq!(hash_map.get(b"coucou"), Some(2));
    }

    #[test]
    fn test_insert_and_update() {
        let mut hash_map: HashMap<u32> = HashMap::new();
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
        let mut hash_map: HashMap<u32> = HashMap::new();
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
        let mut hash_map: HashMap<u32> = HashMap::new();
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
