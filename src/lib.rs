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
struct Bucket {
    hash: u32,
    addr: u32,
}

impl Bucket {

    #[inline(always)]
    fn vacant() -> Bucket {
        Bucket {
            hash: 0u32,
            addr: 0u32
        }
    }

    fn occupied(hash: HashKey, addr: Addr) -> Bucket {
        Bucket {
            hash,
            addr: addr.0 as u32
        }
    }

    #[inline(always)]
    fn hash(&self) -> HashKey {
        self.hash
    }

    #[inline(always)]
    fn addr(&self) -> Addr {
        Addr(self.addr as usize)
    }

    #[inline(always)]
    fn is_vacant(&self) -> bool {
        let v: &u64 = unsafe { mem::transmute(self) };
        *v == 0u64
    }
}

impl Default for Bucket {
    fn default() -> Self {
        Bucket::vacant()
    }
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
        let vacant_bucket = Bucket::vacant();
        HashMap {
            key_addr: vec![vacant_bucket; INITIAL_TABLE_SIZE],
            arena: Arena::new(),
            data: PhantomData,
            max_probe_dist: 0, //< TODO fix me.
            len: 0
        }
    }

}

struct Probe {
    bucket_addr: usize,
    mask: usize
}

impl Probe {


    #[inline(always)]
    fn new(hash: HashKey, len: usize) -> Probe {
        let mask = len - 1;
        Probe {
            bucket_addr: (hash as usize) & mask,
            mask
        }
    }

    fn new_with_dist(hash: HashKey, len: usize, dist: usize) -> Probe {
        let mask = len - 1;
        Probe {
            bucket_addr: (hash as usize).wrapping_add(dist) & mask,
            mask
        }
    }

    #[inline(always)]
    fn dist(&self, bucket_id: usize, hash: HashKey) -> usize {
        bucket_id.wrapping_sub(hash as usize) & self.mask
    }

    #[inline(always)]
    fn advance(&mut self) -> usize {
        self.bucket_addr = (self.bucket_addr + 1) & self.mask;
        self.bucket_addr
    }
}

/*
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
        self.hash_map.key_addr[self.bucket_id] = bucket::occupied(self.hash, addr);
        if let NotPresentEntryCases::KickableEntry { probing_distance, hash, addr } = self.inner {
            self.kick(probing_distance + 1, hash, addr);
        }
        unsafe { self.hash_map.arena.set_new_handler(value) }
    }

    #[inline(always)]
    fn kick(&mut self, probing_distance: usize, hash: HashKey, addr: Addr) {
        let mut occupied_bucket = bucket::occupied(hash, addr);
        let mut d = probing_distance;
        let mut probe = Probe::new_with_dist(self.hash, self.hash_map.key_addr.len(), probing_distance);
        let buckets = &mut self.hash_map.key_addr[..];
        let mut bucket_id = probe.advance();
        loop {
            let bucket = unsafe { *buckets.get_unchecked(bucket_id) };
            if bucket.is_vacant() {
                break;
            }
            let in_place_probing_distance = probe.dist(bucket_id, bucket.hash());
            if in_place_probing_distance < d {
                mem::swap(&mut occupied_bucket, &mut buckets[bucket_id]);
                d = in_place_probing_distance;
            }
            bucket_id = probe.advance();
            d += 1;
        }
        buckets[bucket_id] = occupied_bucket;
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
*/

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


    fn kick(&mut self, mut probing_distance: usize, hash: HashKey, addr: Addr) {
        let mut occupied_bucket = Bucket::occupied(hash, addr);
        let buckets = &mut self.key_addr[..];
        let mut probe = Probe::new_with_dist(hash, buckets.len(), probing_distance);
        let mut bucket_id = probe.advance();
        loop {
            let bucket = buckets[bucket_id];
            if bucket.is_vacant() {
                break;
            }
            let in_place_hash = bucket.hash();
            let in_place_probing_distance = probe.dist(bucket_id, in_place_hash);
            if in_place_probing_distance < probing_distance {
                mem::swap(&mut occupied_bucket, &mut buckets[bucket_id]);
                probing_distance = in_place_probing_distance;
            }
            probing_distance += 1;
            bucket_id = probe.advance();
        }
        buckets[bucket_id] = occupied_bucket;
    }

    pub fn get_or_insert<'a>(&'a mut self, key: &'a [u8], default_value: V) -> Handler<'a, V> {
        let mut hash = self.make_hash(key);
        let mut probe = Probe::new(hash, self.key_addr.len());
        for probing_distance in 0.. {
            let bucket_addr = probe.advance();
            let bucket = self.key_addr[bucket_addr];
            if bucket.is_vacant()  {
                self.len += 1;
                let addr = self.arena.save_slice(key);
                self.max_probe_dist = cmp::max(self.max_probe_dist, probing_distance);
                self.key_addr[bucket_addr] = Bucket::occupied(hash, addr);
                return unsafe { self.arena.set_new_handler(default_value) };
            } else {
                let in_place_hash = bucket.hash();
                let in_place_addr = bucket.addr();
                if in_place_hash == hash {
                    // ugly scope to drop the borrow on `self.arena`
                    unsafe {
                        let (key_matches, bucket_value_offset) = {
                            let (bucket_key, bucket_value_offset_val) = self.arena.read_slice(in_place_addr);
                            (key == bucket_key, bucket_value_offset_val)
                        };
                        if key_matches {
                            return self.arena.get_handler::<V>(bucket_value_offset);
                        }
                    }
                } else {
                    let in_place_probing_distance = probe.dist(bucket_addr, in_place_hash);
                    if in_place_probing_distance < probing_distance {
                        self.len += 1;
                        let addr = self.arena.save_slice(key);
                        self.max_probe_dist = cmp::max(self.max_probe_dist, probing_distance);
                        self.key_addr[bucket_addr] = Bucket::occupied(hash, addr);
                        self.kick(in_place_probing_distance + 1, in_place_hash, in_place_addr);
                        return unsafe { self.arena.set_new_handler(default_value) };
                    } else {
                        // keep on probing...
                    }
                }
            }
        }
        unreachable!()
    }


    /*
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
                        // ugly scope to drop the borrow on `self.arena`
                        unsafe {
                            let (key_matches, bucket_value_offset) = {
                                let (bucket_key, bucket_value_offset_val) = self.arena.read_slice(addr);
                                (key == bucket_key, bucket_value_offset_val)
                            };
                            if key_matches {
                                let handler = self.arena.get_handler::<V>(bucket_value_offset);
                                return Entry::Present(handler);
                            }
                        }
                    } else {
                        let in_place_probing_distance = probe.dist(bucket_addr, in_place_hash);
                        if in_place_probing_distance < probing_distance {
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
        }
        unreachable!();
    }
    */

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
            let bucket = self.key_addr[bucket_addr];
            if bucket.is_vacant() {
                return None;
            } else {
                let in_place_hash_key = bucket.hash();
                let addr = bucket.addr();
                if in_place_hash_key == hash {
                    // ugly scope to drop the borrow on `self.arena`
                    unsafe {
                        let (bucket_key, bucket_value_offset) = self.arena.read_slice(addr);
                        if key == bucket_key {
                            let value: V = self.arena.read(bucket_value_offset);
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
