mod murmurhash2;
mod arena;

use std::hash::{Hash, Hasher, BuildHasher};
use std::mem;
use std::ptr;
use std::collections::hash_map::RandomState;
use std::marker::PhantomData;
use std::slice;

use arena::{Addr, Handler, Arena};


pub type HashKey = u64;

#[derive(Clone, Copy)]
enum Bucket {
    Vacant,
    Occupied(HashKey, Addr)
}

const INITIAL_TABLE_SIZE: usize = 1_024;
const DEFAULT_HEAP_CAPACITY: usize = 1_024 * 128;

pub struct HashMap<V, S=RandomState> {
    hash_builder: S,
    key_addr: Vec<Bucket>,
    arena: Arena,
    data: PhantomData<V>,
}

impl<V, S> HashMap<V, S>
    where S: Default, V: Copy {

    fn new() -> HashMap<V> {
        Default::default()
    }
}

impl<V, S> HashMap<V, S> {
    fn with_hash_builder(hash_builder: S) -> HashMap<V, S> {
        HashMap {
            hash_builder,
            key_addr: vec![Bucket::Vacant; INITIAL_TABLE_SIZE],
            arena: Arena::new(),
            data: PhantomData,
        }
    }
}

impl<V, S> Default for HashMap<V, S>
    where S: Default, V: Copy {
    fn default() -> Self {
        HashMap::with_hash_builder(S::default())
    }
}


struct Probe {
    dist: usize,
    bucket_addr: usize,
    mask: usize
}

impl Probe {
    fn advance(&mut self) -> usize {
        self.dist += 1;
        self.bucket_addr = (self.bucket_addr + 1) & self.mask;
        self.bucket_addr
    }
}


fn read_key(data: &[u8]) -> (&[u8], usize) {
    unimplemented!();
}


pub struct NotPresentEntry<'a, V: Copy> {
    inner: NotPresentEntryCases<'a, V>
}

impl<'a, V:Copy> NotPresentEntry<'a, V> {
    fn new(cases: NotPresentEntryCases<'a, V>) -> Self {
        NotPresentEntry {
            inner: cases,
        }
    }
}

enum NotPresentEntryCases<'a, V>  where V: Copy {
    VacantEntry(&'a mut Arena, &'a mut Bucket, PhantomData<V>),
    KickableEntry(&'a mut Arena, usize, &'a mut [Bucket], PhantomData<V>)
}

pub enum Entry<'a, V:'a> where V: Copy {
    NotPresent(NotPresentEntry<'a, V>),
    Present(Handler<'a, V>)
}

impl<'a, V:'a> Entry<'a, V:'a> {
    fn or_insert(mut self, default_value: V) -> Handler<'a, V> {
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

    fn probe(&self, hash: u64) -> Probe {
        let mask = self.capacity() - 1;
        Probe {
            dist: 0,
            bucket_addr: (hash as usize) & mask,
            mask
        }
    }

    fn reserve(&mut self, additional_capacity: usize) {
        unimplemented!();
    }

    fn make_hash(&self, key: &[u8]) -> HashKey {
        let mut hasher: S::Hasher = self.hash_builder.build_hasher();
        hasher.write(key);
        hasher.finish()
    }

    fn dist(&self, bucket_id: usize, hash: u64) -> usize {
        let target_pos: usize = (hash as usize) & (self.capacity() - 1);
        if bucket_id >= target_pos {
            bucket_id - target_pos
        } else {
            self.capacity() + bucket_id - target_pos
        }
    }

    #[inline(always)]
    fn entry(&mut self, key: &[u8]) -> Entry<V> {
        let hash = self.make_hash(key);
        let mut probe = self.probe(hash);
        for probing_distance in 0.. {
            let bucket_addr = probe.advance();
            match self.key_addr[bucket_addr] {
                Bucket::Vacant => {
                    let vacant_entry = NotPresentEntryCases::VacantEntry(&mut self.arena, &mut self.key_addr[bucket_addr], PhantomData);
                    return Entry::NotPresent(NotPresentEntry::new(vacant_entry));
                }
                Bucket::Occupied(in_place_hash_key, addr) => {
                    if in_place_hash_key == hash {
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
                    if self.dist(bucket_addr, in_place_hash_key) <= probing_distance {
                        let kickable_entry = NotPresentEntryCases::KickableEntry(&mut self.arena, bucket_addr, &mut self.key_addr[..], PhantomData);
                        return Entry::NotPresent(NotPresentEntry::new(kickable_entry));
                    } else {
                        // keep on probing...
                    }
                }
            }
        }
        unreachable!();
    }

    pub unsafe fn get(&self, key: &[u8]) -> Option<V> {
        unimplemented!();
//        let (_, val_addr_opt) = self.get_bucket_value_addr(key);
//        val_addr_opt.map(|val_addr| self.heap.read::<V>(val_addr))
    }

    pub unsafe fn get_handler(&mut self, key: &[u8]) -> Option<Handler<V>> {
        unimplemented!();
//        let (_, val_addr_opt) = self.get_bucket_value_addr(key);
//        val_addr_opt.map(move |val_addr| {
//            self.heap.get_handler::<V>(val_addr)
//        })
    }
}


#[cfg(test)]
mod test {

//    #[test]
//    fn test_read_key() {
//        let key = "";
//        read_key();
//    }

}
