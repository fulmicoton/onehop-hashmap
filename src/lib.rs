use std::cell::Cell;
use std::borrow::Borrow;
use std::cmp::max;
use std::hash::{Hash, Hasher, BuildHasher};
use std::iter::FromIterator;
use std::mem::{self, replace};
use std::ptr;
use std::collections::hash_map::RandomState;
use std::marker::PhantomData;
mod murmurhash2;

pub type Addr = usize;
pub type HashKey = u64;


#[derive(Clone)]
enum Bucket {
    Entry((HashKey, Addr)),
    Vacant
}

const INITIAL_TABLE_SIZE: usize = 1_024;
const DEFAULT_HEAP_CAPACITY: usize = 1_024 * 128;

#[derive(Clone)]
pub struct HashMap<V, S=RandomState> {
    hash_builder: S,
    key_addr: Vec<Bucket>,
    heap: Vec<u8>,
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
            heap: Vec::with_capacity(DEFAULT_HEAP_CAPACITY),
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


struct Probe<'a> {
    key_addr: &'a mut [Bucket],
    bucket_addr: usize
}


// copy pasted from the std hash table.
/*
fn robin_hood<'a, V: 'a>(bucket: FullBucketMut<'a, K, V>,
                        mut displacement: usize,
                        mut hash: SafeHash,
                        mut key: &[u8],
                        mut val: V)
                        -> FullBucketMut<'a, K, V> {
    let size = bucket.table().size();
    let raw_capacity = bucket.table().capacity();

    // There can be at most `size - dib` buckets to displace, because
    // in the worst case, there are `size` elements and we already are
    // `displacement` buckets away from the initial one.
    let idx_end = (bucket.index() + size - bucket.displacement()) % raw_capacity;

    // Save the *starting point*.
    let mut bucket = bucket.stash();

    loop {
        let (old_hash, old_key, old_val) = bucket.replace(hash, key, val);
        hash = old_hash;
        key = old_key;
        val = old_val;

        loop {
            displacement += 1;
            let probe = bucket.next();
            debug_assert!(probe.index() != idx_end);

            let full_bucket = match probe.peek() {
                Empty(bucket) => {
                    // Found a hole!
                    let bucket = bucket.put(hash, key, val);
                    // Now that it's stolen, just read the value's pointer
                    // right out of the table! Go back to the *starting point*.
                    //
                    // This use of `into_table` is misleading. It turns the
                    // bucket, which is a FullBucket on top of a
                    // FullBucketMut, into just one FullBucketMut. The "table"
                    // refers to the inner FullBucketMut in this context.
                    return bucket.into_table();
                }
                Full(bucket) => bucket,
            };

            let probe_displacement = full_bucket.displacement();

            bucket = full_bucket;

            // Robin hood! Steal the spot.
            if probe_displacement < displacement {
                displacement = probe_displacement;
                break;
            }
        }
    }
}
*/

impl<'a> Iterator for Probe<'a> {
    type Item = &'a mut Bucket;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        unimplemented!();
    }
}

impl<V, S> HashMap<V, S>
    where S: BuildHasher {

    pub fn insert(&mut self, key: &str, value: V) -> Option<V> {
        self.reserve(1);
        unimplemented!("error");
    }

    fn reserve(&mut self, additional_capacity: usize) {
        unimplemented!();
    }

    fn make_hash(&self, key: &str) -> HashKey {
        let mut hasher: S::Hasher = self.hash_builder.build_hasher();
        hasher.write(key.as_bytes());
        hasher.finish()
    }

    fn probe(&self, hash: HashKey) -> Probe {
        unimplemented!()
    }

    pub fn get(&self, key: &str) -> Option<V> {
        let hash = self.make_hash(key);
        for bucket in self.probe(hash) {
            match bucket {
                Bucket::Vacant => {
                    return None;
                }
                Bucket::Entry((HashKey, Addr)) => {
                    unimplemented!()
                }
            }
        }
        unimplemented!()
    }
}