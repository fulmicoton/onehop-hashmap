#![feature(nll)]

mod murmurhash2;
mod arena;

const INITIAL_TABLE_SIZE: usize = 1_024 * 1_024 * 8;

use std::cmp;
use std::mem;
use std::marker::PhantomData;
pub use arena::{Addr, Arena};
use std::ptr;
use std::ops::{Deref, DerefMut};


pub type HashKey = u32;

/// Returns the number in bytes that will
/// be required to serialisze `n` as a variable int.
fn vint_len(n: usize) -> usize {
    if n < 128 {
        1
    } else {
        let num_bits_required: usize = 64 - n.leading_zeros() as usize;
        num_bits_required.wrapping_add(6) / 7
    }
}


const CONTINUE_FLAG: u8 = 1u8 << 7;
const SEVEN_BIT_MASK: usize = (CONTINUE_FLAG - 1u8) as usize;

fn write_vint(buffer: &mut [u8], mut val: usize) {
    for dest_byte in buffer.iter_mut() {
        let b = (val & SEVEN_BIT_MASK) as u8;
        val >>= 7;
        if val == 0 {
            *dest_byte = b;
            break;
        } else {
            *dest_byte = b | CONTINUE_FLAG;
        }
    }
}

fn read_vint(buffer: &[u8]) -> (usize, usize) {
    let mut n = 0;
    let mut bit_shift = 0;
    for (num_consumed_bytes, b) in buffer.iter().cloned().enumerate() {
        n |= ((b as usize) & SEVEN_BIT_MASK) << bit_shift;
        if b & CONTINUE_FLAG == 0u8 {
            return (n, num_consumed_bytes + 1);
        } else {
            bit_shift += 7;
        }
    }
    (n, buffer.len())
}

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


pub struct Handler<'a, V: Copy> {
    dest: &'a mut [u8],
    val: V
}

impl<'a, T> Deref for Handler<'a, T> where T: Copy {

    type Target = T;

    fn deref(&self) -> &T {
        &self.val
    }
}

impl<'a, T> DerefMut for Handler<'a, T> where T: Copy {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.val
    }
}

impl<'a, V> Drop for Handler<'a, V>
    where V: Copy {
    fn drop(&mut self) {
        unsafe {
            ptr::write_unaligned(self.dest.as_mut_ptr() as *mut V, self.val)
        }
    }
}

impl<'a, V: Copy> Handler<'a, V> {
    fn new(dest: &'a mut[u8]) -> Handler<'a, V> {
        let val = unsafe { ptr::read(dest.as_ptr() as *const V) };
        Handler::new_with_value(dest, val)
    }

    fn new_with_value(dest: &'a mut[u8], val: V) -> Handler<'a, V> {
        Handler {
            dest,
            val
        }
    }
}

fn extract_slice(data: &[u8]) -> (&[u8], usize) {
    let len = data[0] as usize;
    if len < 128 {
        (&data[1..1 + len], 1 + len)
    } else {
        let (len, read_len) = read_vint(data);
        (&data[read_len..read_len+len], read_len + len)
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

    fn insert_key_value<'a>(&'a mut self, bucket_addr: usize, hash: HashKey,  key: &[u8], value: V) -> Handler<'a, V> {
        let bytes_len: usize = key.len();
        let len_len: usize = vint_len(bytes_len);
        let payload_len: usize = len_len + bytes_len + mem::size_of::<V>();
        let (addr, data): (Addr, &mut [u8]) = self.arena.allocate_slice(payload_len);
        self.key_addr[bucket_addr] = Bucket::occupied(hash, addr);
        write_vint(&mut data[..len_len], bytes_len);
        data[len_len..len_len + bytes_len].copy_from_slice(key);
        Handler::new_with_value(&mut data[len_len + bytes_len..], value)
    }

//    pub fn update<'a, F: Fn(Option<V>)->V >(&'a mut self, key: &'a [u8], default_value: V, f: F) {}

    pub fn get_or_insert<'a>(&'a mut self, key: &'a [u8], default_value: V) -> Handler<'a, V> {
        let hash = self.make_hash(key);
        let mut probe = Probe::new(hash, self.key_addr.len());
        for probing_distance in 0.. {
            let bucket_addr = probe.advance();
            let bucket = self.key_addr[bucket_addr];
            {
                if bucket.is_vacant()  {
                    self.len += 1;
                    self.max_probe_dist = cmp::max(self.max_probe_dist, probing_distance);
                    return self.insert_key_value(bucket_addr, hash, key, default_value);
                }
            }
            {
                let in_place_hash = bucket.hash();
                let in_place_addr = bucket.addr();
                if in_place_hash == hash {
                    // ugly scope to drop the borrow on `self.arena`
                    let data: &mut [u8] = self.arena.get_large_slice_mut(in_place_addr);
                    let (key_matches, value_offset) = {
                        let (bucket_key, len) = extract_slice(data);
                        (key == bucket_key, len)
                    };
                    if key_matches {
                        let value_data = &mut data[value_offset..value_offset+mem::size_of::<V>()];
                        return Handler::new(value_data);
                    }
                } else {
                    let in_place_probing_distance = probe.dist(bucket_addr, in_place_hash);
                    if in_place_probing_distance < probing_distance {
                        self.len += 1;
                        self.max_probe_dist = cmp::max(self.max_probe_dist, probing_distance);
                        self.kick(in_place_probing_distance + 1, in_place_hash, in_place_addr);
                        return self.insert_key_value(bucket_addr, hash, key, default_value);
                    } else {
                        // keep on probing...
                    }
                }
            }
        }
        unreachable!()
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
            let bucket = self.key_addr[bucket_addr];
            if bucket.is_vacant() {
                return None;
            } else {
                let in_place_hash_key = bucket.hash();
                let addr = bucket.addr();
                if in_place_hash_key == hash {
                    // ugly scope to drop the borrow on `self.arena`
                    unsafe {
                        let data = self.arena.get_large_slice(addr);
                        let (key_matches, value_offset) = {
                            let (bucket_key, len) = extract_slice(data);
                            (key == bucket_key, len)
                        };
                        if key_matches {
                            let value_ptr = data.as_ptr().offset(value_offset as isize) as *const V;
                            return Some(ptr::read_unaligned(value_ptr));
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
    use super::vint_len;
    use super::write_vint;
    use super::read_vint;

    #[test]
    fn test_insert_one() {
        let mut hash_map: HashMap<u32> = HashMap::new();
        assert_eq!(hash_map.get(b"coucou"), None);
        {
            let mut handler = hash_map.get_or_insert(b"coucou", 0);
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
            let mut handler = hash_map.get_or_insert(b"coucou", 0);
            *handler += 1;
        }
        assert_eq!(hash_map.len(), 1);
        {
            let mut handler = hash_map.get_or_insert(b"coucou", 0);
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
            let mut handler = hash_map.get_or_insert(b"coucou", 0);
            *handler += 1;
        }
        {
            let mut handler = hash_map.get_or_insert(b"coucou", 0);
            *handler += 1;
        }
        assert_eq!(hash_map.get(b"coucou"), Some(2));
    }

    #[test]
    fn test_insert_two_keys() {
        let mut hash_map: HashMap<u32> = HashMap::new();
        assert_eq!(hash_map.get(b"coucou"), None);
        {
            let mut handler = hash_map.get_or_insert(b"coucou", 0);
            *handler += 1;
        }
        assert_eq!(hash_map.get(b"coucou2"), None);
        {
            let mut handler = hash_map.get_or_insert(b"coucou2", 0);
            *handler += 1;
        }
        {
            let mut handler = hash_map.get_or_insert(b"coucou3", 0);
            *handler += 1;
        }
        {
            let mut handler = hash_map.get_or_insert(b"coucou4", 0);
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
                let mut handler = hash_map.get_or_insert(key_bytes, 0);
                *handler += 1;
            }
        }
        for i in 0..800 {
            let key = format!("key{}", i);
            let key_bytes = key.as_bytes();
            assert_eq!(hash_map.get(key_bytes), Some(1));
            {
                let mut handler = hash_map.get_or_insert(key_bytes, 0);
                *handler += 1;
            }
        }
        for i in 0..800 {
            let key = format!("key{}", i);
            let key_bytes = key.as_bytes();
            assert_eq!(hash_map.get(key_bytes), Some(2));
        }
    }


    #[test]
    fn test_vint() {
        let test_aux = |n: usize| {
            let mut dest = [0u8; 10];
            let len_num_bytes = vint_len(n);
            write_vint(&mut dest[..len_num_bytes], n);
            let (n_read, num_bytes) = read_vint(&dest);
            assert_eq!(n_read, n);
            assert_eq!(num_bytes, len_num_bytes);
        };
        test_aux(0);
        test_aux(1);
        test_aux(127);
        test_aux(256);
        test_aux(1_000_000);
    }

}