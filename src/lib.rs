use std::mem;
use std::marker::PhantomData;
use std::ptr;
use std::slice;

mod murmurhash2;
mod arena;
pub use arena::{Addr, Arena};

const DEFAULT_CAPACITY: usize = 1_024 * 1_024;
const CAPACITY_RATIO: usize = 4;

pub type HashKey = u32;

#[derive(Clone, Copy, Debug)]
enum Bucket {
    Vacant,
    Occupied {
        hash: HashKey,
        addr: Addr
    }
}

pub struct HashMap<V> {
    key_addr: Vec<Bucket>,
    arena: Arena,
    data: PhantomData<V>,
    len: usize
}

fn is_power_of_two(n: usize) -> bool {
    if n == 0 {
        false
    } else {
        n & (n - 1) == 0
    }
}

impl<V> HashMap<V> where V: Copy {
    pub fn new() -> HashMap<V> {
        HashMap::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> HashMap<V> {
        let table_len = capacity * CAPACITY_RATIO;
        assert!(is_power_of_two(table_len));
        HashMap {
            key_addr: vec![Bucket::Vacant; table_len],
            arena: Arena::new(),
            data: PhantomData,
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

    #[inline(always)]
    fn advance(&mut self) -> usize {
        self.bucket_addr = (self.bucket_addr + 1) & self.mask;
        self.bucket_addr
    }
}

unsafe fn write_key(buffer_ptr: *mut u8, data: &[u8]) {
    // assert!(data.len() < (1 << 16));
    ptr::write_unaligned(buffer_ptr as *mut u16, data.len() as u16);
    ptr::copy_nonoverlapping(data.as_ptr(), buffer_ptr.offset(2), data.len());
}

unsafe fn read_key<'a>(data: *const u8) -> &'a [u8] {
    let len = ptr::read_unaligned(data as *const u16) as usize;
    slice::from_raw_parts(data.offset(2), len)
}

#[inline(never)]
fn cmp_slice(left: &[u8], right: &[u8]) -> bool {
    left == right
}

impl<V> HashMap<V>
    where V: Copy {

    pub fn capacity(&self) -> usize {
        self.key_addr.len() / CAPACITY_RATIO
    }

    fn ensure_capacity(&mut self) {
        if self.len + 1 > self.capacity() {
            let new_len = self.key_addr.len() * 2;
            self.resize_table(new_len);
        }
    }

    fn resize_table(&mut self, new_size: usize) {
        let old_key_addr = mem::replace(&mut self.key_addr, vec![Bucket::Vacant; new_size]);
        for bucket in old_key_addr {
            if let Bucket::Occupied {hash, addr: _} = bucket {
                let mut probe = Probe::new(hash, self.key_addr.len());
                loop {
                    let bucket_addr = probe.advance();
                    match self.key_addr[bucket_addr] {
                        Bucket::Vacant => {
                            self.key_addr[bucket_addr] = bucket;
                            break;
                        }
                        _ => {
                        }
                    }
                }
            }
        }
    }

    #[inline(always)]
    fn make_hash(&self, key: &[u8]) -> HashKey {
        murmurhash2::murmurhash2(key)
    }

    #[inline(always)]
    fn insert_key_value<'a>(&'a mut self, bucket_addr: usize, hash: HashKey, key: &[u8], value: V) {
        let payload_len: usize = key.len() + 2 + mem::size_of::<V>();
        let (addr, data): (Addr, &mut [u8]) = self.arena.allocate(payload_len);
        self.key_addr[bucket_addr] = Bucket::Occupied { hash, addr };
        let data_ptr = data.as_mut_ptr();
        unsafe {
            let val_ptr = data_ptr as *mut V;
            ptr::write_unaligned(val_ptr, value);
            let key_ptr = data_ptr.offset(mem::size_of::<V>() as isize);
            write_key(key_ptr, key);
        }
    }

    pub fn update<'a, F: Fn(&mut V), DefaultF: Fn()->V>(&'a mut self, key: &'a [u8], update_value: F, default_value: DefaultF) {
        self.ensure_capacity();
        let hash = self.make_hash(key);
        let mut probe = Probe::new(hash, self.key_addr.len());
        loop {
            let bucket_addr = probe.advance();
            let bucket = self.key_addr[bucket_addr];
            match bucket {
                Bucket::Vacant => {
                    self.len += 1;
                    let value = default_value();
                    self.insert_key_value(bucket_addr, hash, key, value);
                    return;
                }
                Bucket::Occupied {
                    hash: in_place_hash,
                    addr: in_place_addr
                } => unsafe {
                    if in_place_hash == hash {
                        let data: *mut u8 = self.arena.get_mut_ptr(in_place_addr);
                        let in_place_key = read_key(data.offset(std::mem::size_of::<V>() as isize) as *const u8);
                        if cmp_slice(key, in_place_key) {
                            if mem::align_of::<V>() == 1 {
                                let data = data as *mut V;
                                let mut val: &mut V = &mut *data;
                                update_value(&mut val);
                            } else {
                                let mut val: V = ptr::read_unaligned(data as *const V);
                                update_value(&mut val);
                                ptr::write_unaligned(data as *mut V, val);
                            }
                            return;
                        }
                    }
                }
            }
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn get(&self, key: &[u8]) -> Option<V> {
        let hash = self.make_hash(key);
        let mut probe = Probe::new(hash, self.key_addr.len());
        loop {
            let bucket_addr = probe.advance();
            let bucket = self.key_addr[bucket_addr];
            match bucket {
                Bucket::Vacant => {
                    return None;
                }
                Bucket::Occupied {
                    hash: in_place_hash_key,
                    addr: in_place_addr
                } if in_place_hash_key == hash => {
                    unsafe {
                        let data = self.arena.get_ptr(in_place_addr);
                        let value = ptr::read_unaligned(data as *const V);
                        let in_place_key = read_key(data.offset(std::mem::size_of::<V>() as isize));
                        if cmp_slice(key, in_place_key) {
                            return Some(value);
                        }
                    }
                }
                _ => {}
            }
        }
    }

}


#[cfg(test)]
mod test {
    use super::HashMap;

    #[test]
    fn test_insert_one() {
        let mut hash_map: HashMap<u32> = HashMap::new();
        assert_eq!(hash_map.get(b"coucou"), None);
        hash_map.update(b"coucou", |val | {*val+=1}, || 1);
        assert_eq!(hash_map.len(), 1);
        assert_eq!(hash_map.get(b"coucou"), Some(1));
    }


    #[test]
    fn test_insert_same_el_twice() {
        let mut hash_map: HashMap<u32> = HashMap::new();
        assert_eq!(hash_map.get(b"coucou"), None);
        hash_map.update(b"coucou", |val| {*val+=1}, || 1);
        assert_eq!(hash_map.len(), 1);
        hash_map.update(b"coucou", |val| {*val+=1}, || 1);
        assert_eq!(hash_map.len(), 1);
        assert_eq!(hash_map.get(b"coucou"), Some(2));
    }

    #[test]
    fn test_insert_several_keys() {
        let mut hash_map: HashMap<u32> = HashMap::new();
        assert_eq!(hash_map.get(b"coucou"), None);
        hash_map.update(b"coucou", |val| {*val+=1}, || 1);
        assert_eq!(hash_map.get(b"coucou2"), None);
        hash_map.update(b"coucou2", |val| {*val+=1}, || 1);
        hash_map.update(b"coucou3", |val| {*val+=1}, || 1);
        hash_map.update(b"coucou4", |val| {*val+=1}, || 1);
        assert_eq!(hash_map.get(b"coucou"), Some(1));
        assert_eq!(hash_map.get(b"coucou2"), Some(1));
        assert_eq!(hash_map.get(b"coucou3"), Some(1));
        assert_eq!(hash_map.get(b"coucou4"), Some(1));
        assert_eq!(hash_map.get(b"coucou5"), None);
    }

    #[test]
    fn test_insert_thousand() {
        let mut hash_map: HashMap<u32> = HashMap::new();
        for i in 0..800 {
            let key = format!("key{}", i);
            let key_bytes = key.as_bytes();
            assert_eq!(hash_map.get(key_bytes), None);
            hash_map.update(key_bytes, |value| {*value +=1 }, || 1);
        }
        for i in 0..800 {
            let key = format!("key{}", i);
            let key_bytes = key.as_bytes();
            assert_eq!(hash_map.get(key_bytes), Some(1));
            hash_map.update(key_bytes, |value| { *value += 1 }, || { 0 });
        }
        for i in 0..800 {
            let key = format!("key{}", i);
            let key_bytes = key.as_bytes();
            assert_eq!(hash_map.get(key_bytes), Some(2));
        }
    }


}