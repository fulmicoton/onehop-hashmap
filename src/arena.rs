use std::ptr;
use std::mem;
use std::ops::DerefMut;
use std::ops::Deref;

const NUM_BITS_PAGE_ADDR: usize = 20;
const PAGE_SIZE: usize = 1 << NUM_BITS_PAGE_ADDR;

#[derive(Clone, Copy, Debug)]
pub struct Addr(usize);

impl Addr {
    #[inline(always)]
    fn new(page_id: usize, local_addr: usize) -> Addr {
        Addr(page_id << NUM_BITS_PAGE_ADDR | local_addr)
    }

    #[inline(always)]
    fn page_id(&self) -> usize {
        self.0 >> NUM_BITS_PAGE_ADDR
    }

    #[inline(always)]
    fn page_local_addr(&self) -> usize {
        self.0 & (PAGE_SIZE - 1)
    }

}

struct Page {
    page_id: usize,
    len: usize,
    data: Box<[u8]>
}

impl Page {
    fn new(page_id: usize) -> Page {
        Page {
            page_id,
            len: 0,
            data: vec![0u8; PAGE_SIZE].into_boxed_slice()
        }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    fn allocate(&mut self, len: usize) -> Option<Addr> {
        if len + self.len <= PAGE_SIZE {
            let local_addr = self.len;
            self.len += len;
            Some(Addr::new(self.page_id, local_addr))
        } else {
            None
        }
    }

    unsafe fn read<V: Sized + Copy>(&self, addr: usize) -> V {
        ptr::read_unaligned(self.data.as_ptr().offset(addr as isize) as *const V)
    }

    unsafe fn write<V: Sized + Copy>(&mut self, addr: usize, val: V) {
        ptr::write_unaligned(self.data.as_mut_ptr().offset(addr as isize) as *mut V, val)
    }

    fn get_mut_slice(&mut self, addr: usize, len: usize) -> &mut [u8] {
        &mut (*self.data)[addr..addr+len]
    }

    #[inline(always)]
    unsafe fn read_slice(&self, addr: usize) -> (&[u8], Addr) {
        let data: &[u8] = &(*self.data)[addr..];
        // fast track for small slices
        let len = data[0] as usize;
        if len < 128 {
            (&data[1..1 + len], Addr::new(self.page_id, addr + 1 + len))
        } else {
            let (len, read_len) = read_vint(data);
            let slice: &[u8] = &data[read_len..read_len+len];
            (slice, Addr::new(self.page_id, addr + read_len + len))
        }

    }
}


pub struct Handler<'a, V: Copy> {
    arena: &'a mut Arena,
    val: V,
    addr: Addr,
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
            self.arena.write(self.addr, self.val);
        }
    }
}

impl<'a, V: Copy> Handler<'a, V> {
    fn new(arena: &mut Arena, val: V, addr: Addr) -> Handler<V> {
        Handler {
            arena,
            val,
            addr
        }
    }
}

pub struct Arena {
    pages: Vec<Page>,
}


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


impl Arena {

    pub fn new() -> Arena {
        Arena {
            pages: vec![Page::new(0)]
        }
    }

    fn add_page(&mut self) -> &mut Page {
        let new_page_id = self.pages.len();
        self.pages.push(Page::new(new_page_id));
        &mut self.pages[new_page_id]
    }

    pub unsafe fn read<V: Sized + Copy>(&self, addr: Addr) -> V {
        self.pages[addr.page_id()].read::<V>(addr.page_local_addr())
    }

    pub unsafe fn write<V: Sized + Copy>(&mut self, addr: Addr, val: V) {
        self.pages[addr.page_id()].write::<V>(addr.page_local_addr(), val)
    }

    pub fn save<V: Sized + Copy>(&mut self, val: V) -> Addr {
        let addr = self.allocate(mem::size_of::<V>());
        unsafe {
            self.write::<V>(addr, val);
        }
        addr
    }

//    pub fn get_slice(&self, addr: Addr, len: usize) -> &[u8] {
//        self.pages[addr.page_id()].get_slice(addr.page_local_addr(), len)
//    }

    pub fn get_mut_slice(&mut self, addr: Addr, len: usize) -> &mut [u8] {
        self.pages[addr.page_id()]
            .get_mut_slice(addr.page_local_addr(), len)
    }

    pub unsafe fn get_handler<V: Copy>(&mut self, addr: Addr) -> Handler<V> {
        let val = self.read::<V>(addr);
        Handler::new(self, val, addr)
    }

//    pub unsafe fn set_handler<V: Copy>(&mut self, addr: Addr, val: V) -> Handler<V> {
//        Handler::new(self, val, addr)
//    }

    pub fn len(&self) -> usize {
        self.pages.last().unwrap().len()
    }

    pub unsafe fn set_new_handler<V: Copy>(&mut self, val: V) -> Handler<V> {
        let num_bytes = mem::size_of::<V>();
        let addr = self.allocate(num_bytes);
        Handler::new(self, val, addr)
    }

    /// reads the slice stored at the given address
    /// and returns the address right after it.
    pub unsafe fn read_slice(&self, addr: Addr) -> (&[u8], Addr) {
        self.pages[addr.page_id()].read_slice(addr.page_local_addr())
    }

    pub fn save_slice(&mut self, bytes: &[u8]) -> Addr {
        let bytes_len = bytes.len();
        let len_len = vint_len(bytes_len);
        let payload_len = len_len + bytes_len;
        let addr = self.allocate(payload_len);
        let buffer = self.get_mut_slice(addr, payload_len);
        write_vint(&mut buffer[..len_len], bytes_len);
        buffer[len_len..].copy_from_slice(bytes);
        addr
    }

    pub fn allocate(&mut self, len: usize) -> Addr {
        assert!(len < PAGE_SIZE, "Can't allocate anything over {}", PAGE_SIZE);
        let page_id = self.pages.len() - 1;
        if let Some(addr) = self.pages[page_id].allocate(len) {
            addr
        } else {
            self.add_page().allocate(len).unwrap()
        }

    }

}

#[cfg(test)]
mod tests {

    use super::Arena;
    use super::{read_vint, vint_len, write_vint};

    #[derive(Debug, Copy, Clone, Eq, PartialEq)]
    #[repr(packed)]
    struct TestDatum {
        i: u32,
        j: u8
    }

    impl TestDatum {
        fn new(i: u32, j: u8) -> TestDatum {
            TestDatum { i, j }
        }

    }

    #[test]
    fn test_arena_obj() {
        let mut arena = Arena::new();
        let a = TestDatum::new(17u32, 9u8);
        let b = TestDatum::new(119u32, 234u8);
        let addr_a = arena.save(a);
        let addr_b = arena.save(b);
        let b_loaded: TestDatum = unsafe { arena.read(addr_b) };
        let a_loaded: TestDatum = unsafe { arena.read(addr_a) };
        assert_eq!(a_loaded, a);
        assert_eq!(b_loaded, b);
    }

    #[test]
    fn test_arena_slice() {
        let mut arena = Arena::new();
        let a = b"hello";
        let b = b"happy tax payer";
        let addr_a = arena.save_slice(a);
        let addr_b = arena.save_slice(b);
        let (b_loaded, _) = unsafe { arena.read_slice(addr_b) };
        let (a_loaded, _) = unsafe { arena.read_slice(addr_a) };
        assert_eq!(a_loaded, a);
        assert_eq!(b_loaded, b);
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