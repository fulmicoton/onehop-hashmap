use std::ptr;
use std::mem;

const NUM_BITS_PAGE_ADDR: usize = 20;
const PAGE_SIZE: usize = 1 << NUM_BITS_PAGE_ADDR; // pages are 1 MB large

#[derive(Clone, Copy, Debug)]
pub struct Addr(pub u32);

impl Addr {
    #[inline(always)]
    fn new(page_id: usize, local_addr: usize) -> Addr {
        Addr( (page_id << NUM_BITS_PAGE_ADDR | local_addr) as u32)
    }

    #[inline(always)]
    fn page_id(&self) -> usize {
        (self.0 as usize) >> NUM_BITS_PAGE_ADDR
    }

    #[inline(always)]
    fn page_local_addr(&self) -> usize {
        (self.0 as usize) & (PAGE_SIZE - 1)
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
    fn is_available(&self, len: usize) -> bool {
        len + self.len <= PAGE_SIZE
    }

    #[inline(always)]
    fn allocate(&mut self, len: usize) -> Option<(Addr, &mut [u8])> {
        if self.is_available(len) {
            let local_addr = self.len;
            self.len += len;
            let addr = Addr::new(self.page_id, local_addr);
            Some((addr, &mut (*self.data)[local_addr..][..len]))
        } else {
            None
        }
    }

    fn get_mut_slice(&mut self, addr: usize, len: usize) -> &mut [u8] {
        &mut (*self.data)[addr..addr+len]
    }

    #[inline(always)]
    pub(crate) unsafe fn get_ptr(&self, addr: usize) -> *const u8 {
        self.data.as_ptr().offset(addr as isize)
    }

    #[inline(always)]
    pub(crate) unsafe fn get_mut_ptr(&mut self, addr: usize) -> *mut u8 {
        self.data.as_mut_ptr().offset(addr as isize)
    }
}


pub struct Arena {
    pages: Vec<Page>,
}


impl Arena {

    pub fn new() -> Arena {
        let mut first_page = Page::new(0);
        // reserving addr=0
        first_page.allocate(1);
        Arena {
            pages: vec![first_page]
        }
    }

    fn add_page(&mut self) -> &mut Page {
        let new_page_id = self.pages.len();
        self.pages.push(Page::new(new_page_id));
        &mut self.pages[new_page_id]
    }

    pub fn save<V: Sized + Copy>(&mut self, val: V) -> Addr {
        let (addr, slice) = self.allocate(mem::size_of::<V>());
        unsafe {
            ptr::write_unaligned(slice.as_mut_ptr() as *mut V, val)
        }
        addr
    }

    #[inline(always)]
    pub fn get_mut_slice(&mut self, addr: Addr, len: usize) -> &mut [u8] {
        self.pages[addr.page_id()]
            .get_mut_slice(addr.page_local_addr(), len)
    }

    pub unsafe fn get_ptr(&self, addr: Addr) -> *const u8 {
        self.pages[addr.page_id()].get_ptr(addr.page_local_addr())
    }

    pub unsafe fn get_mut_ptr(&mut self, addr: Addr) -> *mut u8 {
        self.pages[addr.page_id()].get_mut_ptr(addr.page_local_addr())
    }

    pub fn allocate(&mut self, len: usize) -> (Addr, &mut [u8]) {
        assert!(len < PAGE_SIZE, "Can't allocate anything over {}", PAGE_SIZE);
        let page_id = self.pages.len() - 1;
        if self.pages[page_id].is_available(len) {
            return self.pages[page_id].allocate(len).unwrap();
        } else {
            return self.add_page().allocate(len).unwrap();
        }
    }

}

#[cfg(test)]
mod tests {

    use super::Arena;

    #[test]
    fn test_arena_allocate() {
        let mut arena = Arena::new();
        let a = b"hello";
        let b = b"happy tax payer";

        let addr_a = {
            let (addr_a, data) = arena.allocate(a.len());
            data.copy_from_slice(a);
            addr_a
        };
        let addr_b = {
            let (addr_b, data) = arena.allocate(b.len());
            data.copy_from_slice(b);
            addr_b
        };
        {
            let a_retrieve = arena.get_mut_slice(addr_a, a.len());
            assert_eq!(a_retrieve, a);
        }
        {
            let b_retrieve = arena.get_mut_slice(addr_b, b.len());
            assert_eq!(b_retrieve, b);
        }
    }

}