use std::ptr;
use std::mem;

const NUM_BITS_PAGE_ADDR: usize = 20;
const PAGE_SIZE: usize = 1 << NUM_BITS_PAGE_ADDR;

#[derive(Clone, Copy, Debug)]
pub struct Addr(pub usize);

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
    fn allocate_slice(&mut self, len: usize) -> Option<(Addr, &mut [u8])> {
        if len + self.len <= PAGE_SIZE {
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

    pub(crate) fn get_large_slice(&self, addr: usize) -> &[u8] {
        &(*self.data)[addr..]
    }

    pub(crate) fn get_large_slice_mut(&mut self, addr: usize) -> &mut [u8] {
        &mut (*self.data)[addr..]
    }
}


pub struct Arena {
    pages: Vec<Page>,
}

impl Arena {
    pub fn new() -> Arena {
        let mut first_page = Page::new(0);
        // reserving addr=0
        first_page.allocate_slice(1);
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
        let (addr, slice) = self.allocate_slice(mem::size_of::<V>());
        unsafe {
            ptr::write_unaligned(slice.as_mut_ptr() as *mut V, val)
        }
        addr
    }

    pub fn get_mut_slice(&mut self, addr: Addr, len: usize) -> &mut [u8] {
        self.pages[addr.page_id()]
            .get_mut_slice(addr.page_local_addr(), len)
    }

    pub fn get_large_slice(&self, addr: Addr) -> &[u8] {
        self.pages[addr.page_id()].get_large_slice(addr.page_local_addr())
    }

    pub fn get_large_slice_mut(&mut self, addr: Addr) -> &mut [u8] {
        self.pages[addr.page_id()].get_large_slice_mut(addr.page_local_addr())
    }

    pub fn allocate_slice(&mut self, len: usize) -> (Addr, &mut [u8]) {
        assert!(len < PAGE_SIZE, "Can't allocate anything over {}", PAGE_SIZE);
        let page_id = self.pages.len() - 1;
        if let Some(res) = self.pages[page_id].allocate_slice(len) {
            res
        } else {
            self.add_page().allocate_slice(len).unwrap()
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
            let (addr_a, data) = arena.allocate_slice(a.len());
            data.copy_from_slice(a);
            addr_a
        };
        let addr_b = {
            let (addr_b, data) = arena.allocate_slice(b.len());
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