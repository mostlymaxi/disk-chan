use std::{
    cell::UnsafeCell,
    fs::File,
    marker::PhantomData,
    path::Path,
    sync::atomic::{AtomicU32, Ordering},
};

use memmap2::MmapMut;

use crate::atomic_union::AtomicUnion;

const MAX_WAITING_GROUPS: usize = 16;
const MAX_MAP_IDX_SLOTS: usize = u16::MAX as usize;
const SALT: u32 = 1;

#[repr(transparent)]
struct MapIdxSlot {
    idx_with_salt: AtomicU32,
}

impl MapIdxSlot {
    fn wake_all(&self) {
        atomic_wait::wake_all(&self.idx_with_salt);
    }
}

#[repr(C)]
struct ChanPagePersist<Data: ?Sized = [u8]> {
    write_idx_count: AtomicUnion,
    read_count_groups: [AtomicUnion; MAX_WAITING_GROUPS],
    map: [MapIdxSlot; MAX_MAP_IDX_SLOTS],
    data: Data,
}

pub struct ChanPage {
    inner: UnsafeCell<MmapMut>,
    _phantom: PhantomData<ChanPagePersist>,
}

unsafe impl Send for ChanPage {}
unsafe impl Sync for ChanPage {}

pub enum ChanPageError {
    PageFull,
}

impl ChanPage {
    #[inline]
    pub fn len(&self) -> usize {
        unsafe { (&*self.inner.get()).len() - size_of::<ChanPagePersist<[u8; 0]>>() }
    }

    #[inline]
    unsafe fn get_inner<'a>(&'a self) -> &'a ChanPagePersist {
        unsafe {
            &*(std::ptr::slice_from_raw_parts((&*self.inner.get()).as_ptr(), self.len())
                as *const ChanPagePersist)
        }
    }

    #[inline]
    unsafe fn get_inner_mut<'a>(&'a self) -> &'a mut ChanPagePersist {
        unsafe {
            &mut *(std::ptr::slice_from_raw_parts_mut(
                (&mut *self.inner.get()).as_mut_ptr(),
                self.len(),
            ) as *mut ChanPagePersist)
        }
    }

    pub unsafe fn new<P: AsRef<Path>>(path: P, len: usize) -> Result<Self, std::io::Error> {
        let size = size_of::<ChanPagePersist<[u8; 0]>>();
        let size = len + size;

        let f = File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(path.as_ref())?;

        f.set_len(size as u64)?;

        let raw = unsafe { memmap2::MmapMut::map_mut(&f)? };
        let raw = UnsafeCell::new(raw);

        Ok(ChanPage {
            inner: raw,
            _phantom: PhantomData,
        })
    }

    pub fn push<V: AsRef<[u8]>>(&self, val: V) -> Result<(), ChanPageError> {
        let page = unsafe { self.get_inner_mut() };

        let (count, idx) = page.write_idx_count.fetch_add_high_low(
            size_of::<u32>() as u32 + val.as_ref().len() as u32,
            1,
            Ordering::Relaxed,
        );

        if count >= MAX_MAP_IDX_SLOTS as u32 {
            return Err(ChanPageError::PageFull);
        }

        if idx + size_of::<u32>() as u32 + val.as_ref().len() as u32 >= self.len() as u32 {
            page.map[count as usize]
                .idx_with_salt
                .store(u32::MAX, Ordering::Release);

            page.map[count as usize].wake_all();

            return Err(ChanPageError::PageFull);
        }

        let idx_offset = idx as usize + size_of::<u32>();

        page.data[idx as usize..idx_offset]
            .copy_from_slice(&(val.as_ref().len() as u32).to_le_bytes());
        page.data[idx_offset..idx_offset + val.as_ref().len()].copy_from_slice(val.as_ref());

        page.map[count as usize]
            .idx_with_salt
            .store(idx + SALT, Ordering::Relaxed);

        page.map[count as usize].wake_all();

        Ok(())
    }

    pub fn pop(&self, group: usize) -> Result<&[u8], ChanPageError> {
        let page = unsafe { self.get_inner_mut() };
        let (count, _) = page.read_count_groups[group].fetch_add_low(1, Ordering::Relaxed);

        if count >= MAX_MAP_IDX_SLOTS as u32 {
            let _ = page.read_count_groups[group].fetch_add_high(1, Ordering::Relaxed);
            return Err(ChanPageError::PageFull);
        }

        let data = self.get(count);

        let _ = page.read_count_groups[group].fetch_add_high(1, Ordering::Relaxed);
        data
    }

    fn get(&self, count: u32) -> Result<&[u8], ChanPageError> {
        let page = unsafe { self.get_inner() };

        let idx = loop {
            let idx = page.map[count as usize]
                .idx_with_salt
                .load(Ordering::Acquire);

            let idx = match idx {
                _ if idx < SALT => {
                    atomic_wait::wait(&page.map[count as usize].idx_with_salt, 0);
                    continue;
                }
                idx => idx - SALT,
            };

            break idx;
        };

        if idx >= self.len() as u32 {
            if count < MAX_MAP_IDX_SLOTS as u32 - 1 {
                page.map[count as usize + 1]
                    .idx_with_salt
                    .store(u32::MAX, Ordering::Release);
                page.map[count as usize + 1].wake_all();
            }
            return Err(ChanPageError::PageFull);
        }

        let idx_offset = idx as usize + size_of::<u32>();

        let data_len = u32::from_le_bytes(page.data[idx as usize..idx_offset].try_into().unwrap());

        Ok(&page.data[idx_offset..idx_offset + data_len as usize])
    }
}
