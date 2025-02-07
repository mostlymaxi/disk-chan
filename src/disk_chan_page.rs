use std::{
    cell::UnsafeCell,
    marker::PhantomData,
    path::Path,
    sync::atomic::{AtomicU32, AtomicU8, Ordering},
    task::{Context, Poll},
};

use futures::{future::poll_fn, task::AtomicWaker};
use memmap2::MmapMut;
use tokio::fs::File;

use crate::atomic_union::AtomicUnion;

pub(super) type MapUsize = u16;
pub(super) type IdxUsize = u32;
pub(super) type CountUsize = u32;

const MAX_WAITING_GROUPS: usize = 16;
const MAX_MAP_IDX_SLOTS: usize = MapUsize::MAX as usize;
const SALT: u32 = 1;

struct MapIdxSlot {
    waiting_count: AtomicU8,
    idx_with_salt: AtomicU32,
    waiters: [AtomicWaker; MAX_WAITING_GROUPS],
}

impl MapIdxSlot {
    fn wake_all(&self) {
        let waiting_count = self.waiting_count.swap(0, Ordering::Relaxed);
        if waiting_count > 0 {
            self.waiters[..waiting_count as usize]
                .iter()
                .for_each(|w| w.wake());
        }
    }
}

#[repr(C)]
pub(super) struct ChanPagePersist<Data: ?Sized = [u8]> {
    write_idx_count: AtomicUnion,
    read_count_groups: [AtomicUnion; MAX_WAITING_GROUPS],
    map: [MapIdxSlot; MAX_MAP_IDX_SLOTS],
    data: Data,
}

impl std::fmt::Debug for ChanPagePersist {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChanPagePersist")
            .field("write_idx_count", &self.write_idx_count)
            .field("read_count_groups", &&self.read_count_groups[0..2])
            .finish_non_exhaustive()
    }
}

#[repr(transparent)]
pub(crate) struct ChanPage {
    inner: UnsafeCell<MmapMut>,
    _phantom: PhantomData<ChanPagePersist>,
}

impl std::fmt::Debug for ChanPage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChanPage")
            .field("inner", unsafe { &self.get_inner() })
            .finish()
    }
}

unsafe impl Send for ChanPage {}
unsafe impl Sync for ChanPage {}

pub enum ChanPageError {
    PageFull,
}

impl ChanPage {
    pub(super) unsafe fn reset_all_waiters(&mut self) {
        for slot in &mut self.get_inner_mut().map {
            slot.waiting_count.store(0, Ordering::SeqCst);
            slot.waiters.fill_with(Default::default);
        }
    }

    pub(super) unsafe fn reset_read_count_groups(&mut self) {
        for group in &mut self.get_inner_mut().read_count_groups {
            let strong_read_count = group.load_high(Ordering::SeqCst);
            group.store_low(strong_read_count, Ordering::SeqCst);
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        unsafe { (*self.inner.get()).len() - size_of::<ChanPagePersist<[u8; 0]>>() }
    }

    #[inline]
    unsafe fn get_inner(&self) -> &ChanPagePersist {
        unsafe {
            &*(std::ptr::slice_from_raw_parts((*self.inner.get()).as_ptr(), self.len())
                as *const ChanPagePersist)
        }
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    unsafe fn get_inner_mut(&self) -> &mut ChanPagePersist {
        unsafe {
            &mut *(std::ptr::slice_from_raw_parts_mut((*self.inner.get()).as_mut_ptr(), self.len())
                as *mut ChanPagePersist)
        }
    }

    /// Safety: trust me
    pub(super) async unsafe fn new<P: AsRef<Path>>(
        path: P,
        len: IdxUsize,
    ) -> Result<Self, std::io::Error> {
        let size: u64 = size_of::<ChanPagePersist<[u8; 0]>>()
            .try_into()
            .expect("to be optimized out");
        let size: u64 = len as u64 + size;

        let f = File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.as_ref())
            .await?;

        f.set_len(size).await?;

        let raw = unsafe { memmap2::MmapMut::map_mut(&f)? };
        let raw = UnsafeCell::new(raw);

        Ok(ChanPage {
            inner: raw,
            _phantom: PhantomData,
        })
    }

    pub(super) fn push<V: AsRef<[u8]>>(&self, val: V) -> Result<(), ChanPageError> {
        let page = unsafe { self.get_inner_mut() };

        let (count, idx) = page.write_idx_count.fetch_add_high_low(
            size_of::<u32>() as u32 + val.as_ref().len() as u32,
            1,
            Ordering::Relaxed,
        );

        //trace! {
        //    %count,
        //    %idx
        //}

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

    pub(super) async fn pop(&self, group: usize) -> Result<&[u8], ChanPageError> {
        debug_assert!(group < MAX_WAITING_GROUPS);

        let page = unsafe { self.get_inner_mut() };
        let (count, _) = page.read_count_groups[group].fetch_add_low(1, Ordering::Relaxed);

        if count >= MAX_MAP_IDX_SLOTS as u32 {
            return Err(ChanPageError::PageFull);
        }

        let data = poll_fn(|cx| self.get_poll(cx, count)).await;

        let _ = page.read_count_groups[group].fetch_add_high(1, Ordering::Relaxed);
        data
    }

    fn get_poll(
        &self,
        cx: &mut Context<'_>,
        count: CountUsize,
    ) -> Poll<Result<&[u8], ChanPageError>> {
        let page = unsafe { self.get_inner() };

        let idx = page.map[count as usize]
            .idx_with_salt
            .load(Ordering::Acquire);

        let idx = match idx {
            _ if idx < SALT => {
                let wait_idx = page.map[count as usize]
                    .waiting_count
                    .fetch_add(1, Ordering::Relaxed);

                page.map[count as usize].waiters[wait_idx as usize].register(cx.waker());

                // second check to make sure that a writer didn't finish while we were registering
                let idx = page.map[count as usize]
                    .idx_with_salt
                    .load(Ordering::Acquire);

                match idx {
                    _ if idx < SALT => return Poll::Pending,
                    idx => idx - SALT,
                }
            }
            idx => idx - SALT,
        };

        if idx >= self.len() as u32 {
            if count < MAX_MAP_IDX_SLOTS as u32 - 1 {
                page.map[count as usize + 1]
                    .idx_with_salt
                    .store(u32::MAX, Ordering::Release);
                page.map[count as usize + 1].wake_all();
            }
            return Poll::Ready(Err(ChanPageError::PageFull));
        }

        let idx_offset = idx as usize + size_of::<u32>();

        let data_len = u32::from_le_bytes(page.data[idx as usize..idx_offset].try_into().unwrap());

        Poll::Ready(Ok(&page.data[idx_offset..idx_offset + data_len as usize]))
    }
}
