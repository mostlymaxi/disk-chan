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

const MAX_WAITING_GROUPS: usize = 16;

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
struct ChanPagePersist<Data: ?Sized = [u8]> {
    write_idx_count: AtomicUnion,
    read_count_groups: [AtomicUnion; MAX_WAITING_GROUPS],
    map: [MapIdxSlot; u16::MAX as usize],
    data: Data,
}

pub struct ChanPage {
    inner: UnsafeCell<MmapMut>,
    _phantom: PhantomData<ChanPagePersist>,
}

unsafe impl Send for ChanPage {}
unsafe impl Sync for ChanPage {}

const SALT: u32 = 1;

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

    pub async unsafe fn new<P: AsRef<Path>>(path: P, len: usize) -> Result<Self, std::io::Error> {
        let size = size_of::<ChanPagePersist<[u8; 0]>>();
        let size = len + size;

        let f = File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(path.as_ref())
            .await?;

        f.set_len(size as u64).await?;

        let raw = unsafe { memmap2::MmapMut::map_mut(&f)? };
        let raw = UnsafeCell::new(raw);

        Ok(ChanPage {
            inner: raw,
            _phantom: PhantomData,
        })
    }

    pub fn push<V: AsRef<[u8]>>(&self, val: V) {
        let page = unsafe { self.get_inner_mut() };

        let (count, idx) = page.write_idx_count.fetch_add_high_low(
            size_of::<u32>() as u32 + val.as_ref().len() as u32,
            1,
            Ordering::Relaxed,
        );

        let idx_offset = idx as usize + size_of::<u32>();

        page.data[idx as usize..idx_offset]
            .copy_from_slice(&(val.as_ref().len() as u32).to_le_bytes());
        page.data[idx_offset..idx_offset + val.as_ref().len()].copy_from_slice(val.as_ref());

        page.map[count as usize]
            .idx_with_salt
            .store(idx + SALT, Ordering::Release);

        page.map[count as usize].wake_all();
    }

    pub async fn pop(&self, group: usize) -> &[u8] {
        let page = unsafe { self.get_inner_mut() };
        let (count, _) = page.read_count_groups[group].fetch_add_low(1, Ordering::Relaxed);

        let data = poll_fn(|cx| self.get_poll(cx, count)).await;

        let _ = page.read_count_groups[group].fetch_add_high(1, Ordering::Relaxed);
        data
    }

    fn get_poll(&self, cx: &mut Context<'_>, count: u32) -> Poll<&[u8]> {
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

        let idx_offset = idx as usize + size_of::<u32>();

        let data_len = u32::from_le_bytes(page.data[idx as usize..idx_offset].try_into().unwrap());

        Poll::Ready(&page.data[idx_offset..idx_offset + data_len as usize])
    }
}
