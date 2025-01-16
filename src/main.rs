use core::str;
use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    future::poll_fn,
    sync::{
        atomic::{AtomicU32, AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::task::AtomicWaker;
use mmapcell::MmapCell;

mod atomic_union;
use atomic_union::AtomicUnion;

const MAX_WAITING_GROUPS: usize = 16;

// this can potentially be pulled out into memory
// for more optimization
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

// #[repr(C)]
struct ChanPage {
    write_idx_count: AtomicUnion,
    map: [MapIdxSlot; u16::MAX as usize],
    data: [u8; u32::MAX as usize],
}

type UnsafeChanPage = UnsafeCell<MmapCell<ChanPage>>;

#[derive(Clone)]
struct Chan {
    rx_group_count: Arc<AtomicUsize>,
    // this is VERY unsafe and is only done because
    // im using "logical" synchronization to assure
    // that no two indexes are written to at the same time
    page: Arc<UnsafeChanPage>, // page: VecDeque<Arc<UnsafeChanPage>>,
}

impl Drop for Chan {
    fn drop(&mut self) {
        let page = unsafe { (&mut *self.page.get()).get_mut() };
        for idx in &mut page.map {
            idx.waiting_count.store(0, Ordering::SeqCst);
            idx.waiters = [const { AtomicWaker::new() }; MAX_WAITING_GROUPS];
        }
    }
}

unsafe impl Sync for Chan {}
unsafe impl Send for Chan {}

const SALT: u32 = 1;

impl Chan {
    fn new() -> Self {
        let page = unsafe { MmapCell::new_named("test").unwrap() };
        let page = Arc::new(UnsafeCell::new(page));
        // let page = VecDeque::from(vec![page]);

        Chan {
            rx_group_count: Arc::new(AtomicUsize::new(0)),
            page,
        }
    }

    fn push<V: AsRef<[u8]>>(&self, val: V) {
        let page = unsafe { (&mut *self.page.get()).get_mut() };

        let (count, idx) = page.write_idx_count.fetch_add_high_low(
            size_of::<u32>() as u32 + val.as_ref().len() as u32,
            1,
            Ordering::Relaxed,
        );

        eprintln!("{count} {idx}");

        let idx_offset = idx as usize + size_of::<u32>();

        page.data[idx as usize..idx_offset]
            .copy_from_slice(&(val.as_ref().len() as u32).to_le_bytes());
        page.data[idx_offset..idx_offset + val.as_ref().len()].copy_from_slice(val.as_ref());

        page.map[count as usize]
            .idx_with_salt
            .store(idx + SALT, Ordering::Release);

        page.map[count as usize].wake_all();
    }

    async fn get(&self, count: u32) -> &[u8] {
        poll_fn(|cx| self.get_poll(cx, count)).await
    }

    fn get_poll(&self, cx: &mut Context<'_>, count: u32) -> Poll<&[u8]> {
        let page = unsafe { (&*self.page.get()).get() };

        let idx = page.map[count as usize]
            .idx_with_salt
            .load(Ordering::Acquire);

        eprintln!("idx = {idx}");

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

#[tokio::main]
async fn main() {
    let chan = Chan::new();
    let chan2 = chan.clone();
    let handle = tokio::spawn(async move {
        dbg!(str::from_utf8(chan.get(1).await).unwrap());
    });

    tokio::time::sleep(Duration::from_secs(1)).await;
    // chan2.push("asdf");
    // chan2.push("hello");
    //
    let _ = handle.await;
}
