use std::{
    cell::{Cell, UnsafeCell},
    future::poll_fn,
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        Arc, RwLock,
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::task::AtomicWaker;
use tokio::time::sleep;

struct Chan {
    count: AtomicUsize,
    count_strong: AtomicUsize,
    rx_count: AtomicUsize,

    waiting: AtomicU8,
    waiting_strong: AtomicU8,
    woken: AtomicU8,
    wakers: [AtomicWaker; u8::MAX as usize],

    data: UnsafeCell<[u8; 8192]>,
}

struct WaitingGroups<const G: usize, const N: usize> {
    group_count_weak: AtomicUsize,
    group_count: AtomicUsize,
    groups: [Arc<WaitingGroup<N>>; G],
}

impl<const G: usize, const N: usize> WaitingGroups<G, N> {
    pub fn wake_groups(&self) {
        let group_count = self.group_count.load(Ordering::Acquire);
        self.groups[..group_count].iter().for_each(|g| g.wake_one());
    }
}

struct WaitingGroup<const N: usize> {
    wake_count: AtomicUsize,
    waiting_count_weak: AtomicUsize,
    waiting_count: AtomicUsize,
    current_waker: AtomicWaker,
    wakers: [AtomicWaker; N],
}

impl<const N: usize> WaitingGroup<N> {
    pub fn enqueue(&self, cx: Context<'_>) {
        // WARN: we're waking a big assumption that we will never
        // have more than N waiters per group
        let idx = self.waiting_count_weak.fetch_add(1, Ordering::Relaxed);
        self.wakers[idx % N].register(cx.waker());
        self.waiting_count.fetch_add(1, Ordering::Release);
    }

    pub fn wake_one(&self) {
        let wake_count = self.wake_count.fetch_add(1, Ordering::Relaxed);
        let waiting_count = self.waiting_count.load(Ordering::Acquire);

        // no new waiters
        if wake_count == waiting_count {
            self.wake_count.fetch_sub(1, Ordering::Relaxed);
            return;
        }

        self.wakers[wake_count as usize].wake();
    }

    pub fn wake_all(&self) {
        let waiting_count = self.waiting_count.load(Ordering::Acquire);
        let wake_count = self.wake_count.swap(waiting_count, Ordering::Relaxed);

        let waiting_count = waiting_count % N;
        let wake_count = wake_count % N;

        match wake_count.cmp(&waiting_count) {
            std::cmp::Ordering::Less => self.wakers[wake_count as usize..waiting_count as usize]
                .iter()
                .for_each(|w| w.wake()),
            std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Greater => {
                self.wakers[waiting_count as usize..]
                    .iter()
                    .for_each(|w| w.wake());

                self.wakers[..wake_count as usize]
                    .iter()
                    .for_each(|w| w.wake());
            }
        }
    }
}

fn new() -> (Producer, Consumer) {
    let chan = Chan {
        count: AtomicUsize::new(0),
        count_strong: AtomicUsize::new(0),
        rx_count: AtomicUsize::new(0),
        waiting: AtomicU8::new(0),
        waiting_strong: AtomicU8::new(0),
        woken: AtomicU8::new(0),
        wakers: [const { AtomicWaker::new() }; u8::MAX as usize],
        data: UnsafeCell::new([0; 8192]),
    };

    let chan = Arc::new(chan);

    (
        Producer {
            inner: chan.clone(),
        },
        Consumer { inner: chan },
    )
}

impl Chan {
    fn recv(&self, cx: &mut Context<'_>) -> Poll<u8> {
        let total_count = self.count.load(Ordering::Acquire);
        let rx_count = self.rx_count.fetch_add(1, Ordering::Relaxed);

        if rx_count >= total_count {
            let line = self.waiting.fetch_add(1, Ordering::Relaxed);
            self.wakers[line as usize].register(cx.waker());
            self.waiting_strong.fetch_add(1, Ordering::Release);

            let total_count = self.count.load(Ordering::Acquire);

            if rx_count >= total_count {
                self.rx_count.fetch_sub(1, Ordering::Relaxed);
                return Poll::Pending;
            }
        }

        let data = self.data.read().expect("idgaf");

        Poll::Ready(data[rx_count])
    }

    fn send(&self, val: u8) {
        let idx = self.count.fetch_add(1, Ordering::Release);

        // SAFETY: trust me bro
        unsafe {
            let data = &mut *self.data.get();
            data[idx] = val;
        }

        let idx = self.waiting_strong.load(Ordering::Acquire);
        let start_idx = self.woken.swap(val, Ordering::Relaxed);

        match start_idx.cmp(&idx) {
            std::cmp::Ordering::Less => self.wakers[start_idx as usize..idx as usize]
                .iter()
                .for_each(|w| w.wake()),
            std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Greater => {
                self.wakers[start_idx as usize..]
                    .iter()
                    .for_each(|w| w.wake());

                self.wakers[..idx as usize].iter().for_each(|w| w.wake());
            }
        }

        self.count_strong.fetch_add(1, Ordering::Release);
    }
}

struct Producer {
    inner: Arc<Chan>,
}

impl Producer {
    fn send<V: AsRef<[u8]>>(&self, val: V) {
        self.inner.send(val.as_ref()[0])
    }
}

struct Consumer {
    inner: Arc<Chan>,
}

impl Consumer {
    async fn recv(&self) -> u8 {
        poll_fn(|cx| self.inner.recv(cx)).await
    }
}

#[tokio::main]
async fn main() {
    let (tx, rx) = new();
    let tx = Arc::new(tx);
    let rx = Arc::new(rx);

    for i in 0..4 {
        let rx_c = rx.clone();

        tokio::spawn(async move {
            loop {
                let v = rx_c.recv().await;
                println!("{i}: {v}");
            }
        });
    }

    sleep(Duration::from_secs(3)).await;

    for _ in 0..40 {
        tx.send(b"asdf");
    }

    sleep(Duration::from_secs(1)).await;
}
