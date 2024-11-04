use std::{
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
    rx_count: AtomicUsize,
    data: RwLock<Vec<u8>>,

    // if lock_state = 0 => noone is writing
    // if lock_state = 1 => someone is writing
    // if lock_state = 2 => reseting
    waiting: AtomicU8,
    wakers: [AtomicWaker; u8::MAX as usize],
    // AtomicVecDeque
    // waker_linked_list
}

fn new() -> (Producer, Consumer) {
    let chan = Chan {
        count: AtomicUsize::new(0),
        rx_count: AtomicUsize::new(0),
        data: RwLock::new(Vec::new()),
        waiting: AtomicU8::new(0),
        wakers: [const { AtomicWaker::new() }; u8::MAX as usize],
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
        let _ = self.count.fetch_add(1, Ordering::Release);
        // THIS IS A LOCK
        // but i also know how to not make it lock
        let mut data = self.data.write().expect("to not fail");
        data.push(val);

        // this is the part i dont know how
        // to not make lock
        let idx = self.waiting.fetch_min(0, Ordering::Relaxed);
        self.wakers[0..idx as usize].iter().for_each(|w| w.wake());
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
