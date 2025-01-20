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

mod atomic_union;

mod disk_chan_page;
use disk_chan_page::*;

mod disk_chan;
use disk_chan::DiskChan;

#[derive(Clone)]
struct Consumer {
    current_page: usize,
    group: usize,
    local: Arc<ChanPage>,
    chan: Arc<DiskChan>,
}

impl Consumer {
    async fn recv(&self) -> &[u8] {
        self.local.pop(self.group).await
    }
}

#[derive(Clone)]
struct Producer {
    current_page: usize,
    local: Arc<ChanPage>,
    chan: Arc<DiskChan>,
}

impl Producer {
    pub async fn subscribe(&self, group: usize) -> Consumer {
        let (current_page, local) = self.chan.get_page(0).await;
        let chan = self.chan.clone();

        Consumer {
            current_page,
            group,
            local,
            chan,
        }
    }

    pub async fn send<V: AsRef<[u8]>>(&self, val: V) {
        self.local.push(val);
    }
}

async fn new() -> Producer {
    let chan = DiskChan::new(2_usize.pow(32), 4);
    let chan = Arc::new(chan);
    let (current_page, local) = chan.get_page(0).await;

    Producer {
        current_page,
        local,
        chan,
    }
}

#[tokio::main]
async fn main() {
    let tx = new().await;
    let rx = tx.subscribe(0).await;

    let handle = tokio::spawn(async move {
        for _ in 0..99 {
            dbg!(str::from_utf8(rx.recv().await).unwrap());
        }
    });

    tokio::time::sleep(Duration::from_secs(1)).await;

    for i in 0..99_usize {
        tx.send(i.to_le_bytes()).await;
    }

    // chan2.push("asdf");
    // chan2.push("hello");
    //
    let _ = handle.await;
}
