use std::sync::Arc;

mod atomic_union;

#[cfg(feature = "async")]
mod disk_chan_page;
#[cfg(feature = "async")]
use disk_chan_page::*;

mod disk_chan_page_sync;
use disk_chan_page_sync::ChanPage;

mod disk_chan;
use disk_chan::DiskChan;

use tokio::sync::Barrier;

struct Consumer {
    current_page: usize,
    group: usize,
    local: Arc<ChanPage>,
    chan: Arc<DiskChan>,
}

impl Consumer {
    fn try_clone(&self) -> Result<Self, std::io::Error> {
        let (current_page, local) = self.chan.get_page(self.current_page);

        Ok(Consumer {
            current_page,
            group: self.group,
            local,
            chan: self.chan.clone(),
        })
    }

    fn recv(&self) -> Option<&[u8]> {
        match self.local.pop(self.group) {
            Ok(data) => Some(data),
            Err(_) => None,
        }
    }

    fn next_page(&mut self) {
        let (current_page, local) = self.chan.get_page(self.current_page + 1);
        self.current_page = current_page;
        self.local = local;
    }
}

//impl Consumer {
//    async fn try_clone(&self) -> Result<Self, std::io::Error> {
//        let (current_page, local) = self.chan.get_page(self.current_page).await;
//
//        Ok(Consumer {
//            current_page,
//            group: self.group,
//            local,
//            chan: self.chan.clone(),
//        })
//    }
//
//    async fn recv(&self) -> Option<&[u8]> {
//        match self.local.pop(self.group).await {
//            Ok(data) => Some(data),
//            Err(_) => None,
//        }
//    }
//
//    async fn next_page(&mut self) {
//        let (current_page, local) = self.chan.get_page(self.current_page + 1).await;
//        self.current_page = current_page;
//        self.local = local;
//    }
//}

#[derive(Clone)]
struct Producer {
    current_page: usize,
    local: Arc<ChanPage>,
    chan: Arc<DiskChan>,
}

impl Producer {
    pub fn subscribe(&self, group: usize) -> Consumer {
        let (current_page, local) = self.chan.get_page(0);
        let chan = self.chan.clone();

        Consumer {
            current_page,
            group,
            local,
            chan,
        }
    }

    pub fn send<V: AsRef<[u8]>>(&mut self, val: V) {
        loop {
            match self.local.push(&val) {
                Ok(()) => return,
                Err(_) => {}
            }

            let (current_page, local) = self.chan.get_page(self.current_page + 1);
            self.current_page = current_page;
            self.local = local;
        }
    }
}

fn new() -> Producer {
    let chan = DiskChan::new(2_usize.pow(24), 16);
    let chan = Arc::new(chan);
    let (current_page, local) = chan.get_page(0);

    Producer {
        current_page,
        local,
        chan,
    }
}

//async fn new() -> Producer {
//    let chan = DiskChan::new(2_usize.pow(24), 16);
//    let chan = Arc::new(chan);
//    let (current_page, local) = chan.get_page(0).await;
//
//    Producer {
//        current_page,
//        local,
//        chan,
//    }
//}

//impl Producer {
//    pub async fn subscribe(&self, group: usize) -> Consumer {
//        let (current_page, local) = self.chan.get_page(0).await;
//        let chan = self.chan.clone();
//
//        Consumer {
//            current_page,
//            group,
//            local,
//            chan,
//        }
//    }
//
//    pub async fn send<V: AsRef<[u8]>>(&mut self, val: V) {
//        loop {
//            match self.local.push(&val) {
//                Ok(()) => return,
//                Err(_) => {}
//            }
//
//            let (current_page, local) = self.chan.get_page(self.current_page + 1).await;
//            self.current_page = current_page;
//            self.local = local;
//        }
//    }
//}
//
//async fn new() -> Producer {
//    let chan = DiskChan::new(2_usize.pow(24), 16);
//    let chan = Arc::new(chan);
//    let (current_page, local) = chan.get_page(0).await;
//
//    Producer {
//        current_page,
//        local,
//        chan,
//    }
//}

fn main() {
    let messages = 5_000_000;
    let message = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    const NUM_THREADS: usize = 4;

    let tx = new();
    let rx = tx.subscribe(0);

    let barrier = Arc::new(std::sync::Barrier::new(NUM_THREADS * 2 + 1));

    let mut handles = Vec::new();

    for _ in 0..NUM_THREADS {
        let mut rx_c = rx.try_clone().unwrap();
        let barrier_c = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier_c.wait();
            for _ in 0..messages / NUM_THREADS {
                loop {
                    match rx_c.recv() {
                        Some(_) => break,
                        None => rx_c.next_page(),
                    }
                }
                // dbg!(str::from_utf8(rx.recv().await).unwrap());
            }
        }));
    }

    for _ in 0..NUM_THREADS {
        let mut tx_c = tx.clone();
        let barrier_c = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier_c.wait();
            for _ in 0..messages / NUM_THREADS {
                tx_c.send(message);
            }
        }));
    }

    barrier.wait();
    let now = std::time::SystemTime::now();

    for h in handles {
        let _ = h.join();
    }

    eprintln!("{:#?}", now.elapsed());
}
