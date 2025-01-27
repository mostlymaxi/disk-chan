use std::{path::Path, sync::Arc};

mod atomic_union;

mod disk_chan_page;
use disk_chan_page::*;

mod disk_chan;
use disk_chan::DiskChan;

pub struct Consumer {
    current_page: usize,
    group: usize,
    local: Arc<ChanPage>,
    chan: Arc<DiskChan>,
}

impl Consumer {
    pub async fn try_clone(&self) -> Result<Self, std::io::Error> {
        let (current_page, local) = self.chan.get_page(self.current_page).await?;

        Ok(Consumer {
            current_page,
            group: self.group,
            local,
            chan: self.chan.clone(),
        })
    }

    pub async fn recv(&self) -> Option<&[u8]> {
        match self.local.pop(self.group).await {
            Ok(data) => Some(data),
            Err(_) => None,
        }
    }

    pub async fn next_page(&mut self) -> Result<(), std::io::Error> {
        let (current_page, local) = self.chan.get_page(self.current_page + 1).await?;
        self.current_page = current_page;
        self.local = local;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Producer {
    current_page: usize,
    local: Arc<ChanPage>,
    chan: Arc<DiskChan>,
}

pub async fn new<P: AsRef<Path>>(
    path: P,
    page_size: usize,
    max_pages: usize,
) -> Result<Producer, std::io::Error> {
    let chan = DiskChan::new(path, page_size, max_pages)?;
    let chan = Arc::new(chan);
    let (current_page, local) = chan.get_page(0).await?;

    Ok(Producer {
        current_page,
        local,
        chan,
    })
}

impl Producer {
    /// Clone the [Producer]. This is actually infallible, but exists
    /// to have consistency with the [Consumer] API.
    pub async fn try_clone(&self) -> Result<Self, std::io::Error> {
        Ok(self.clone())
    }

    pub async fn subscribe(&self, group: usize) -> Result<Consumer, std::io::Error> {
        let (current_page, local) = self.chan.get_page(0).await?;
        let chan = self.chan.clone();

        Ok(Consumer {
            current_page,
            group,
            local,
            chan,
        })
    }

    pub async fn send<V: AsRef<[u8]>>(&mut self, val: V) -> Result<(), std::io::Error> {
        loop {
            match self.local.push(&val) {
                Ok(()) => return Ok(()),
                Err(_) => {}
            }

            let (current_page, local) = self.chan.get_page(self.current_page + 1).await?;
            self.current_page = current_page;
            self.local = local;
        }
    }
}
