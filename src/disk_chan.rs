use std::{
    collections::VecDeque,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use tokio::sync::RwLock;

use crate::ChanPage;

pub struct DiskChan {
    path: PathBuf,
    _lock: File,
    count: AtomicUsize,
    max_pages: usize,
    page_size: usize,
    pages: RwLock<VecDeque<Arc<ChanPage>>>,
}

impl DiskChan {
    pub fn new<P: AsRef<Path>>(
        path: P,
        page_size: usize,
        max_pages: usize,
    ) -> Result<Self, std::io::Error> {
        let _ = std::fs::create_dir_all(path.as_ref());

        let mut lock = match std::fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path.as_ref().join(".pid.lock"))
        {
            Ok(lock) => lock,
            Err(e) => {
                eprintln!("only one DiskChan process should own a path at any time");
                return Err(e);
            }
        };

        lock.write_all(std::process::id().to_string().as_bytes())?;

        // TODO: load pages
        let pages = RwLock::new(VecDeque::new());

        Ok(DiskChan {
            path: path.as_ref().into(),
            _lock: lock,
            count: AtomicUsize::new(0),
            max_pages,
            page_size,
            pages,
        })
    }

    fn get_page_path(&self, num: usize) -> PathBuf {
        let num: u64 = num.try_into().expect("to be optimized out");

        self.path
            .join(format!("data.{}.bin", base62::encode_fmt(num)))
    }

    pub async fn get_page(&self, page_no: usize) -> Result<(usize, Arc<ChanPage>), std::io::Error> {
        // either we do lock contention or read the count a bunch
        // of times and i think the second option is better

        if page_no >= self.count.load(Ordering::Relaxed) {
            let mut pages = self.pages.write().await;

            if page_no >= self.count.load(Ordering::Relaxed) {
                // TODO: catch overflow edge case (this can be super slow
                // because i expect this to happen once every 10,000 years)
                let count = self.count.fetch_add(1, Ordering::Relaxed);

                // might be off by one
                if count >= self.max_pages {
                    tokio::fs::remove_file(self.get_page_path(count - self.max_pages)).await?;
                    let _ = pages.pop_front();
                }

                let new_page =
                    unsafe { ChanPage::new(self.get_page_path(count), self.page_size).await? };

                let new_page = Arc::new(new_page);

                pages.push_back(new_page.clone());

                return Ok((count, new_page));
            }

            drop(pages);
        }

        let pages = self.pages.read().await;
        let count = self.count.load(Ordering::Relaxed);
        let min_page = match count.cmp(&self.max_pages) {
            std::cmp::Ordering::Less => 0,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => count - self.max_pages,
        };

        let page_no = min_page.max(page_no);

        Ok((page_no, pages[page_no - min_page].clone()))
    }
}
