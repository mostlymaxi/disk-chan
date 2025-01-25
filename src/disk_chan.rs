use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

#[cfg(feature = "async")]
use tokio::sync::RwLock;

use std::sync::RwLock;

use crate::ChanPage;

pub struct DiskChan {
    count: AtomicUsize,
    max_pages: usize,
    page_size: usize,
    pages: RwLock<VecDeque<Arc<ChanPage>>>,
}

impl DiskChan {
    pub fn new(page_size: usize, max_pages: usize) -> Self {
        // TODO: load pages
        let pages = RwLock::new(VecDeque::new());

        DiskChan {
            count: AtomicUsize::new(0),
            max_pages,
            page_size,
            pages,
        }
    }

    #[cfg(not(feature = "async"))]
    pub fn get_page(&self, page_no: usize) -> (usize, Arc<ChanPage>) {
        // either we do lock contention or read the count a bunch
        // of times and i think the second option is better

        if page_no >= self.count.load(Ordering::Relaxed) {
            let mut pages = self.pages.write().expect("not poisoned");

            if page_no >= self.count.load(Ordering::Relaxed) {
                // TODO: catch overflow edge case (this can be super slow
                // because i expect this to happen once every 10,000 years)
                let count = self.count.fetch_add(1, Ordering::Relaxed);

                // might be off by one
                if count >= self.max_pages {
                    std::fs::remove_file(format!("test.{:X}", count - self.max_pages)).unwrap();
                    let _ = pages.pop_front();
                }

                let new_page =
                    unsafe { ChanPage::new(format!("test.{:X}", count), self.page_size).unwrap() };

                let new_page = Arc::new(new_page);

                pages.push_back(new_page.clone());

                return (count, new_page);
            }

            drop(pages);
        }

        let pages = self.pages.read().expect("not poisoned");
        let count = self.count.load(Ordering::Relaxed);
        let min_page = match count.cmp(&self.max_pages) {
            std::cmp::Ordering::Less => 0,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => count - self.max_pages,
        };

        let page_no = min_page.max(page_no);

        (page_no, pages[page_no - min_page].clone())
    }

    #[cfg(feature = "async")]
    pub async fn get_page(&self, page_no: usize) -> (usize, Arc<ChanPage>) {
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
                    tokio::fs::remove_file(format!("test.{:X}", count - self.max_pages))
                        .await
                        .unwrap();
                    let _ = pages.pop_front();
                }

                let new_page = unsafe {
                    ChanPage::new(format!("test.{:X}", count), self.page_size)
                        .await
                        .unwrap()
                };

                let new_page = Arc::new(new_page);

                pages.push_back(new_page.clone());

                return (count, new_page);
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

        (page_no, pages[page_no - min_page].clone())
    }
}
