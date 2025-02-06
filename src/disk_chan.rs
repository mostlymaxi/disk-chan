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

impl std::fmt::Debug for DiskChan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskChan")
            .field("path", &self.path)
            .field("count", &self.count)
            .field("max_pages", &self.max_pages)
            .field("page_size", &self.page_size)
            .finish_non_exhaustive()
    }
}

impl Drop for DiskChan {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.path.join(".pid.lock"));
    }
}

impl DiskChan {
    fn get_lock<P: AsRef<Path>>(path: P) -> Result<File, std::io::Error> {
        let mut lock = match std::fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .truncate(true)
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

        Ok(lock)
    }

    fn parse_page_no(name: String) -> Option<usize> {
        let mut name_itr = name.split('.');

        if name_itr.next()? != "data" {
            return None;
        }
        let Some(i) = name_itr.next().and_then(|s| base62::decode(s).ok()) else {
            return None;
        };
        if name_itr.next()? != "bin" {
            return None;
        }
        if name_itr.next().is_some() {
            return None;
        }

        i.try_into().ok()
    }

    async fn load_pages_from_path<P: AsRef<Path>>(path: P) -> (usize, VecDeque<Arc<ChanPage>>) {
        let mut pages_unordered = Vec::new();

        for entry in std::fs::read_dir(path).expect("path exists") {
            let Ok(entry) = entry else { continue };
            let Ok(meta) = entry.metadata() else { continue };
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            let path = entry.path();
            let Some(page_no) = Self::parse_page_no(name) else {
                continue;
            };
            let Ok(mut page) = (unsafe { ChanPage::new(path, meta.len() as usize).await }) else {
                continue;
            };

            unsafe {
                page.reset_all_waiters();
                page.reset_read_count_groups();
            }

            pages_unordered.push((page_no, Arc::new(page)));
        }

        pages_unordered.sort_by(|a, b| a.0.cmp(&b.0));
        let count = pages_unordered.last().map(|(c, _)| *c + 1).unwrap_or(0);
        let pages = pages_unordered.into_iter().map(|(_, page)| page).collect();

        (count, pages)
    }

    pub(crate) async fn new<P: AsRef<Path>>(
        path: P,
        page_size: usize,
        max_pages: usize,
    ) -> Result<Self, std::io::Error> {
        let _ = std::fs::create_dir_all(path.as_ref());
        let lock = Self::get_lock(path.as_ref())?;

        let (count, mut pages) = Self::load_pages_from_path(&path).await;

        // if i load pages 3, 4, 5, 6 w/ max_pages = 2
        // pages.len() = 4
        if pages.len() > max_pages {
            // logic here is rough but i'm pretty sure it's correct
            // off by one errors all over the place ugh
            //                                3 ..= 4
            //          6   - (    4       - 1 )      6   -     2
            for i in (count - (pages.len() - 1))..=(count - max_pages) {
                let _ = pages.pop_front();
                let num: u64 = i.try_into().expect("to be optimized out");
                if let Err(e) = std::fs::remove_file(
                    path.as_ref()
                        .join(format!("data.{}.bin", base62::encode_fmt(num))),
                ) {
                    eprintln!("something went wrong with page cleanup... channel may be corrupted");
                    return Err(e);
                }
            }
        }

        let pages = RwLock::new(pages);

        Ok(DiskChan {
            path: path.as_ref().into(),
            _lock: lock,
            count: AtomicUsize::new(count),
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
                pages.make_contiguous();

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

        // make_contiguous ensures that all pages are in the first slice, in order
        let (pages_slice, _) = pages.as_slices();

        Ok((page_no, pages_slice[page_no - min_page].clone()))
    }
}
