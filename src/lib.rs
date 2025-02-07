#![doc = include_str!("../README.md")]

use std::{path::Path, sync::Arc};

mod atomic_union;

mod disk_chan_page;
use disk_chan_page::{ChanPage, IdxUsize};

mod disk_chan;
use disk_chan::DiskChan;

/// Thread-safe Consumer type with a group number representing the consumer group.
///
/// Use [try_clone](Self::try_clone()) to make copies of the [Consumer] in the same group or
/// use [subscribe](Producer::subscribe()) on a producer to create new Consumers.
///
/// ```no_run
/// # async fn consumer_example() -> Result<(), std::io::Error> {
/// let mut tx = disk_chan::new("foo", 2_u32.pow(24), 1).await?;
/// let mut rx = tx.subscribe(0).await?;
/// let mut rx2 = rx.try_clone().await?;
///
/// let msg = loop {
///     match rx.recv().await {
///         Some(msg) => break msg,
///         None => rx.next_page().await?,
///     }
/// };
/// # Ok(())
/// # }
/// ```
pub struct Consumer {
    current_page: usize,
    group: usize,
    local: Arc<ChanPage>,
    chan: Arc<DiskChan>,
}

impl std::fmt::Debug for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("current_page", &self.current_page)
            .field("group", &self.group)
            .field("chan", &self.chan)
            .finish_non_exhaustive()
    }
}

impl Consumer {
    /// Attempts to clone the current [Consumer] with the same group.
    ///
    /// This can potentially fail to do interactions with the file system, but
    /// should be near impossible with correct usage.
    pub async fn try_clone(&self) -> Result<Self, std::io::Error> {
        let (current_page, local) = self.chan.get_page(self.current_page).await?;

        Ok(Consumer {
            current_page,
            group: self.group,
            local,
            chan: self.chan.clone(),
        })
    }

    /// Asynchronously receives the next message in the consumer group.
    ///
    /// Returns [None] when the consumer has read all messages from the current page,
    /// and the user must call [next_page](Consumer::next_page()) to continue reading.
    ///
    /// This is necessary due to the message's lifetime being tied to the consumer's current page so it must
    /// not be accessed after a call to [next_page](Consumer::next_page()). Thus, it is up to the
    /// user to ensure the compiler is happy with lifetimes.
    ///
    /// ```no_run
    /// # async fn consumer_example() -> Result<(), std::io::Error> {
    /// let mut tx = disk_chan::new("foo", 2_u32.pow(24), 1).await?;
    /// let mut rx = tx.subscribe(0).await?;
    /// let mut rx2 = rx.try_clone().await?;
    ///
    /// let msg = loop {
    ///     match rx.recv().await {
    ///         Some(msg) => break msg,
    ///         None => rx.next_page().await?,
    ///     }
    /// };
    /// # Ok(())
    /// # }
    /// ```
    pub async fn recv(&self) -> Option<&[u8]> {
        match self.local.pop(self.group).await {
            Ok(data) => Some(data),
            Err(_) => None,
        }
    }

    /// Sets the internal page to the next available one.
    ///
    /// Should be called after [recv](Self::recv()) returns [None].
    ///
    /// ```no_run
    /// # async fn consumer_example() -> Result<(), std::io::Error> {
    /// let mut tx = disk_chan::new("foo", 2_u32.pow(24), 1).await?;
    /// let mut rx = tx.subscribe(0).await?;
    /// let mut rx2 = rx.try_clone().await?;
    ///
    /// let msg = loop {
    ///     match rx.recv().await {
    ///         Some(msg) => break msg,
    ///         None => rx.next_page().await?,
    ///     }
    /// };
    /// # Ok(())
    /// # }
    /// ```
    pub async fn next_page(&mut self) -> Result<(), std::io::Error> {
        let (current_page, local) = self.chan.get_page(self.current_page + 1).await?;
        self.current_page = current_page;
        self.local = local;
        Ok(())
    }
}

/// Thread-safe Producer type.
///
/// use [disk_chan::new](new) to create a new [Producer] for a channel. This will lock
/// the channel to the process as only one process should own the channel at any time.
///
/// Use [clone](Self::clone()) to make copies of the Producer. Note that
/// [try_clone](Self::try_clone) also exists but is solely for consistency with the
/// [Consumer] api and cannot actually fail.
///
/// ```no_run
/// # async fn producer_example() -> Result<(), std::io::Error> {
/// let mut tx = disk_chan::new("foo", 2_u32.pow(24), 1).await?;
///
/// tx.send("test").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Producer {
    current_page: usize,
    local: Arc<ChanPage>,
    chan: Arc<DiskChan>,
}

impl std::fmt::Debug for Producer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("current_page", &self.current_page)
            .field("chan", &self.chan)
            .finish_non_exhaustive()
    }
}

/// Creates a new channel and returns the [Producer] for that channel.
///
/// Call [Producer::subscribe()] to create a [Consumer] for the channel.
///
/// Takes a `page_size` representing the maximum number of bytes per page and
/// a `max_pages` for the number of pages to have on disk at any moment.
///
/// For example,
/// if you want the channel to hold 4GB of data (~ 2^32 bytes) you can create a channel
/// with a `page_size` of 2^28 bytes and 16 `max_pages`.
///
/// Note that all pages have a maximum number of `2^16 - 1` messages per page so you
/// want to optimize the `page_size` to be approximately `average message size * (2^16 - 1)`
/// and adjust `max_pages` to tune the amount of data stored.
pub async fn new<P: AsRef<Path>>(
    path: P,
    page_size: IdxUsize,
    max_pages: usize,
) -> Result<Producer, std::io::Error> {
    let chan = DiskChan::new(path, page_size, max_pages).await?;
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

    /// Creates a new [Consumer] with the given group.
    ///
    /// This is thread safe and can be called multiple times with the same group, but it
    /// is generally recommend to clone existing consumers.
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

    /// Asynchronously sends a message to the channel.
    ///
    /// ```no_run
    /// # async fn producer_example() -> Result<(), std::io::Error> {
    /// let mut tx = disk_chan::new("foo", 2_u32.pow(24), 1).await?;
    ///
    /// tx.send("test").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send<V: AsRef<[u8]>>(&mut self, val: V) -> Result<(), std::io::Error> {
        loop {
            #[allow(clippy::single_match)]
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
