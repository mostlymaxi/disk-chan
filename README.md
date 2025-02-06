# Disk Chan
An on-disk, (almost) lock-free, async, mpmc channel.

This is effectively a file system made for fast, concurrent message passing. Features
are kept to a minimum to reduce complexity as Disk Chan should be used as a building
block for other software with more safe guards in place.

Playing with message protocols on disk-backed files is inherently tricky and comes with
several quirks - this library should only be used if it is truly necessary as many use cases
should use other memory backed message queues such as [crossbeam](https://github.com/crossbeam-rs/crossbeam) and simply dump messages
to disk when needed.

See documentation on [docs.rs](https://docs.rs/disk-chan/latest/disk_chan/)

## Usage
```no_run
#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    const MESSAGE_COUNT: usize = 5_000_000;
    const MESSAGE: &str = "test message";
    const NUM_THREADS: usize = 4;

    let tx = disk_chan::new("/tmp/path-to-channel", 2_usize.pow(24), 16).await?;
    let rx = tx.subscribe(0).await?;

    let mut handles = Vec::new();

    for _ in 0..NUM_THREADS {
        let mut rx_c = rx.try_clone().await?;

        handles.push(tokio::spawn(async move {
            for _ in 0..MESSAGE_COUNT / NUM_THREADS {
                loop {
                    match rx_c.recv().await {
                        Some(m) => {
                            assert_eq!(m, MESSAGE.as_bytes());
                            break;
                        }
                        None => rx_c.next_page().await.unwrap(),
                    }
                }
            }
        }));
    }

    for i in 0..NUM_THREADS {
        let mut tx_c = tx.clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..MESSAGE_COUNT / NUM_THREADS {
                tx_c.send(MESSAGE).await.unwrap();
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    Ok(())
}
```

see [tests](https://github.com/mostlymaxi/disk-chan/tree/main/tests) for more examples.

## Page Sizing
Disk Chan takes a `page_size` representing the maximum number of bytes per page and
a `max_pages` for the number of pages to have on disk at any moment.

For example,
if you want the channel to hold 4GB of data (~ 2^32 bytes) you can create a channel
with a `page_size` of 2^28 bytes and 16 `max_pages`.

Note that all pages have a maximum number of `2^16 - 1` messages per page so you
want to optimize the `page_size` to be approximately `average message size * (2^16 - 1)`
and adjust `max_pages` to tune the amount of data stored.


