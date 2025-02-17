use std::sync::Arc;

use disk_chan::*;
use tokio::sync::Barrier;
use tracing::info;

#[tokio::test]
async fn mpmc() {
    const MESSAGE_COUNT: usize = 5_000_000;
    const MESSAGE: &str = const_str::repeat!("a", 100);
    const NUM_THREADS: usize = 4;

    tracing_subscriber::fmt::init();

    let tx = new("/tmp/disk-chan-test/mpmc", 2_u32.pow(24), 16)
        .await
        .unwrap();
    let rx = tx.subscribe(0).await.unwrap();

    let barrier = Arc::new(Barrier::new(NUM_THREADS * 2 + 1));

    let mut handles = Vec::new();

    for i in 0..NUM_THREADS {
        let mut rx_c = rx.try_clone().await.unwrap();
        let barrier_c = barrier.clone();

        handles.push(tokio::spawn(async move {
            barrier_c.wait().await;
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

            info!("{i} done reading!");
        }));
    }

    for i in 0..NUM_THREADS {
        let mut tx_c = tx.clone();
        let barrier_c = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier_c.wait().await;
            for _ in 0..MESSAGE_COUNT / NUM_THREADS {
                tx_c.send(MESSAGE).await.unwrap();
            }

            info!("{i} done writing!");
        }));
    }

    barrier.wait().await;
    let now = std::time::SystemTime::now();

    for h in handles {
        let _ = h.await;
    }

    eprintln!("{:#?}", now.elapsed());
    let _ = std::fs::remove_dir_all("/tmp/disk-chan-test/mpmc");
}
