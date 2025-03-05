use disk_chan::*;

#[tokio::test]
async fn seq() {
    const MESSAGE_COUNT: usize = 5_000_000;
    const MESSAGE: &str = const_str::repeat!("a", 100);

    tracing_subscriber::fmt::init();

    let mut tx = new("/tmp/disk-chan-test/seq", 2_u32.pow(23), usize::MAX)
        .await
        .unwrap();
    let mut rx = tx.subscribe(0).await.unwrap();

    let now_1 = std::time::SystemTime::now();

    for _ in 0..MESSAGE_COUNT {
        tx.send(MESSAGE).await.unwrap();
    }

    let elapsed = now_1.elapsed().unwrap();

    eprintln!(
        "sent {} bytes over {} ms for a total of {:.2} MB/s. {} ns per iter",
        MESSAGE.bytes().len() * MESSAGE_COUNT,
        elapsed.as_millis(),
        MESSAGE.bytes().len() as f64 * (MESSAGE_COUNT as f64 / elapsed.as_micros() as f64),
        elapsed.as_nanos() / MESSAGE_COUNT as u128,
    );

    let now_2 = std::time::SystemTime::now();

    for _ in 0..MESSAGE_COUNT {
        loop {
            match rx.recv().await {
                Some(m) => {
                    assert_eq!(m, MESSAGE.as_bytes());
                    break;
                }
                None => rx.next_page().await.unwrap(),
            }
        }
    }

    let elapsed = now_2.elapsed().unwrap();
    let elapsed_total = now_1.elapsed().unwrap();

    eprintln!(
        "received {} bytes over {} ms for a total of {:.2} MB/s. {} ns per iter",
        MESSAGE.bytes().len() * MESSAGE_COUNT,
        elapsed.as_millis(),
        MESSAGE.bytes().len() as f64 * (MESSAGE_COUNT as f64 / elapsed.as_micros() as f64),
        elapsed.as_nanos() / MESSAGE_COUNT as u128,
    );

    eprintln!(
        "sent + received {} bytes over {} ms for a total of {:.2} MB/s. {} ns per iter",
        MESSAGE.bytes().len() * MESSAGE_COUNT,
        elapsed_total.as_millis(),
        MESSAGE.bytes().len() as f64 * (MESSAGE_COUNT as f64 / elapsed_total.as_micros() as f64),
        elapsed_total.as_nanos() / MESSAGE_COUNT as u128,
    );

    let _ = std::fs::remove_dir_all("/tmp/disk-chan-test/seq");
}
