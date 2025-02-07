use disk_chan::*;

#[tokio::test]
async fn seq() {
    const MESSAGE_COUNT: usize = 5_000_000;
    const MESSAGE: &str = const_str::repeat!("a", 100);

    tracing_subscriber::fmt::init();

    let mut tx = new("/tmp/disk-chan-test/seq", 2_u32.pow(24), usize::MAX)
        .await
        .unwrap();
    let mut rx = tx.subscribe(0).await.unwrap();

    let now = std::time::SystemTime::now();

    for _ in 0..MESSAGE_COUNT {
        tx.send(MESSAGE).await.unwrap();
    }

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

    eprintln!("{:#?}", now.elapsed());
    let _ = std::fs::remove_dir_all("/tmp/disk-chan-test/seq");
}
