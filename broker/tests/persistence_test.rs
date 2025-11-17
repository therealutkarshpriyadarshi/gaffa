/// Integration test for broker persistence across restarts
use common::config::BrokerConfig;
use protocol::{Message, Request, Response};
use storage::TopicManager;

#[tokio::test]
async fn test_broker_persistence_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap().to_string();

    // Phase 1: Create topics and produce messages
    {
        let topic_manager = TopicManager::new(&data_dir).unwrap();

        // Create two topics
        topic_manager.create_topic("topic1".to_string(), 2).unwrap();
        topic_manager.create_topic("topic2".to_string(), 1).unwrap();

        // Produce messages to topic1, partition 0
        let topic1 = topic_manager.get_topic("topic1").unwrap();
        let messages1 = vec![
            Message::new(b"message1".to_vec()).with_key(b"key1".to_vec()),
            Message::new(b"message2".to_vec()).with_key(b"key2".to_vec()),
            Message::new(b"message3".to_vec()).with_key(b"key3".to_vec()),
        ];
        topic1.append(0, messages1).await.unwrap();

        // Produce messages to topic2, partition 0
        let topic2 = topic_manager.get_topic("topic2").unwrap();
        let messages2 = vec![
            Message::new(b"data1".to_vec()),
            Message::new(b"data2".to_vec()),
        ];
        topic2.append(0, messages2).await.unwrap();
    }

    // Phase 2: Restart broker (simulate by reopening TopicManager)
    {
        let topic_manager = TopicManager::open(&data_dir).unwrap();

        // Verify topics exist
        assert_eq!(topic_manager.topic_count(), 2);

        // Verify topic1
        let topic1 = topic_manager.get_topic("topic1").unwrap();
        assert_eq!(topic1.num_partitions(), 2);

        let records = topic1.fetch(0, 0, 10).await.unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].message.value, b"message1");
        assert_eq!(records[0].message.key, Some(b"key1".to_vec()));
        assert_eq!(records[1].message.value, b"message2");
        assert_eq!(records[2].message.value, b"message3");

        // Verify topic2
        let topic2 = topic_manager.get_topic("topic2").unwrap();
        assert_eq!(topic2.num_partitions(), 1);

        let records = topic2.fetch(0, 0, 10).await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].message.value, b"data1");
        assert_eq!(records[1].message.value, b"data2");

        // Produce more messages after restart
        let messages3 = vec![
            Message::new(b"message4".to_vec()),
        ];
        topic1.append(0, messages3).await.unwrap();
    }

    // Phase 3: Restart again and verify all messages
    {
        let topic_manager = TopicManager::open(&data_dir).unwrap();

        let topic1 = topic_manager.get_topic("topic1").unwrap();
        let records = topic1.fetch(0, 0, 10).await.unwrap();
        assert_eq!(records.len(), 4);
        assert_eq!(records[0].message.value, b"message1");
        assert_eq!(records[1].message.value, b"message2");
        assert_eq!(records[2].message.value, b"message3");
        assert_eq!(records[3].message.value, b"message4");
    }
}

#[tokio::test]
async fn test_segment_rotation_persistence() {
    use storage::SegmentConfig;

    let dir = tempfile::tempdir().unwrap();

    // Create a partition with small segment size to force rotation
    let config = SegmentConfig {
        max_size: 500, // Very small for testing
        index_interval: 5,
    };

    let partition = storage::Partition::with_config(
        "test-topic".to_string(),
        0,
        dir.path(),
        config.clone(),
    )
    .unwrap();

    // Write enough messages to trigger multiple segment rotations
    for i in 0..20 {
        let messages = vec![Message::new(format!("message{}", i).into_bytes())];
        partition.append(messages).await.unwrap();
    }

    partition.flush().await.unwrap();

    // Reopen and verify all messages are readable
    let partition = storage::Partition::open_with_config(
        "test-topic".to_string(),
        0,
        dir.path(),
        config,
    )
    .unwrap();

    let records = partition.fetch(0, 100).await.unwrap();
    assert_eq!(records.len(), 20);

    for (i, record) in records.iter().enumerate() {
        assert_eq!(
            record.message.value,
            format!("message{}", i).into_bytes()
        );
        assert_eq!(record.offset, i as u64);
    }
}

#[tokio::test]
async fn test_concurrent_writes_and_reads() {
    let dir = tempfile::tempdir().unwrap();
    let topic_manager = TopicManager::new(dir.path()).unwrap();

    topic_manager.create_topic("concurrent-topic".to_string(), 1).unwrap();
    let topic = topic_manager.get_topic("concurrent-topic").unwrap();

    // Spawn multiple writers
    let mut write_handles = vec![];
    for i in 0..5 {
        let topic_clone = topic.clone();
        let handle = tokio::spawn(async move {
            for j in 0..10 {
                let message = Message::new(format!("writer{}:msg{}", i, j).into_bytes());
                topic_clone.append(0, vec![message]).await.unwrap();
            }
        });
        write_handles.push(handle);
    }

    // Wait for all writes to complete
    for handle in write_handles {
        handle.await.unwrap();
    }

    // Verify all 50 messages were written
    let records = topic.fetch(0, 0, 100).await.unwrap();
    assert_eq!(records.len(), 50);

    // Reopen and verify persistence
    drop(topic);
    drop(topic_manager);

    let topic_manager = TopicManager::open(dir.path()).unwrap();
    let topic = topic_manager.get_topic("concurrent-topic").unwrap();
    let records = topic.fetch(0, 0, 100).await.unwrap();
    assert_eq!(records.len(), 50);
}
