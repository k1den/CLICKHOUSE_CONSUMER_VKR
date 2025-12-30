package org.k1den.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.k1den.config.ClickHouseConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaToClickHouseConsumer {

    private final KafkaConsumer<String, String> consumer;
    private final ClickHouseService clickHouseService;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final String topic;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Для отслежиния offset'ов
    private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    private long lastCommitTime = System.currentTimeMillis();

    public KafkaToClickHouseConsumer(ClickHouseConfig config, ClickHouseService clickHouseService) {
        this.clickHouseService = clickHouseService;
        this.topic = config.metricsTopic;

        Properties props = new Properties();
        props.putAll(config.kafkaConsumerProps);
        props.put("client.id", "clickhouse-consumer");

        this.consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        Thread consumerThread = new Thread(this::runConsumer, "kafka-clickhouse-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();

        // Поток для периодического коммита offset'ов
        Thread commitThread = new Thread(this::commitOffsetsPeriodically, "offset-committer");
        commitThread.setDaemon(true);
        commitThread.start();
    }

    private void runConsumer() {
        consumer.subscribe(Collections.singletonList(topic));
        System.out.println("Subscribed to topic: " + topic);

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Валидируем JSON
                        JsonNode jsonNode = objectMapper.readTree(record.value());

                        // Добавляем device_id если его нет (для обратной совместимости)
                        if (!jsonNode.has("deviceId")) {
                            // Можно добавить дефолтное значение или пропустить
                            continue;
                        }

                        // Отправляем в ClickHouse
                        clickHouseService.insertMetric(record.value());

                        // Сохраняем offset для коммита
                        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        offsetsToCommit.put(tp, new OffsetAndMetadata(record.offset() + 1));

                    } catch (Exception e) {
                        System.err.println("Failed to process record: " + e.getMessage());
                        System.err.println("Problematic record: " + record.value());
                    }
                }

                // Периодический коммит
                commitOffsetsIfNeeded();
            }
        } finally {
            consumer.close();
        }
    }

    private void commitOffsetsIfNeeded() {
        synchronized (offsetsToCommit) {
            if (!offsetsToCommit.isEmpty() &&
                    (System.currentTimeMillis() - lastCommitTime) > 5000) {
                try {
                    consumer.commitSync(offsetsToCommit);
                    System.out.println("Committed offsets for " + offsetsToCommit.size() + " partitions");
                    offsetsToCommit.clear();
                    lastCommitTime = System.currentTimeMillis();
                } catch (Exception e) {
                    System.err.println("Failed to commit offsets: " + e.getMessage());
                }
            }
        }
    }

    private void commitOffsetsPeriodically() {
        while (running.get()) {
            try {
                Thread.sleep(5000);
                commitOffsetsIfNeeded();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public void shutdown() {
        running.set(false);
        try {
            // Финальный коммит
            synchronized (offsetsToCommit) {
                if (!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(offsetsToCommit);
                }
            }
            clickHouseService.forceFlush();
            clickHouseService.close();
        } catch (Exception e) {
            System.err.println("Error during shutdown: " + e.getMessage());
        }
    }
}