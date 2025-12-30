package org.k1den.config;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

@SuppressWarnings("unchecked")
public class ClickHouseConfig {

    public final String kafkaBootstrap;
    public final String metricsTopic;
    public final String consumerGroupId;

    public final String clickhouseUrl;
    public final String clickhouseUser;
    public final String clickhousePassword;
    public final String metricsTable;
    public final int batchSize;
    public final long batchIntervalMs;

    public final Properties kafkaConsumerProps = new Properties();

    public ClickHouseConfig(Map<String, Object> yamlRoot) {
        // Kafka
        Map<String, Object> kafka = (Map<String, Object>) yamlRoot.get("kafka");
        Map<String, Object> bootstrap = (Map<String, Object>) kafka.get("bootstrap");
        Map<String, Object> topic = (Map<String, Object>) kafka.get("topic");
        Map<String, Object> consumer = (Map<String, Object>) kafka.get("consumer");
        Map<String, Object> consumerGroup = (Map<String, Object>) consumer.get("group");

        kafkaBootstrap = getString(bootstrap, "servers", "localhost:9092");
        metricsTopic = getString(topic, "metrics", "metrics");
        consumerGroupId = getString(consumerGroup, "id", "clickhouse-consumer-group");

        // Получаем auto.offset.reset безопасно
        String autoOffsetReset = "earliest";
        if (consumer.containsKey("auto")) {
            Map<String, Object> auto = (Map<String, Object>) consumer.get("auto");
            if (auto.containsKey("offset")) {
                Map<String, Object> offset = (Map<String, Object>) auto.get("offset");
                autoOffsetReset = getString(offset, "reset", "earliest");
            }
        }

        // ClickHouse
        Map<String, Object> clickhouse = (Map<String, Object>) yamlRoot.get("clickhouse");
        Map<String, Object> table = (Map<String, Object>) clickhouse.get("table");
        Map<String, Object> batch = (Map<String, Object>) clickhouse.get("batch");

        clickhouseUrl = getString(clickhouse, "url", "jdbc:clickhouse://localhost:8123/default");
        clickhouseUser = getString(clickhouse, "username", "default");
        clickhousePassword = getString(clickhouse, "password", "");
        metricsTable = getString(table, "metrics", "system_metrics");
        batchSize = getInt(batch, "size", 1000);

        // Получаем batch interval
        long intervalValue = 5000L;
        if (batch.containsKey("interval")) {
            Map<String, Object> interval = (Map<String, Object>) batch.get("interval");
            intervalValue = getLong(interval, "ms", 5000L);
        }
        batchIntervalMs = intervalValue;

        // Kafka Consumer Properties
        kafkaConsumerProps.put("bootstrap.servers", kafkaBootstrap);
        kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProps.put("group.id", consumerGroupId);
        kafkaConsumerProps.put("auto.offset.reset", autoOffsetReset);
        kafkaConsumerProps.put("enable.auto.commit", "false");
        kafkaConsumerProps.put("max.poll.records", "500");
    }

    // Вспомогательные методы для безопасного получения значений
    private String getString(Map<String, Object> map, String key, String defaultValue) {
        Object value = map.get(key);
        return (value != null) ? value.toString() : defaultValue;
    }

    private int getInt(Map<String, Object> map, String key, int defaultValue) {
        Object value = map.get(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value != null) {
            try {
                return Integer.parseInt(value.toString());
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    private long getLong(Map<String, Object> map, String key, long defaultValue) {
        Object value = map.get(key);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value != null) {
            try {
                return Long.parseLong(value.toString());
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    public static ClickHouseConfig load() {
        try (InputStream in = ClickHouseConfig.class.getClassLoader().getResourceAsStream("application.yml")) {
            if (in == null) {
                throw new RuntimeException("application.yml not found in resources");
            }
            Yaml yaml = new Yaml();
            Map<String, Object> root = yaml.load(in);
            return new ClickHouseConfig(root);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load YAML config: " + e.getMessage(), e);
        }
    }
}