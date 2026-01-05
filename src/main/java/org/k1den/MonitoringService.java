package org.k1den;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;

public class MonitoringService {

    private static final Logger logger = Logger.getLogger(MonitoringService.class.getName());
    private static final String KAFKA_TOPIC = "metrics";

    public static void main(String[] args) throws Exception {
        logger.info("Starting Monitoring Service...");

        // Регистрируем драйвер ClickHouse
        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            logger.info("ClickHouse driver registered successfully");
        } catch (ClassNotFoundException e) {
            logger.severe("Failed to register ClickHouse driver: " + e.getMessage());
            return;
        }

        // Kafka consumer setup
        Properties props = new Properties();
        String kafkaBootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "monitoring-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));
        logger.info("Subscribed to topic: " + KAFKA_TOPIC);
        logger.info("Connecting to Kafka at: " + kafkaBootstrap);

        // ClickHouse connection
        String clickhouseUrl = System.getenv().getOrDefault("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/default");
        logger.info("Connecting to ClickHouse at: " + clickhouseUrl);

        try (Connection ch = DriverManager.getConnection(clickhouseUrl)) {
            logger.info("Connected to ClickHouse successfully");

            // Create tables if not exists
            try {
                ch.createStatement().execute("""
                    CREATE TABLE IF NOT EXISTS device_metrics (
                        deviceId String,
                        deviceName String,
                        hostname String,
                        timestamp UInt64,
                        cpuLoad Float64,
                        systemLoadAverage Float64,
                        memoryUsedPercent Float64,
                        memoryTotal UInt64,
                        memoryAvailable UInt64,
                        networkRxBytes UInt64,
                        networkTxBytes UInt64,
                        processCount UInt32,
                        cpuTemperature Float64,
                        tags String
                    ) ENGINE = MergeTree() ORDER BY (deviceId, timestamp)
                """);
                logger.info("Created/verified device_metrics table");
            } catch (Exception e) {
                logger.warning("Could not create device_metrics table (might already exist): " + e.getMessage());
            }

            try {
                ch.createStatement().execute("""
                    CREATE TABLE IF NOT EXISTS disk_metrics (
                        deviceId String,
                        timestamp UInt64,
                        mountPoint String,
                        total UInt64,
                        free UInt64,
                        usedPercent Float64
                    ) ENGINE = MergeTree() ORDER BY (deviceId, timestamp, mountPoint)
                """);
                logger.info("Created/verified disk_metrics table");
            } catch (Exception e) {
                logger.warning("Could not create disk_metrics table (might already exist): " + e.getMessage());
            }

            ObjectMapper mapper = new ObjectMapper();

            // Main loop
            logger.info("Starting main processing loop...");
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (!records.isEmpty()) {
                    logger.info("Polled " + records.count() + " records");

                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            DeviceMetric metric = mapper.readValue(record.value(), DeviceMetric.class);
                            logger.info("Processing metric from device: " + metric.deviceId);

                            // Insert device metric
                            try (PreparedStatement psDevice = ch.prepareStatement("""
                                INSERT INTO device_metrics
                                (deviceId, deviceName, hostname, timestamp, cpuLoad, systemLoadAverage, memoryUsedPercent,
                                 memoryTotal, memoryAvailable, networkRxBytes, networkTxBytes, processCount, cpuTemperature, tags)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """)) {
                                psDevice.setString(1, metric.deviceId);
                                psDevice.setString(2, metric.deviceName);
                                psDevice.setString(3, metric.hostname);
                                psDevice.setLong(4, metric.timestamp);
                                psDevice.setDouble(5, metric.cpuLoad);
                                psDevice.setDouble(6, metric.systemLoadAverage);
                                psDevice.setDouble(7, metric.memoryUsedPercent);
                                psDevice.setLong(8, metric.memoryTotal);
                                psDevice.setLong(9, metric.memoryAvailable);
                                psDevice.setLong(10, metric.networkRxBytes);
                                psDevice.setLong(11, metric.networkTxBytes);
                                psDevice.setInt(12, metric.processCount);
                                psDevice.setDouble(13, metric.cpuTemperature);
                                psDevice.setString(14, convertMapToString(metric.tags));
                                psDevice.executeUpdate();
                                logger.info("Inserted device metric for: " + metric.deviceId);
                            }

                            // Insert disk metrics
                            if (metric.disks != null && !metric.disks.isEmpty()) {
                                try (PreparedStatement psDisk = ch.prepareStatement("""
                                    INSERT INTO disk_metrics
                                    (deviceId, timestamp, mountPoint, total, free, usedPercent)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                """)) {
                                    for (DiskMetric disk : metric.disks) {
                                        psDisk.setString(1, metric.deviceId);
                                        psDisk.setLong(2, metric.timestamp);
                                        psDisk.setString(3, disk.mountPoint);
                                        psDisk.setLong(4, disk.total);
                                        psDisk.setLong(5, disk.free);
                                        psDisk.setDouble(6, disk.usedPercent);
                                        psDisk.addBatch();
                                    }
                                    psDisk.executeBatch();
                                    logger.info("Inserted " + metric.disks.size() + " disk metrics for device: " + metric.deviceId);
                                }
                            }

                        } catch (Exception e) {
                            logger.severe("Error processing record: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                }
            }

        } catch (Exception e) {
            logger.severe("Fatal error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close();
            logger.info("Consumer closed");
        }
    }

    private static String convertMapToString(java.util.Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{");
        for (java.util.Map.Entry<String, String> entry : map.entrySet()) {
            if (sb.length() > 1) sb.append(",");
            sb.append("'").append(entry.getKey()).append("':'").append(entry.getValue()).append("'");
        }
        sb.append("}");
        return sb.toString();
    }
}