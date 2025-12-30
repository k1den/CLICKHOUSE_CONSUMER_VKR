package org.k1den.service;

import com.clickhouse.jdbc.ClickHouseDataSource;
import org.k1den.config.ClickHouseConfig;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ClickHouseService implements AutoCloseable {

    private final ClickHouseDataSource dataSource;
    private final String metricsTable;
    private final int batchSize;

    private final List<String> batchBuffer = new ArrayList<>();
    private long lastFlushTime = System.currentTimeMillis();

    public ClickHouseService(ClickHouseConfig config) throws SQLException {
        String url = config.clickhouseUrl;
        if (config.clickhouseUser != null && !config.clickhouseUser.isEmpty()) {
            url += "?user=" + config.clickhouseUser;
            if (config.clickhousePassword != null && !config.clickhousePassword.isEmpty()) {
                url += "&password=" + config.clickhousePassword;
            }
        }

        this.dataSource = new ClickHouseDataSource(url);
        this.metricsTable = config.metricsTable;
        this.batchSize = config.batchSize;

        initializeTable();
    }

    private void initializeTable() throws SQLException {
        String createTableSQL = String.format("""
            CREATE TABLE IF NOT EXISTS %s (
                device_id String,
                device_name String,
                hostname String,
                timestamp DateTime64(3),
                cpu_load Float64,
                system_load_average Float64,
                memory_used_percent Float64,
                memory_total UInt64,
                memory_available UInt64,
                network_rx_bytes UInt64,
                network_tx_bytes UInt64,
                process_count UInt32,
                cpu_temperature Float64,
                tags Map(String, String),
                disks Nested (
                    mount_point String,
                    total_bytes UInt64,
                    free_bytes UInt64,
                    used_percent Float64
                )
            ) ENGINE = MergeTree()
            ORDER BY (device_id, timestamp)
            TTL timestamp + INTERVAL 30 DAY
            SETTINGS index_granularity = 8192
            """, metricsTable);

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSQL);
            System.out.println("Table " + metricsTable + " initialized or already exists");
        }
    }

    public void insertMetric(String json) {
        synchronized (batchBuffer) {
            batchBuffer.add(json);

            // Проверяем условия для flush
            boolean shouldFlush = batchBuffer.size() >= batchSize ||
                    (System.currentTimeMillis() - lastFlushTime) > 5000;

            if (shouldFlush) {
                flushBatch();
            }
        }
    }

    private void flushBatch() {
        if (batchBuffer.isEmpty()) {
            return;
        }

        List<String> toInsert;
        synchronized (batchBuffer) {
            toInsert = new ArrayList<>(batchBuffer);
            batchBuffer.clear();
            lastFlushTime = System.currentTimeMillis();
        }

        try (Connection conn = dataSource.getConnection()) {
            // Используем формат JSONEachRow для эффективной вставки
            StringBuilder sql = new StringBuilder();
            sql.append("INSERT INTO ").append(metricsTable).append(" FORMAT JSONEachRow ");

            try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
                // ClickHouse JDBC не поддерживает batch для JSONEachRow напрямую,
                // поэтому собираем все JSON в одну строку
                StringBuilder jsonBatch = new StringBuilder();
                for (String json : toInsert) {
                    jsonBatch.append(json).append("\n");
                }

                // Отправляем как один большой запрос
                pstmt.setString(1, jsonBatch.toString());
                pstmt.execute();

                System.out.println("Inserted " + toInsert.size() + " metrics to ClickHouse");
            }
        } catch (SQLException e) {
            System.err.println("Failed to insert batch to ClickHouse: " + e.getMessage());
            // В случае ошибки возвращаем данные в буфер (упрощённо)
            synchronized (batchBuffer) {
                batchBuffer.addAll(0, toInsert);
            }
        }
    }

    public void forceFlush() {
        flushBatch();
    }

    @Override
    public void close() {
        forceFlush();
    }
}