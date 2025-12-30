package org.k1den;

import org.k1den.config.ClickHouseConfig;
import org.k1den.service.ClickHouseService;
import org.k1den.service.KafkaToClickHouseConsumer;

public class Main {

    public static void main(String[] args) {
        System.out.println("Starting ClickHouse Consumer Service...");

        try {
            // Загрузка конфигурации
            ClickHouseConfig config = ClickHouseConfig.load();

            // Инициализация ClickHouse
            ClickHouseService clickHouseService = new ClickHouseService(config);

            // Создание и запуск consumer'а
            KafkaToClickHouseConsumer consumer = new KafkaToClickHouseConsumer(config, clickHouseService);
            consumer.start();

            System.out.println("ClickHouse Consumer Service started successfully");
            System.out.println("Consuming from Kafka topic: " + config.metricsTopic);
            System.out.println("Writing to ClickHouse table: " + config.metricsTable);

            // Shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down ClickHouse Consumer Service...");
                consumer.shutdown();
                System.out.println("Shutdown complete");
            }));

            // Бесконечное ожидание
            while (true) {
                Thread.sleep(1000);
            }

        } catch (Exception e) {
            System.err.println("Failed to start ClickHouse Consumer Service: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}