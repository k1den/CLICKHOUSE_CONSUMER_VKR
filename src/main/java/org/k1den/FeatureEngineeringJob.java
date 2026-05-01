package org.k1den;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.k1den.dto.DeviceFeature;
import org.k1den.dto.DeviceMetric;
import org.k1den.dto.DiskMetric;

import java.util.HashMap;
import java.util.Map;

public class FeatureEngineeringJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
                .setTopics("metrics")
                .setGroupId("flink-feature-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<DeviceMetric> metricsStream = rawStream.map(new org.apache.flink.api.common.functions.RichMapFunction<String, DeviceMetric>() {
            private transient ObjectMapper mapper;

            @Override
            public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                super.open(parameters);
                mapper = new ObjectMapper();
            }

            @Override
            public DeviceMetric map(String json) throws Exception {
                return mapper.readValue(json, DeviceMetric.class);
            }
        });

        String clickhouseUrl = System.getenv().getOrDefault("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/default");


        metricsStream.addSink(JdbcSink.sink(
                "INSERT INTO device_metrics (deviceId, deviceName, hostname, timestamp, cpuLoad, systemLoadAverage, memoryUsedPercent, memoryTotal, memoryAvailable, networkRxBytes, networkTxBytes, processCount, cpuTemperature) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (statement, m) -> {
                    statement.setString(1, m.deviceId);
                    statement.setString(2, m.deviceName);
                    statement.setString(3, m.hostname);
                    statement.setLong(4, m.timestamp);
                    statement.setDouble(5, m.cpuLoad);
                    statement.setDouble(6, m.systemLoadAverage);
                    statement.setDouble(7, m.memoryUsedPercent);
                    statement.setLong(8, m.memoryTotal);
                    statement.setLong(9, m.memoryAvailable);
                    statement.setLong(10, m.networkRxBytes);
                    statement.setLong(11, m.networkTxBytes);
                    statement.setInt(12, m.processCount);
                    statement.setDouble(13, m.cpuTemperature);
                },
                JdbcExecutionOptions.builder().withBatchSize(3000).withBatchIntervalMs(3000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(clickhouseUrl).withDriverName("com.clickhouse.jdbc.ClickHouseDriver").build()
        ));

        metricsStream.flatMap(new DiskExtractor())
                .addSink(JdbcSink.sink(
                        "INSERT INTO disk_metrics (deviceId, timestamp, mountPoint, total, free, usedPercent) VALUES (?, ?, ?, ?, ?, ?)",
                        (statement, fd) -> {
                            statement.setString(1, fd.deviceId);
                            statement.setLong(2, fd.timestamp);
                            statement.setString(3, fd.mountPoint);
                            statement.setLong(4, fd.total);
                            statement.setLong(5, fd.free);
                            statement.setDouble(6, fd.usedPercent);
                        },
                        JdbcExecutionOptions.builder().withBatchSize(3000).withBatchIntervalMs(3000).build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(clickhouseUrl).withDriverName("com.clickhouse.jdbc.ClickHouseDriver").build()
                ));

        DataStream<DeviceFeature> featuresStream = metricsStream
                .keyBy(metric -> metric.deviceId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .process(new FeatureCalculator());

        featuresStream.addSink(JdbcSink.sink(
                "INSERT INTO metrics_features (deviceId, timestamp, avgCpuLoad, maxMemoryUsed, avgCpuTemp, avgNetRx, avgNetTx, avgProcesses) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (statement, f) -> {
                    statement.setString(1, f.deviceId);
                    statement.setLong(2, f.windowEndTimestamp);
                    statement.setDouble(3, f.avgCpuLoad);
                    statement.setDouble(4, f.maxMemoryUsed);
                    statement.setDouble(5, f.avgCpuTemp);
                    statement.setDouble(6, f.avgNetRx);
                    statement.setDouble(7, f.avgNetTx);
                    statement.setDouble(8, f.avgProcesses);
                },
                JdbcExecutionOptions.builder().withBatchSize(1000).withBatchIntervalMs(5000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(clickhouseUrl).withDriverName("com.clickhouse.jdbc.ClickHouseDriver").build()
        ));

        DataStream<String> jsonFeaturesStream = featuresStream.map(new org.apache.flink.api.common.functions.RichMapFunction<DeviceFeature, String>() {
            private transient ObjectMapper mapper;

            @Override
            public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                super.open(parameters);
                mapper = new ObjectMapper();
            }

            @Override
            public String map(DeviceFeature feature) throws Exception {
                return mapper.writeValueAsString(feature);
            }
        });

        jsonFeaturesStream.print("FLINK ВЫДАЛ ФИЧУ");

        org.apache.flink.connector.kafka.sink.KafkaSink<String> kafkaSink =
                org.apache.flink.connector.kafka.sink.KafkaSink.<String>builder()
                        .setBootstrapServers(System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
                        .setRecordSerializer(
                                org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder()
                                        .setTopic("features_topic")
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build()
                        ).build();

        jsonFeaturesStream.sinkTo(kafkaSink);

        env.execute("Distributed Infrastructure Feature Engineering");
    }

    public static class FlatDisk {
        public String deviceId;
        public long timestamp;
        public String mountPoint;
        public long total;
        public long free;
        public double usedPercent;
    }

    public static class DiskExtractor implements FlatMapFunction<DeviceMetric, FlatDisk> {
        @Override
        public void flatMap(DeviceMetric metric, Collector<FlatDisk> out) {
            if (metric.disks != null) {
                for (DiskMetric d : metric.disks) {
                    FlatDisk fd = new FlatDisk();
                    fd.deviceId = metric.deviceId;
                    fd.timestamp = metric.timestamp;
                    fd.mountPoint = d.mountPoint;
                    fd.total = d.total;
                    fd.free = d.free;
                    fd.usedPercent = d.usedPercent;
                    out.collect(fd);
                }
            }
        }
    }

    public static class FeatureCalculator extends ProcessWindowFunction<DeviceMetric, DeviceFeature, String, TimeWindow> {
        @Override
        public void process(String deviceId, Context context, Iterable<DeviceMetric> elements, Collector<DeviceFeature> out) {
            double sumCpu = 0, sumTemp = 0, sumRx = 0, sumTx = 0, sumProcs = 0;
            double maxMem = 0;
            int count = 0;

            Map<String, Double> diskSum = new HashMap<>();
            Map<String, Integer> diskCount = new HashMap<>();

            for (DeviceMetric m : elements) {
                sumCpu += m.cpuLoad;
                sumTemp += m.cpuTemperature;
                sumRx += m.networkRxBytes;
                sumTx += m.networkTxBytes;
                sumProcs += m.processCount;
                if (m.memoryUsedPercent > maxMem) maxMem = m.memoryUsedPercent;

                if (m.disks != null) {
                    for (DiskMetric d : m.disks) {
                        diskSum.put(d.mountPoint, diskSum.getOrDefault(d.mountPoint, 0.0) + d.usedPercent);
                        diskCount.put(d.mountPoint, diskCount.getOrDefault(d.mountPoint, 0) + 1);
                    }
                }
                count++;
            }

            if (count > 0) {
                DeviceFeature f = new DeviceFeature();
                f.deviceId = deviceId;
                f.windowEndTimestamp = context.window().getEnd();
                f.avgCpuLoad = sumCpu / count;
                f.maxMemoryUsed = maxMem;
                f.avgCpuTemp = sumTemp / count;
                f.avgNetRx = sumRx / count;
                f.avgNetTx = sumTx / count;
                f.avgProcesses = sumProcs / count;

                for (String mountPoint : diskSum.keySet()) {
                    f.disksUsedPercents.put(mountPoint, diskSum.get(mountPoint) / diskCount.get(mountPoint));
                }

                out.collect(f);
            }
        }
    }
}