package org.k1den;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.k1den.dto.DeviceFeature;
import org.k1den.dto.DeviceMetric;
import org.k1den.dto.DiskMetric;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class FeatureEngineeringJob {

    @FunctionalInterface
    public interface StatementSetter<T> extends Serializable {
        void set(PreparedStatement ps, T value) throws SQLException;
    }

    public static class ClickHouseSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {

        private final String url;
        private final String sql;
        private final StatementSetter<T> setter;
        private final int batchSize;
        private final long flushIntervalMs;

        private transient Connection conn;
        private transient PreparedStatement ps;
        private transient int count;
        private transient long lastFlushTime;

        public ClickHouseSinkFunction(String url, String sql, StatementSetter<T> setter,
                                      int batchSize, long flushIntervalMs) {
            this.url = url;
            this.sql = sql;
            this.setter = setter;
            this.batchSize = batchSize;
            this.flushIntervalMs = flushIntervalMs;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            conn = DriverManager.getConnection(url);
            ps = conn.prepareStatement(sql);
            count = 0;
            lastFlushTime = System.currentTimeMillis();
        }

        @Override
        public void invoke(T value, Context context) throws Exception {
            setter.set(ps, value);
            ps.addBatch();
            count++;

            long now = System.currentTimeMillis();
            if (count >= batchSize || (now - lastFlushTime) >= flushIntervalMs) {
                flush();
            }
        }

        private void flush() throws SQLException {
            if (count == 0) return;
            ps.executeBatch();
            count = 0;
            lastFlushTime = System.currentTimeMillis();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            flush();
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
        }

        @Override
        public void finish() throws Exception {
            if (count > 0) flush();
        }

        @Override
        public void close() throws Exception {
            try { if (count > 0) flush(); } catch (Exception ignored) {}
            if (ps != null) ps.close();
            if (conn != null) conn.close();
        }
    }

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String configFile = params.get("config.file", "config.properties");
        ParameterTool config = ParameterTool.fromPropertiesFile(configFile);

        String clickhouseUrl = config.get("clickhouse.url", "jdbc:clickhouse://localhost:8123/default?socket_timeout=30000&connection_timeout=10000");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String runMode = params.get("mode", "ALL").toUpperCase();

        env.setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(
                3,
                org.apache.flink.api.common.time.Time.seconds(10)
        ));

        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(System.getenv().getOrDefault(
                        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
                .setTopics("metrics")
                .setGroupId("flink-feature-group-" + runMode)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<DeviceMetric> parsedStream = rawStream.map(
                new RichMapFunction<String, DeviceMetric>() {
                    private transient ObjectMapper mapper;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration p) throws Exception {
                        mapper = new ObjectMapper();
                        mapper.configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
                    }

                    @Override
                    public DeviceMetric map(String json) throws Exception {
                        return mapper.readValue(json, DeviceMetric.class);
                    }
                });

        DataStream<DeviceMetric> metricsStream = parsedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DeviceMetric>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withIdleness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> event.timestamp)
        );

        if (runMode.equals("RAW") || runMode.equals("ALL")) {
            metricsStream.addSink(new ClickHouseSinkFunction<>(
                    clickhouseUrl,
                    "INSERT INTO device_metrics (deviceId, deviceName, hostname, timestamp, " +
                            "cpuLoad, systemLoadAverage, memoryUsedPercent, memoryTotal, memoryAvailable, " +
                            "networkRxBytes, networkTxBytes, processCount, cpuTemperature) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (ps, m) -> {
                        ps.setString(1, m.deviceId);
                        ps.setString(2, m.deviceName);
                        ps.setString(3, m.hostname);
                        ps.setLong(4, m.timestamp);
                        ps.setDouble(5, m.cpuLoad);
                        ps.setDouble(6, m.systemLoadAverage);
                        ps.setDouble(7, m.memoryUsedPercent);
                        ps.setLong(8, m.memoryTotal);
                        ps.setLong(9, m.memoryAvailable);
                        ps.setLong(10, m.networkRxBytes);
                        ps.setLong(11, m.networkTxBytes);
                        ps.setInt(12, m.processCount);
                        ps.setDouble(13, Double.isNaN(m.cpuTemperature) ? 0.0 : m.cpuTemperature);
                    },
                    10000, 10_000L
            )).name("ClickHouse Sink: RAW Metrics");
        }

        if (runMode.equals("DISK") || runMode.equals("ALL")) {
            metricsStream.flatMap(new DiskExtractor())
                    .addSink(new ClickHouseSinkFunction<>(
                            clickhouseUrl,
                            "INSERT INTO disk_metrics (deviceId, timestamp, mountPoint, total, free, usedPercent) " +
                                    "VALUES (?, ?, ?, ?, ?, ?)",
                            (ps, fd) -> {
                                ps.setString(1, fd.deviceId);
                                ps.setLong(2, fd.timestamp);
                                ps.setString(3, fd.mountPoint);
                                ps.setLong(4, fd.total);
                                ps.setLong(5, fd.free);
                                ps.setDouble(6, fd.usedPercent);
                            },
                            10000, 10_000L
                    )).name("ClickHouse Sink: DISK Metrics");
        }

        if (runMode.equals("FEATURES") || runMode.equals("ALL")) {
            DataStream<DeviceFeature> featuresStream = metricsStream
                    .keyBy(m -> m.deviceId)
                    .window(TumblingEventTimeWindows.of(Time.seconds(20)))
                    .process(new FeatureCalculator());

            featuresStream.addSink(new ClickHouseSinkFunction<>(
                    clickhouseUrl,
                    "INSERT INTO metrics_features (deviceId, timestamp, avgCpuLoad, maxMemoryUsed, " +
                            "avgCpuTemp, avgNetRx, avgNetTx, avgProcesses) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (ps, f) -> {
                        ps.setString(1, f.deviceId);
                        ps.setLong(2, f.windowEndTimestamp);
                        ps.setDouble(3, f.avgCpuLoad);
                        ps.setDouble(4, f.maxMemoryUsed);
                        ps.setDouble(5, f.avgCpuTemp);
                        ps.setDouble(6, f.avgNetRx);
                        ps.setDouble(7, f.avgNetTx);
                        ps.setDouble(8, f.avgProcesses);
                    },
                    10000, 10_000L
            )).name("ClickHouse Sink: FEATURES");

            DataStream<String> jsonFeaturesStream = featuresStream.map(
                    new RichMapFunction<DeviceFeature, String>() {
                        private transient ObjectMapper mapper;

                        @Override
                        public void open(org.apache.flink.configuration.Configuration p) throws Exception {
                            mapper = new ObjectMapper();
                        }

                        @Override
                        public String map(DeviceFeature f) throws Exception {
                            return mapper.writeValueAsString(f);
                        }
                    });

            jsonFeaturesStream.print("FLINK ВЫДАЛ ФИЧУ");

            org.apache.flink.connector.kafka.sink.KafkaSink<String> kafkaSink =
                    org.apache.flink.connector.kafka.sink.KafkaSink.<String>builder()
                            .setBootstrapServers(System.getenv().getOrDefault(
                                    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
                            .setRecordSerializer(
                                    org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder()
                                            .setTopic("features_topic")
                                            .setValueSerializationSchema(new SimpleStringSchema())
                                            .build()
                            ).build();

            jsonFeaturesStream.sinkTo(kafkaSink).name("Kafka Sink: Features Topic");
        }

        env.execute("Distributed Infrastructure Job [" + runMode + "]");
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
            if (metric == null || metric.disks == null) {
                return;
            }

            for (DiskMetric d : metric.disks) {
                if (d.usedPercent <= 0) {
                    continue;
                }

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

    public static class FeatureCalculator extends ProcessWindowFunction<DeviceMetric, DeviceFeature, String, TimeWindow> {
        @Override
        public void process(String deviceId, Context context, Iterable<DeviceMetric> elements, Collector<DeviceFeature> out) {
            double sumCpu = 0, sumRx = 0, sumTx = 0, sumProcs = 0;
            double sumTemp = 0;
            double maxMem = 0;
            int count = 0;
            int tempCount = 0;

            Map<String, Double> diskSum = new HashMap<>();
            Map<String, Integer> diskCount = new HashMap<>();

            for (DeviceMetric m : elements) {
                sumCpu += m.cpuLoad;
                sumRx += m.networkRxBytes;
                sumTx += m.networkTxBytes;
                sumProcs += m.processCount;
                if (m.memoryUsedPercent > maxMem) maxMem = m.memoryUsedPercent;

                if (!Double.isNaN(m.cpuTemperature) && m.cpuTemperature > 0) {
                    sumTemp += m.cpuTemperature;
                    tempCount++;
                }

                if (m.disks != null) {
                    for (DiskMetric d : m.disks) {
                        diskSum.merge(d.mountPoint, d.usedPercent, Double::sum);
                        diskCount.merge(d.mountPoint, 1, Integer::sum);
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
                f.avgCpuTemp = tempCount > 0 ? sumTemp / tempCount : 0.0;
                f.avgNetRx = sumRx / count;
                f.avgNetTx = sumTx / count;
                f.avgProcesses = sumProcs / count;

                diskSum.forEach((mp, sum) ->
                        f.disksUsedPercents.put(mp, sum / diskCount.get(mp))
                );

                out.collect(f);
            }
        }
    }
}