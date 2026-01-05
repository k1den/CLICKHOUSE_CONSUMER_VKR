package org.k1den;

import java.util.List;
import java.util.Map;

public class DeviceMetric {
    public String deviceId;
    public String deviceName;
    public String hostname;
    public long timestamp;
    public double cpuLoad;
    public double systemLoadAverage;
    public double memoryUsedPercent;
    public long memoryTotal;
    public long memoryAvailable;
    public List<DiskMetric> disks;
    public long networkRxBytes;
    public long networkTxBytes;
    public int processCount;
    public double cpuTemperature;
    public Map<String, String> tags;
}
