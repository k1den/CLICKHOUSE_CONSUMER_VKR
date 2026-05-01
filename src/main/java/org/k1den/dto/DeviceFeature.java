package org.k1den.dto;

import java.util.HashMap;
import java.util.Map;

public class DeviceFeature {
    public String deviceId;
    public long windowEndTimestamp;

    public double avgCpuLoad;
    public double maxMemoryUsed;
    public double avgCpuTemp;
    public double avgNetRx;
    public double avgNetTx;
    public double avgProcesses;

    public Map<String, Double> disksUsedPercents = new HashMap<>();
}