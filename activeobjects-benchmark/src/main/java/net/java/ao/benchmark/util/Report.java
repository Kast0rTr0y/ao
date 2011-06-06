package net.java.ao.benchmark.util;

import java.util.List;

public final class Report
{
    private String name;
    private long start;
    private long stop;
    private List<Long> laps;

    public Report(String name, long start, long stop, List<Long> laps)
    {
        this.name = name;
        this.start = start;
        this.stop = stop;
        this.laps = laps;
    }

    public String getName()
    {
        return name;
    }

    public boolean hasLaps()
    {
        return !laps.isEmpty();
    }

    public double getAverageTime()
    {
        if (!hasLaps())
        {
            throw new IllegalStateException("Cannot calculate average with no laps");
        }
        return convertToMillis((stop - start) / laps.size());
    }

    public double getTotalTime()
    {
        return convertToMillis(stop - start);
    }

    private static double convertToMillis(double valueInNano)
    {
        return valueInNano / (1000 * 1000);
    }
}
