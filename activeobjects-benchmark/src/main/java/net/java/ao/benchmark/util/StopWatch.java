package net.java.ao.benchmark.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.*;

public final class StopWatch<K>
{
    private final SortedMap<K, Long> laps = new TreeMap<K, Long>();

    private final String name;
    private long start;
    private long stop;
    private long lap;

    public StopWatch(String name)
    {
        this.name = checkNotNull(name);
    }

    public String getName()
    {
        return name;
    }

    public void start()
    {
        start = now();
        lap = start;
        laps.clear();
    }

    public void lap(K k)
    {
        final long now = now();
        laps.put(k, now - lap);
        lap = now;
    }

    public void stop()
    {
        stop = now();
    }

    private long now()
    {
        return System.nanoTime();
    }

    private boolean isStopped()
    {
        return stop > 0;
    }

    public Report getReport()
    {
        if (!isStopped())
        {
            throw new IllegalStateException("Stop watch must be stopped to generate report");
        }
        return new Report(name, start, stop, laps());
    }

    private List<Long> laps()
    {
        final List<Long> lapValues = new ArrayList<Long>(laps.size());
        for (Map.Entry<K, Long> e : laps.entrySet())
        {
            lapValues.add(e.getValue());
        }
        return lapValues;
    }

    public void printResults(boolean printLaps)
    {
        System.out.printf("%s:\n", name);
        System.out.printf("\tTotal time: %sms\n", convertToMillis(stop - start));
        if (!laps.isEmpty())
        {
            System.out.printf("\tAverage time: %sms\n", convertToMillis((stop - start) / laps.size()));
        }

        if (printLaps)
        {
            System.out.println("\tLaps:");
            for (Map.Entry<K, Long> lap : laps.entrySet())
            {
                System.out.printf("\t\t%s : %sms\n", lap.getKey(), convertToMillis(lap.getValue()));
            }
        }
    }

    private static double convertToMillis(double valueInNano)
    {
        return valueInNano / (1000 * 1000);
    }
}