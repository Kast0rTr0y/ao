package net.java.ao.benchmark.util;

public final class WikiReportPrinter implements ReportPrinter
{
    @Override
    public void print(Report report)
    {
        System.out.printf("| %s ", report.getTotalTime());
        if (report.hasLaps())
        {
            System.out.printf("| %s ", report.getAverageTime());
        }
    }
}
