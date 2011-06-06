package net.java.ao.benchmark.util;

public final class PrettyPrintReportPrinter implements ReportPrinter
{
    @Override
    public void print(Report report)
    {
        System.out.printf("%s:\n", report.getName());
        System.out.printf("\tTotal: %sms\n", report.getTotalTime());
        if (report.hasLaps())
        {
            System.out.printf("\tAvg  : %sms\n", report.getAverageTime());
        }
    }
}
