package net.java.ao.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import net.java.ao.ActiveObjectsException;

public final class DateUtils
{
    static
    {
        MAX_DATE = newDate(9999, 12, 31);
    }

    public static final Date MAX_DATE;

    public static DateFormat newDateFormat(String pattern)
    {
        return new SimpleDateFormat(pattern);
    }

    public static Calendar checkAgainstMaxDate(Calendar c)
    {
        checkAgainstMaxDate(c.getTime());
        return c;
    }

    public static Date checkAgainstMaxDate(Date date)
    {
        if (date.compareTo(MAX_DATE) > 0)
        {
            throw new ActiveObjectsException("Default date value must be strictly before " + MAX_DATE);
        }
        return date;
    }

    private static Date newDate(int year, int month, int dayOfMonth)
    {
        return newCalendar(year, month, dayOfMonth).getTime();
    }

    private static Calendar newCalendar(int year, int month, int dayOfMonth)
    {
        final Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, year);
        c.set(Calendar.MONTH, month - 1);
        c.set(Calendar.DAY_OF_MONTH, dayOfMonth);
        return c;
    }
}
