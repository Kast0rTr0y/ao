package net.java.ao.types;

import net.java.ao.EntityManager;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

import static java.sql.Types.TIMESTAMP;
import static net.java.ao.util.DateUtils.checkAgainstMaxDate;
import static net.java.ao.util.DateUtils.newDateFormat;

final class DateType extends AbstractLogicalType<Date> {
    public static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final DateFormat DATE_FORMAT = newDateFormat(DATE_PATTERN);

    public DateType() {
        super("Date",
                new Class<?>[]{Date.class},
                TIMESTAMP, new Integer[]{TIMESTAMP});
    }

    @Override
    public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, Date value, int jdbcType) throws SQLException {
        stmt.setTimestamp(index, new Timestamp(value.getTime()));
    }

    @Override
    public Date pullFromDatabase(EntityManager manager, ResultSet res, Class<Date> type, String columnName)
            throws SQLException {
        return res.getTimestamp(columnName);
    }

    @Override
    protected Date validateInternal(Date value) {
        return checkAgainstMaxDate(value);
    }

    @Override
    public Date parse(String input) {
        try {
            return checkAgainstMaxDate(DATE_FORMAT.parse(input));
        } catch (ParseException e) {
            throw new IllegalArgumentException("Could not parse '" + input + "' as a valid date following " + DATE_PATTERN);
        }
    }

    @Override
    public String valueToString(Date value) {
        return DATE_FORMAT.format(value);
    }
}
