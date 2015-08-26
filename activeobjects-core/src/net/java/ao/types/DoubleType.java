package net.java.ao.types;

import net.java.ao.EntityManager;
import net.java.ao.util.DoubleUtils;
import net.java.ao.util.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;

import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.NUMERIC;

final class DoubleType extends AbstractLogicalType<Double> {
    public DoubleType() {
        super("Double",
                new Class<?>[]{Double.class, double.class},
                DOUBLE, new Integer[]{DOUBLE, NUMERIC, DECIMAL});
    }

    @Override
    public Double pullFromDatabase(EntityManager manager, ResultSet res, Class<Double> type, String columnName)
            throws SQLException {
        return preserveNull(res, res.getDouble(columnName));
    }

    @Override
    protected Double validateInternal(Double value) {
        DoubleUtils.checkDouble(value);
        return value;
    }

    @Override
    public Double parse(String input) {
        return StringUtils.isBlank(input) ? null : Double.parseDouble(input);
    }
}
