package net.java.ao.types;

import net.java.ao.EntityManager;
import net.java.ao.types.DatabaseType;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 *
 */
public class ClassType extends DatabaseType<Class<?>>
{

	public ClassType() {
		super(Types.VARCHAR, 255, Class.class);
	}

	@Override
	public Class<?> pullFromDatabase(EntityManager manager, ResultSet res, Class<? extends Class<?>> type, String field) throws SQLException
    {
		try {
			return Class.forName(res.getString(field));
		} catch (Throwable t) {
			return null;
		}
	}

	@Override
	public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, Class<?> value) throws SQLException {
		stmt.setString(index, value.getName());
	}

	@Override
	public Object defaultParseValue(String value) {
		try {
			return Class.forName(value);
		} catch (Throwable t) {
			return null;
		}
	}

	@Override
	public String valueToString(Object value) {
		if (value instanceof Class<?>) {
			return ((Class<?>) value).getCanonicalName();
		}

		return super.valueToString(value);
	}

	@Override
	public String getDefaultName() {
		return "VARCHAR";
	}
}
