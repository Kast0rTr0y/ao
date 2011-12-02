package net.java.ao.types;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import net.java.ao.EntityManager;

import static net.java.ao.types.StringTypeProperties.stringType;

/**
 *
 */
public class ClassType extends AbstractStringType<Class<?>>
{

	public ClassType() {
		super(stringType("VARCHAR", "CLOB").withLength(255), Class.class);
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
}
