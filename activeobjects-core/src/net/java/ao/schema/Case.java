package net.java.ao.schema;

import java.util.Locale;

public enum Case
{
    UPPER
            {
                String apply(String s)
                {
                    return nullSafe(s).toUpperCase(Locale.ENGLISH);
                }
            },
    LOWER
            {
                @Override
                String apply(String s)
                {
                    return nullSafe(s).toLowerCase(Locale.ENGLISH);
                }
            };

    abstract String apply(String tableName);

    private static String nullSafe(String s)
    {
        return s != null ? s : "";
    }
}
