package net.java.ao.types;

import net.java.ao.schema.StringLength;

public class StringTypeProperties
{
    static final int DEFAULT_LENGTH = 255;

    private final String sqlTypeIdentifierLimited;
    private final String sqlTypeIdentifierUnlimited;
    private final int length;
    
    private StringTypeProperties(String sqlTypeIdentifierLimited, String sqlTypeIdentifierUnlimited, int length)
    {
        this.sqlTypeIdentifierLimited = sqlTypeIdentifierLimited;
        this.sqlTypeIdentifierUnlimited = sqlTypeIdentifierUnlimited;
        this.length = length;
    }

    public static StringTypeProperties stringType(String sqlTypeIdentifierLimited, String sqlTypeIdentifierUnlimited)
    {
        return new StringTypeProperties(sqlTypeIdentifierLimited, sqlTypeIdentifierUnlimited, DEFAULT_LENGTH);
    }

    public StringTypeProperties withLength(int length)
    {
        return new StringTypeProperties(this.sqlTypeIdentifierLimited, this.sqlTypeIdentifierUnlimited, length);
    }
    
    public String getSqlTypeIdentifier()
    {
        return (length == StringLength.UNLIMITED) ? sqlTypeIdentifierUnlimited : sqlTypeIdentifierLimited;
    }
    
    public int getLength()
    {
        return length;
    }
    
    public boolean isUnlimited()
    {
        return (length == StringLength.UNLIMITED);
    }
}
