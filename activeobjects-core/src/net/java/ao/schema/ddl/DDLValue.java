package net.java.ao.schema.ddl;

/**
 *
 */
public class DDLValue
{
    private DDLField field;
    private Object value;

    public Object getValue()
    {
        return value;
    }

    public void setValue(Object value)
    {
        this.value = value;
    }

    public DDLField getField()
    {
        return field;
    }

    public void setField(DDLField field)
    {
        this.field = field;
    }
}
