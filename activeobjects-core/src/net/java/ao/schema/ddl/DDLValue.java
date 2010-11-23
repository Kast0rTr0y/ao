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

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final DDLValue ddlValue = (DDLValue) o;

        if (field != null ? !field.equals(ddlValue.field) : ddlValue.field != null)
        {
            return false;
        }
        if (value != null ? !value.equals(ddlValue.value) : ddlValue.value != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = field != null ? field.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}
