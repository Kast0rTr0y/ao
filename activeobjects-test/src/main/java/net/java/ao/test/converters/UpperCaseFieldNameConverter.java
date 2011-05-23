package net.java.ao.test.converters;

import net.java.ao.schema.Case;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.FieldNameProcessor;
import net.java.ao.schema.UnderscoreFieldNameConverter;

import java.lang.reflect.Method;

public final class UpperCaseFieldNameConverter implements FieldNameConverter, FieldNameProcessor
{
    private final UnderscoreFieldNameConverter fnc = new UnderscoreFieldNameConverter(Case.UPPER);

    @Override
    public String getName(Method method)
    {
        return fnc.getName(method);
    }

    @Override
    public String getPolyTypeName(Method method)
    {
        return fnc.getPolyTypeName(method);
    }

    @Override
    public String convertName(String name)
    {
        return fnc.convertName(name);
    }
}
