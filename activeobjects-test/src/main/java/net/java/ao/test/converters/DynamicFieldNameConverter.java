package net.java.ao.test.converters;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import net.java.ao.schema.CamelCaseFieldNameConverter;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.FieldNameProcessor;
import net.java.ao.test.ConfigurationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

public final class DynamicFieldNameConverter implements FieldNameConverter, FieldNameProcessor
{
    private static final Logger logger = LoggerFactory.getLogger(DynamicFieldNameConverter.class);

    private final Supplier<FieldNameConverter> fncSupplier;

    public DynamicFieldNameConverter()
    {
        this.fncSupplier = Suppliers.memoize(new SystemPropertyFieldNameConverterSupplier());
    }

    @Override
    public String getName(Method method)
    {
        return fncSupplier.get().getName(method);
    }

    @Override
    public String getPolyTypeName(Method method)
    {
        return fncSupplier.get().getPolyTypeName(method);
    }

    @Override
    public String convertName(String name)
    {
        if (fncSupplier.get() instanceof FieldNameProcessor)
        {
            return ((FieldNameProcessor) fncSupplier.get()).convertName(name);
        }
        else
        {
            return name;
        }
    }

    private static final class SystemPropertyFieldNameConverterSupplier implements Supplier<FieldNameConverter>
    {
        private static final String DEFAULT = "test";

        private final ImmutableMap<String, FieldNameConverter> converters = ImmutableMap.<String, FieldNameConverter>of(
                "test", new TestFieldNameConverter(),
                "camelcase", new CamelCaseFieldNameConverter(),
                "uppercase", new UpperCaseFieldNameConverter()
        );

        @Override
        public FieldNameConverter get()
        {
            final String key = ConfigurationProperties.get("ao.test.fieldnameconverter", DEFAULT);
            final FieldNameConverter fnc = converters.get(key);

            logger.debug("Field name converter key is {} and resolved to {}", key, fnc.getClass().getName());
            return fnc;
        }
    }
}
