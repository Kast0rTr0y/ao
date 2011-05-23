package net.java.ao.test.converters;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import net.java.ao.RawEntity;
import net.java.ao.schema.CamelCaseTableNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.test.ConfigurationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DynamicTableNameConverter implements TableNameConverter
{
    private static final Logger logger = LoggerFactory.getLogger(DynamicTableNameConverter.class);

    private final Supplier<TableNameConverter> tncSupplier;

    public DynamicTableNameConverter()
    {
        this.tncSupplier = Suppliers.memoize(new SystemPropertyTableNameConverterSupplier());
    }

    @Override
    public String getName(Class<? extends RawEntity<?>> clazz)
    {
        return tncSupplier.get().getName(clazz);
    }

    private static final class SystemPropertyTableNameConverterSupplier implements Supplier<TableNameConverter>
    {
        public static final String DEFAULT = "camelcase";

        private final ImmutableMap<String, TableNameConverter> converters = ImmutableMap.of(
                DEFAULT, new CamelCaseTableNameConverter(),
                "uppercase", new UpperCaseTableNameConverter()
        );

        @Override
        public TableNameConverter get()
        {
            final String key = ConfigurationProperties.get("ao.test.tablenameconverter", DEFAULT);
            final TableNameConverter tnc = converters.get(key);

            logger.debug("Table name converter key is {} and resolved to {}", key, tnc.getClass().getName());
            return tnc;
        }
    }
}
