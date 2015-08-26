package net.java.ao.test.converters;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import net.java.ao.RawEntity;
import net.java.ao.atlassian.AtlassianTableNameConverter;
import net.java.ao.atlassian.TablePrefix;
import net.java.ao.schema.CamelCaseTableNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.test.ConfigurationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public final class DynamicTableNameConverter implements TableNameConverter {
    private static final Logger logger = LoggerFactory.getLogger(DynamicTableNameConverter.class);

    private final Supplier<TableNameConverter> tncSupplier;

    public DynamicTableNameConverter() {
        this.tncSupplier = Suppliers.memoize(new SystemPropertyTableNameConverterSupplier());
    }

    @Override
    public String getName(Class<? extends RawEntity<?>> clazz) {
        return tncSupplier.get().getName(clazz);
    }

    private static final class SystemPropertyTableNameConverterSupplier implements Supplier<TableNameConverter> {
        public static final String DEFAULT = "atlassian";

        private final ImmutableMap<String, TableNameConverter> converters = ImmutableMap.of(
                DEFAULT, new AtlassianTableNameConverter(new TestPrefix("AO_000000")),
                "camelcase", new CamelCaseTableNameConverter(),
                "uppercase", new UpperCaseTableNameConverter()
        );

        @Override
        public TableNameConverter get() {
            final String key = ConfigurationProperties.get("ao.test.tablenameconverter", DEFAULT);
            final TableNameConverter tnc = converters.get(key);

            logger.debug("Table name converter key is {} and resolved to {}", key, tnc.getClass().getName());
            return tnc;
        }
    }

    private static final class TestPrefix implements TablePrefix {
        private static final String DEFAULT_SEPARATOR = "_";
        private final String prefix;
        private final String separator;

        public TestPrefix(String prefix) {
            this(prefix, DEFAULT_SEPARATOR);
        }

        public TestPrefix(String prefix, String separator) {
            this.prefix = checkNotNull(prefix);
            this.separator = checkNotNull(separator);
        }

        public String prepend(String string) {
            return new StringBuilder().append(prefix).append(separator).append(string).toString();
        }
    }
}
