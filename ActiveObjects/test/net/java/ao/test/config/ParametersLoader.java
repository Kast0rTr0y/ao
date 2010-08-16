package net.java.ao.test.config;

import com.google.common.collect.ImmutableMap;
import net.java.ao.schema.CamelCaseFieldNameConverter;
import net.java.ao.schema.CamelCaseTableNameConverter;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.PluralizedNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.UnderscoreTableNameConverter;

import java.util.Map;

public class ParametersLoader
{
    private final static Map<String, Parameters> PARAMETERS = ImmutableMap.<String, Parameters>builder()
            .put("1", new ParametersImpl(new CamelCaseTableNameConverter(), new CamelCaseFieldNameConverter()))
            .put("2", new ParametersImpl(new UnderscoreTableNameConverter(false), new CamelCaseFieldNameConverter()))
            .put("3", new ParametersImpl(new PluralizedNameConverter(new CamelCaseTableNameConverter()), new CamelCaseFieldNameConverter()))
            .put("4", new ParametersImpl(new PluralizedNameConverter(new UnderscoreTableNameConverter(true)), new CamelCaseFieldNameConverter()))
            .build();

    private static Parameters parameters;

    public static Parameters get()
    {
        if (parameters == null)
        {
            parameters = loadParameters();
        }
        return parameters;
    }

    private static Parameters loadParameters()
    {
        return PARAMETERS.get(ConfigurationProperties.get("test.parameters", "1"));
    }

    private static final class ParametersImpl implements Parameters
    {
        private final TableNameConverter tableNameConverter;
        private final FieldNameConverter fieldNameConverter;

        public ParametersImpl(TableNameConverter tableNameConverter, FieldNameConverter fieldNameConverter)
        {
            this.tableNameConverter = tableNameConverter;
            this.fieldNameConverter = fieldNameConverter;
        }

        public TableNameConverter getTableNameConverter()
        {
            return tableNameConverter;
        }

        public FieldNameConverter getFieldNameConverter()
        {
            return fieldNameConverter;
        }
    }
}
