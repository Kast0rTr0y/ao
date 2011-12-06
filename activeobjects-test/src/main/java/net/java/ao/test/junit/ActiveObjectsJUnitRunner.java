package net.java.ao.test.junit;

import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.SequenceNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.TriggerNameConverter;
import net.java.ao.test.converters.NameConverters;
import net.java.ao.test.jdbc.DynamicJdbcConfiguration;
import net.java.ao.test.jdbc.Jdbc;
import net.java.ao.test.jdbc.JdbcConfiguration;
import net.java.ao.test.lucene.WithIndex;
import org.junit.rules.MethodRule;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import java.lang.annotation.Annotation;
import java.util.LinkedList;
import java.util.List;

/**
 *
 */
public final class ActiveObjectsJUnitRunner extends BlockJUnit4ClassRunner
{
    private final JdbcConfiguration jdbcConfiguration;
    private final boolean withIndex;
    private final TableNameConverter tableNameConverter;
    private final FieldNameConverter fieldNameConverter;
    private final SequenceNameConverter sequenceNameConverter;
    private final TriggerNameConverter triggerNameConverter;
    private final IndexNameConverter indexNameConverter;

    public ActiveObjectsJUnitRunner(Class<?> klass) throws InitializationError
    {
        super(klass);
        jdbcConfiguration = jdbcConfiguration(klass);
        tableNameConverter = tableNameConverter(klass);
        fieldNameConverter = fieldNameConverter(klass);
        sequenceNameConverter = sequenceNameConverter(klass);
        triggerNameConverter = triggerNameConverter(klass);
        indexNameConverter = indexNameConverter(klass);
        withIndex = withIndex(klass);
    }

    @Override
    protected List<MethodRule> rules(Object test)
    {
        final LinkedList<MethodRule> methodRules = new LinkedList<MethodRule>(super.rules(test));
        methodRules.add(new ActiveObjectTransactionMethodRule(test, jdbcConfiguration, withIndex, tableNameConverter, fieldNameConverter, sequenceNameConverter, triggerNameConverter, indexNameConverter));
        return methodRules;
    }

    private boolean withIndex(Class<?> klass)
    {
        return klass.isAnnotationPresent(WithIndex.class);
    }

    private TableNameConverter tableNameConverter(Class<?> klass)
    {
        return newInstance(getNameConvertersAnnotation(klass).table());
    }

    private FieldNameConverter fieldNameConverter(Class<?> klass)
    {
        return newInstance(getNameConvertersAnnotation(klass).field());
    }

    private SequenceNameConverter sequenceNameConverter(Class<?> klass)
    {
        return newInstance(getNameConvertersAnnotation(klass).sequence());
    }

    private TriggerNameConverter triggerNameConverter(Class<?> klass)
    {
        return newInstance(getNameConvertersAnnotation(klass).trigger());
    }

    private IndexNameConverter indexNameConverter(Class<?> klass)
    {
        return newInstance(getNameConvertersAnnotation(klass).index());
    }

    private NameConverters getNameConvertersAnnotation(Class<?> klass)
    {
        return getAnnotation(klass, NameConverters.class);
    }

    private <A extends Annotation> A getAnnotation(Class<?> klass, Class<A> annotationClass)
    {
        if (klass.isAnnotationPresent(annotationClass))
        {
            return klass.getAnnotation(annotationClass);
        }
        return Annotated.class.getAnnotation(annotationClass);
    }

    private JdbcConfiguration jdbcConfiguration(Class<?> klass)
    {
        return newInstance(getAnnotation(klass, Jdbc.class).value());
    }

    private <T> T newInstance(Class<T> type)
    {
        try
        {
            return type.newInstance();
        }
        catch (InstantiationException e)
        {
            throw new RuntimeException(e);
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
    }

    @NameConverters
    @Jdbc
    private static final class Annotated
    {
    }
}
