package net.java.ao.test.junit;

import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.SequenceNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.TriggerNameConverter;
import net.java.ao.schema.UniqueNameConverter;
import net.java.ao.test.converters.NameConverters;
import net.java.ao.test.jdbc.Jdbc;
import net.java.ao.test.jdbc.JdbcConfiguration;
import net.java.ao.test.lucene.WithIndex;
import org.junit.rules.MethodRule;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import net.java.ao.test.jdbc.NonTransactional;

import net.java.ao.test.jdbc.Data;

import net.java.ao.EntityManager;

import java.lang.annotation.Annotation;
import java.util.LinkedList;
import java.util.List;

/**
 * A JUnit test runner that provides helpful behavior for tests that rely on ActiveObjects.
 * <p>
 * Before each test method is executed, the test runner creates an {@link EntityManager} instance and injects
 * it into the test class; the test class (or a superclass) must have a member field of type EntityManager
 * for this purpose.  The EntityManager configuration is based on the test class annotations {@link Jdbc @Jdbc},
 * {@link Data @Data} and {@link NameConverters @NameConverters}; in the absence of these annotations,
 * the default configuration uses an in-memory HSQL database.
 * <p>
 * By default, each test method is executed within a separate transaction, which is then rolled back so the
 * next test is executed with a fresh database state.  If you annotate a test method with
 * {@link NonTransactional @NonTransactional}, then it is not executed inside a transaction; instead, the
 * database is completely reinitialized after executing the test.
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
    private final UniqueNameConverter uniqueNameConverter;

    public ActiveObjectsJUnitRunner(Class<?> klass) throws InitializationError
    {
        super(klass);
        jdbcConfiguration = jdbcConfiguration(klass);
        tableNameConverter = tableNameConverter(klass);
        fieldNameConverter = fieldNameConverter(klass);
        sequenceNameConverter = sequenceNameConverter(klass);
        triggerNameConverter = triggerNameConverter(klass);
        indexNameConverter = indexNameConverter(klass);
        uniqueNameConverter = uniqueNameConverter(klass);
        withIndex = withIndex(klass);
    }

    @Override
    protected List<MethodRule> rules(Object test)
    {
        final LinkedList<MethodRule> methodRules = new LinkedList<MethodRule>(super.rules(test));
        methodRules.add(new ActiveObjectTransactionMethodRule(test, jdbcConfiguration, withIndex, tableNameConverter, fieldNameConverter, sequenceNameConverter, triggerNameConverter, indexNameConverter, uniqueNameConverter));
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

    private UniqueNameConverter uniqueNameConverter(Class<?> klass) {
        return newInstance(getNameConvertersAnnotation(klass).unique());
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
