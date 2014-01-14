package net.java.ao.test.converters;

import net.java.ao.EntityManager;
import net.java.ao.atlassian.AtlassianIndexNameConverter;
import net.java.ao.atlassian.AtlassianSequenceNameConverter;
import net.java.ao.atlassian.AtlassianTriggerNameConverter;
import net.java.ao.atlassian.AtlassianUniqueNameConverter;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.SequenceNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.TriggerNameConverter;
import net.java.ao.schema.UniqueNameConverter;
import net.java.ao.test.junit.ActiveObjectsJUnitRunner;

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Use with {@link ActiveObjectsJUnitRunner} to specify implementation classes for
 * {@link EntityManager} configuration, to control the naming of tables, fields, etc.
 * <p>
 * The implementation classes specified with this annotation must have no-args constructors.
 * They will be instantiated when the test runner is creating an {@link EntityManager}.
 */
@Documented
@Retention(RUNTIME)
@Target(TYPE)
@Inherited
public @interface NameConverters
{
    /**
     * Specifies an implementation of {@link TableNameConverter}.
     */
    Class<? extends TableNameConverter> table() default DynamicTableNameConverter.class;

    /**
     * Specifies an implementation of {@link FieldNameConverter}.
     */
    Class<? extends FieldNameConverter> field() default DynamicFieldNameConverter.class;

    /**
     * Specifies an implementation of {@link SequenceNameConverter}.
     */
    Class<? extends SequenceNameConverter> sequence() default AtlassianSequenceNameConverter.class;

    /**
     * Specifies an implementation of {@link TriggerNameConverter}.
     */
    Class<? extends TriggerNameConverter> trigger() default AtlassianTriggerNameConverter.class;

    /**
     * Specifies an implementation of {@link IndexNameConverter}.
     */
    Class<? extends IndexNameConverter> index() default AtlassianIndexNameConverter.class;

    /**
     * Specifies an implementation of {@link UniqueNameConverter}.
     */
    Class<? extends UniqueNameConverter> unique() default AtlassianUniqueNameConverter.class;
}
