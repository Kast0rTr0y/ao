package net.java.ao.test.converters;

import net.java.ao.schema.DefaultIndexNameConverter;
import net.java.ao.schema.DefaultSequenceNameConverter;
import net.java.ao.schema.DefaultTriggerNameConverter;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.SequenceNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.TriggerNameConverter;

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.*;

@Documented
@Retention(RUNTIME)
@Target(TYPE)
@Inherited
public @interface NameConverters
{
    Class<? extends TableNameConverter> table() default DynamicTableNameConverter.class;

    Class<? extends FieldNameConverter> field() default DynamicFieldNameConverter.class;

    Class<? extends SequenceNameConverter> sequence() default DefaultSequenceNameConverter.class;

    Class<? extends TriggerNameConverter> trigger() default DefaultTriggerNameConverter.class;

    Class<? extends IndexNameConverter> index() default DefaultIndexNameConverter.class;
}
