package net.java.ao.test;

import net.java.ao.schema.CamelCaseFieldNameConverter;
import net.java.ao.schema.CamelCaseTableNameConverter;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.TableNameConverter;

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Documented
@Retention(RUNTIME)
@Target(TYPE)
@Inherited
public @interface NameConverters
{
    Class<? extends TableNameConverter> table() default CamelCaseTableNameConverter.class;

    Class<? extends FieldNameConverter> field() default CamelCaseFieldNameConverter.class;
}
