/*
 * Copyright 2007 Daniel Spiewak
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 *	    http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.java.ao;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>Marks a method as relevant only to a many-to-many relation.  This
 * informs ActiveObjects that the return value for the method in question
 * should be determined from a many-to-many relation through the
 * specified type onto the type in the return value.  For example:</p>
 * 
 * <pre>public interface Person {
 *     // ...
 *     
 *     &#064;ManyToMany(Authorship.class)
 *     public Book[] getBooks();
 * }</pre>
 * 
 * <p>Thus the return value of the <code>getBooks()</code> method
 * would be determined by a query something like the following:</p>
 * 
 * <code>SELECT bookID FROM authorships WHERE personID = ?</code>
 * 
 * <p>If the {@link #where()} clause is specified, it will be used
 * <i>in addition</i> to the base, necessary criterion to determine the
 * returned entities.  Thus, the many-to-many relation could be referenced
 * in the following way:</p>
 * 
 * <pre>public interface Person {
 *     // ...
 *     
 *     &#064;ManyToMany(value=Authorship.class, where="deleted = FALSE")
 *     public Book[] getBooks();
 * }</pre>
 * 
 * <p>This would lead to a query like the following:</p>
 * 
 * <code>SELECT bookID FROM authorships WHERE personID = ? AND (deleted = FALSE)</code>
 * 
 * <p>The <code>value()</code> parameter is not optional.</p>
 * 
 * @author Daniel Spiewak
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ManyToMany {
	
	/**
	 * The type through which the many-to-many relation is defined.  For
	 * example, if a <code>people</code> table has a many-to-many
	 * relation on <code>books</code>, it would most likely be through an
	 * <code>authorships</code> table.  Thus, the value of this parameter
	 * could be <code>Authorship.class</code>
	 */
	Class<? extends RawEntity<?>> value();

    /**
     * <p>The name of the property in the joining entity that refers to the annotated entity.</p>
     *
     * <p>If this is not specified, a warning will be logged at migration time, and ActiveObjects may behave in
     * unexpected ways. Future versions of ActiveObjects may require that this property be specified.</p>
     *
     * @see <a href="https://studio.atlassian.com/browse/AO-325">AO-325</a>
     */
    String reverse() default "";

    /**
     * <p>The name of the property in the joining entity that refers to the entities to be returned by the annotated
     * method.</p>
     *
     * <p>If this is not specified, a warning will be logged at migration time, and ActiveObjects may behave in
     * unexpected ways. Future versions of ActiveObjects may require that this property be specified.</p>
     *
     * @see <a href="https://studio.atlassian.com/browse/AO-325">AO-325</a>
     */
    String through() default "";

    /**
	 * <p>A String clause allowing developer-specified additional
	 * conditions to be imposed on the relationship.  The String 
	 * must be a proper SQL WHERE clause:</p>
	 * 
	 * <code>"deleted = FALSE"</code>
	 * 
	 * <p>One must be extremely careful with this sort of thing though
	 * because sometimes (as is the case with the above sample), the 
	 * unparameterized code may not execute as expected against every
	 * database (due to differences in typing and value handling).  Thus,
	 * in all but non-trivial cases, defined implementations should be used.</p>
	 * 
	 * @see net.java.ao.Implementation
	 */
	String where() default "";
}
