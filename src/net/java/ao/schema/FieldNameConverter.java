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
package net.java.ao.schema;

import java.lang.reflect.Method;

import net.java.ao.EntityManager;
import net.java.ao.RawEntity;

/**
 * <p>Superinterface to all field name converters; designed to impose conventions
 * upon the auto-conversion of method names to database fields.  The idea
 * behind this is to allow user-specified field name conventions and standards
 * rather than always enforcing ActiveObjects's idea of "good naming".</p>
 * 
 * <p>Every {@link EntityManager} contains a single field name converter which
 * the entire library uses when performing operations.  Any third-party code which
 * interacts with database fields can also make use of this class.  However, it's
 * significantly harder to do so because Java doesn't support method literals.</p>
 * 
 * <p>Most new implementations of field name converters should extend 
 * {@link AbstractFieldNameConverter} rather than implementing this interface
 * directly.  This allows third-party converters to take advantage of boiler-plate
 * conversion code which would otherwise have to be duplicated in every converter.</p>
 * 
 * @author Daniel Spiewak
 */
public interface FieldNameConverter {
	public String getName(Class<? extends RawEntity<?>> clazz, Method method);
}
