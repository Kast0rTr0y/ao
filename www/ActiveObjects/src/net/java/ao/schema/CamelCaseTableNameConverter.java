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

import net.java.ao.Common;
import net.java.ao.RawEntity;

/**
 * @author Daniel Spiewak
 */
public class CamelCaseTableNameConverter extends AbstractTableNameConverter {

	@Override
	protected String convertName(Class<? extends RawEntity<?>> type) {
		String tableName = Common.convertDowncaseName(Common.convertSimpleClassName(type.getCanonicalName()));
		
		if (type.getAnnotation(Table.class) != null) {
			tableName = type.getAnnotation(Table.class).value();
		}
		
		return tableName;
	}
}
