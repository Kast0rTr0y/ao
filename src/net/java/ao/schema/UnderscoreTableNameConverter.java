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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.java.ao.Common;
import net.java.ao.Entity;

/**
 * @author Francis Chong
 * @author Daniel Spiewak
 */
public class UnderscoreTableNameConverter extends AbstractTableNameConverter {
	private static final Pattern WORD_PATTERN = Pattern.compile("([a-z\\d])([A-Z\\d])");
	
	private boolean uppercase;
	
	public UnderscoreTableNameConverter(boolean uppercase) {
		this.uppercase = uppercase;
	}

    @Override
    protected String convertName(Class<? extends Entity> entity) {
    	Matcher matcher = WORD_PATTERN.matcher(Common.convertSimpleClassName(entity.getCanonicalName()));
        String back = matcher.replaceAll("$1_$2");
		
		if (uppercase) {
			back = back.toUpperCase();
		} else {
			back = back.toLowerCase();
		}
        
        return back;
    }
}
