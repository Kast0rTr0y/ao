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
package net.java.ao.it.model;

/**
 * @author Daniel Spiewak
 */
public class PersonImpl {
    public static final String LAST_NAME = "Smith";
    public static boolean enableOverride = false;

    private Person person;

    public PersonImpl(Person person) {
        this.person = person;
    }

    public String getLastName() {
        if (enableOverride) {
            return LAST_NAME;
        }
        return person.getLastName();
    }

    public void setLastName(String lastName) {
        person.setLastName(lastName);
    }
}
