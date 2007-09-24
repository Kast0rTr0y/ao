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
package test.schema;

import net.java.ao.OneToMany;
import net.java.ao.RawEntity;
import net.java.ao.schema.AutoIncrement;
import net.java.ao.schema.NotNull;
import net.java.ao.schema.PrimaryKey;

/**
 * @author Daniel Spiewak
 */
public interface Company extends RawEntity {
	
	@PrimaryKey
	@AutoIncrement
	@NotNull
	public long getCompanyID();
	
	public String getName();
	public void setName(String name);
	
	public boolean isCool();
	public void setCool(boolean cool);
	
	public CompanyAddressInfo getAddressInfo();
	public void setAddressInfo(CompanyAddressInfo info);
	
	@OneToMany
	public Person[] getPeople();
}
