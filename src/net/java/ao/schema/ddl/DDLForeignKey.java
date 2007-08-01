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
package net.java.ao.schema.ddl;

/**
 * @author Daniel Spiewak
 */
public class DDLForeignKey {
	private String field = "";
	
	private String domesticTable = "";
	private String table = "";
	private String foreignField = "";

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getForeignField() {
		return foreignField;
	}

	public void setForeignField(String foreignField) {
		this.foreignField = foreignField;
	}
	
	public String getFKName() {
		return "fk_" + getTable() + "_" + getField() + "_" + getForeignField();
	}

	public String getDomesticTable() {
		return domesticTable;
	}

	public void setDomesticTable(String domesticTable) {
		this.domesticTable = domesticTable;
	}
	
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		
		if (obj instanceof DDLForeignKey) {
			DDLForeignKey key = (DDLForeignKey) obj;
			
			if (key.field.equals(field) && key.foreignField.equals(foreignField) && key.table.equals(table)) {
				return true;
			}
		}
		
		return false;
	}
	
	@Override
	public int hashCode() {
		return field.hashCode() + table.hashCode() + foreignField.hashCode();
	}
	
	@Override
	public String toString() {
		return getDomesticTable() + "." + getField() + " => " + getTable() + "." + getForeignField();
	}
}
