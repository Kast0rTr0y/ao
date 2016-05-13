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

import java.util.Arrays;

/**
 * <p>Database-agnostic representation of any supported DDL action
 * (usually one or two statements).  The idea behind this class
 * is to allow ActiveObjects to internally represent actions to
 * be taken on a database schema without coupling to a
 * database-specific rendering of the action.</p>
 *
 * <p>As this class is meant to be a generic container of different
 * action types, some fields may not be relevant to certain action
 * types, and thus these fields will contain <code>null</code>
 * values.  For example, the equivalent of an ALTER TABLE DROP FIELD
 * action will return <code>null</code> for {@link #getIndex()}.
 * However, code should not depend on this behavior; instead testing
 * the action type and only retrieving relevant data.</p>
 *
 * @author Daniel Spiewak
 */
public class DDLAction {
    private DDLActionType actionType;

    private DDLTable table;
    private DDLField oldField, field;
    private DDLForeignKey key;
    private DDLIndex index;
    private DDLValue[] values;

    public static DDLActionBuilder builder(DDLActionType actionType) {
        return new DDLActionBuilder(actionType);
    }

    public DDLAction(DDLActionType actionType) {
        this.actionType = actionType;
    }

    DDLAction(
            DDLActionType actionType,
            DDLTable table,
            DDLField oldField,
            DDLField field,
            DDLForeignKey key,
            DDLIndex index,
            DDLValue[] values
    ) {
        this.actionType = actionType;
        this.table = table;
        this.oldField = oldField;
        this.field = field;
        this.key = key;
        this.index = index;
        this.values = values;
    }

    public DDLTable getTable() {
        return table;
    }

    public void setTable(DDLTable table) {
        this.table = table;
    }

    public DDLField getField() {
        return field;
    }

    public void setField(DDLField field) {
        this.field = field;
    }

    public DDLForeignKey getKey() {
        return key;
    }

    public void setKey(DDLForeignKey key) {
        this.key = key;
    }

    public DDLActionType getActionType() {
        return actionType;
    }


    public DDLField getOldField() {
        return oldField;
    }

    public void setOldField(DDLField oldField) {
        this.oldField = oldField;
    }

    public DDLIndex getIndex() {
        return index;
    }

    public void setIndex(DDLIndex index) {
        this.index = index;
    }

    public DDLValue[] getValues() {
        return values;
    }

    public void setValues(DDLValue[] values) {
        this.values = values;
    }

    @Override
    public int hashCode() {
        int back = 0;

        if (actionType != null) {
            back += actionType.hashCode();
        }
        if (table != null) {
            back += table.hashCode();
        }
        if (oldField != null) {
            back += oldField.hashCode();
        }
        if (field != null) {
            back += field.hashCode();
        }
        if (key != null) {
            back += key.hashCode();
        }
        if (index != null) {
            back += index.hashCode();
        }
        if (values != null) {
            back += Arrays.hashCode(values);
        }
        back %= 2 << 15;

        return back;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DDLAction) {
            DDLAction action = (DDLAction) obj;
            if (action == this) {
                return true;
            }

            if ((action.getTable() == null || action.getTable().equals(table))
                    && (action.getActionType() == actionType)
                    && (action.getOldField() == null || action.getOldField().equals(oldField))
                    && (action.getField() == null || action.getField().equals(field))
                    && (action.getKey() == null || action.getKey().equals(key))
                    && (action.getIndex() == null || action.getIndex().equals(index))
                    && (action.getValues() == null || Arrays.equals(action.getValues(), values))) {
                return true;
            }

            return false;
        }

        return super.equals(obj);
    }
}
