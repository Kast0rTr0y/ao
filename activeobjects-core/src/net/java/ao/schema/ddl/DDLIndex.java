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

import com.google.common.base.Objects;
import net.java.ao.types.TypeInfo;

import java.util.Arrays;

/**
 * Database-agnostic reprensentation of a general field index
 * statement (not related to full-text indexing).  To save on
 * object creation, as well as to simplify schema parsing, table
 * and field <i>names</i> are stored rather than full DDL
 * representations.  This class also defines the convention
 * imposed to generate the names of field indexes.  It is
 * important that all DDL renderers (i.e. database providers)
 * observe this convention, else migrations will do strange things.
 *
 * @author Daniel Spiewak
 */
public class DDLIndex {
    private String table;
    private DDLIndexField[] fields = {};
    private String indexName;

    public static DDLIndexBuilder builder() {
        return new DDLIndexBuilder();
    }

    private DDLIndex(String table, DDLIndexField[] fields, String indexName) {
        this.table = table;
        this.fields = fields;
        this.indexName = indexName;
    }

    public String getTable() {
        return table;
    }

    public DDLIndexField[] getFields() {
        return fields;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getField() {
        if (fields.length < 1) {
            return null;
        }
        return fields[0].getFieldName();
    }

    public TypeInfo<?> getType() {
        if (fields.length < 1) {
            return null;
        }
        return fields[0].getType();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DDLIndex index = (DDLIndex) o;
        return Objects.equal(table, index.table) &&
                Arrays.equals(fields, index.fields) &&
                Objects.equal(indexName, index.indexName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(table, indexName) + Arrays.hashCode(fields);

    }

    public static class DDLIndexBuilder {
        private String table;
        private DDLIndexField[] fields;
        private String indexName;

        private DDLIndexBuilder() {}

        public DDLIndexBuilder table(String table) {
            this.table = table;
            return this;
        }

        public DDLIndexBuilder field(DDLIndexField field) {
            this.fields = new DDLIndexField[]{field};
            return this;
        }

        public DDLIndexBuilder fields(DDLIndexField[] fields) {
            this.fields = fields;
            return this;
        }

        public DDLIndexBuilder indexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public DDLIndex build() {
            return new DDLIndex(table, fields, indexName);
        }
    }
}
