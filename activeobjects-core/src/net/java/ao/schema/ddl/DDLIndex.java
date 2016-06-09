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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public boolean containsFieldWithName(final String fieldName) {
        return Stream.of(getFields())
                .map(DDLIndexField::getFieldName)
                .anyMatch(indexFieldName -> indexFieldName.equals(fieldName));
    }

    public boolean containsFieldWithNameIgnoreCase(final String fieldName) {
        return Stream.of(getFields())
                .map(DDLIndexField::getFieldName)
                .anyMatch(indexFieldName -> indexFieldName.equalsIgnoreCase(fieldName));
    }

    @Override
    public String toString() {
        return "DDLIndex{" +
                "table='" + table + '\'' +
                ", fields=" + Arrays.toString(fields) +
                ", indexName='" + indexName + '\'' +
                '}';
    }

    /**
     * Check if this is equal to other index.
     * <p>
     *      Two indexes are considered equivalent if and only if they have exactly the same column names in the same
     *      order and the same table name specified.
     *      Please note that index name does not matter when checking for index equality.
     * </p>
     *
     * @param o object to compare with
     * @return true if index is equivalent to the other index false otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DDLIndex index = (DDLIndex) o;
        return Objects.equal(table, index.table) &&
                Arrays.equals(fields, index.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(table) + Arrays.hashCode(fields);

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

        public DDLIndexBuilder fields(DDLIndexField... fields) {
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
