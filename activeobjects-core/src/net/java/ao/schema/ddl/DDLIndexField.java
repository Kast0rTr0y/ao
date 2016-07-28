package net.java.ao.schema.ddl;

import com.google.common.base.Objects;
import net.java.ao.types.TypeInfo;

import static org.apache.commons.lang.StringUtils.equalsIgnoreCase;

public class DDLIndexField {
    private String fieldName;
    private TypeInfo<?> type;

    private DDLIndexField(String fieldName, TypeInfo<?> type) {
        this.fieldName = fieldName;
        this.type = type;
    }

    public String getFieldName() {
        return fieldName;
    }

    public TypeInfo<?> getType() {
        return type;
    }

    @Override
    public String toString() {
        return "DDLIndexField{" +
                "fieldName='" + fieldName + '\'' +
                ", type=" + type +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DDLIndexField that = (DDLIndexField) o;
        return equalsIgnoreCase(fieldName, that.fieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fieldName);
    }

    public static DDLIndexFieldBuilder builder() {
        return new DDLIndexFieldBuilder();
    }

    public static class DDLIndexFieldBuilder {
        private String fieldName;
        private TypeInfo<?> type;

        private DDLIndexFieldBuilder() {}

        public DDLIndexFieldBuilder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public DDLIndexFieldBuilder type(TypeInfo<?> type) {
            this.type = type;
            return this;
        }

        public DDLIndexField build() {
            return new DDLIndexField(fieldName, type);
        }
    }
}
