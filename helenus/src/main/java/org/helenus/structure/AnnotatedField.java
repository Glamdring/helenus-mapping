package org.helenus.structure;

import java.lang.reflect.Field;

public class AnnotatedField {

    private Field field;
    private String columnName;
    private boolean key;
    private boolean secondaryIndex;
    private String secondaryIndexName;

    public Field getField() {
        return field;
    }
    public void setField(Field field) {
        this.field = field;
    }
    public String getColumnName() {
        return columnName;
    }
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
    public boolean isKey() {
        return key;
    }
    public void setKey(boolean key) {
        this.key = key;
    }
    public boolean isSecondaryIndex() {
        return secondaryIndex;
    }
    public void setSecondaryIndex(boolean secondaryIndex) {
        this.secondaryIndex = secondaryIndex;
    }
    public String getSecondaryIndexName() {
        return secondaryIndexName;
    }
    public void setSecondaryIndexName(String secondaryIndexSuffix) {
        this.secondaryIndexName = secondaryIndexSuffix;
    }
}
