package org.helenus.structure;

import java.lang.reflect.Field;

public class AnnotatedField {

    private Field field;
    private String columnName;
    private String superColumnName;
    private String superColumnParentName;
    private String secondaryIndexName;

    private String columnNameField;
    private String superColumnNameField;


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
    public String getSecondaryIndexName() {
        return secondaryIndexName;
    }
    public void setSecondaryIndexName(String secondaryIndexSuffix) {
        this.secondaryIndexName = secondaryIndexSuffix;
    }
    public String getSuperColumnName() {
        return superColumnName;
    }
    public void setSuperColumnName(String superColumnName) {
        this.superColumnName = superColumnName;
    }
    public String getSuperColumnParentName() {
        return superColumnParentName;
    }
    public void setSuperColumnParentName(String superColumnParentName) {
        this.superColumnParentName = superColumnParentName;
    }

    public String getColumnNameField() {
        return columnNameField;
    }
    public void setColumnNameField(String columnNameField) {
        this.columnNameField = columnNameField;
    }
    public String getSuperColumnNameField() {
        return superColumnNameField;
    }
    public void setSuperColumnNameField(String superColumnNameField) {
        this.superColumnNameField = superColumnNameField;
    }
    public boolean isSuperColumn() {
        return superColumnName != null || superColumnNameField != null;
    }

    public boolean isSecondaryIndex() {
        return secondaryIndexName != null;
    }

    public boolean hasSuperColumnParent() {
        return superColumnParentName != null;
    }
}
