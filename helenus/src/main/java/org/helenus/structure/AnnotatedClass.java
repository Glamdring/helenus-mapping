package org.helenus.structure;

import java.util.HashMap;
import java.util.Map;

public class AnnotatedClass {

    private Class<?> clazz;
    private String columnFamilyName;
    private Map<String, AnnotatedField> fields = new HashMap<String, AnnotatedField>();
    private String keyFieldName;
    private boolean dependentKey;
    private String dependentKeyFieldName;
    private boolean inverse;
    private String inverseColumnNameField;
    private String inverseColumnFamilySuffix;
    private boolean hasSuperColumn;

    public Class<?> getClazz() {
        return clazz;
    }
    public void setClazz(Class<?> clazz) {
        this.clazz = clazz;
    }
    public String getColumnFamilyName() {
        return columnFamilyName;
    }
    public void setColumnFamilyName(String columnFamilyName) {
        this.columnFamilyName = columnFamilyName;
    }
    public Map<String, AnnotatedField> getFields() {
        return fields;
    }
    public void setFields(Map<String, AnnotatedField> fields) {
        this.fields = fields;
    }
    public String getKeyFieldName() {
        return keyFieldName;
    }
    public void setKeyFieldName(String keyFieldName) {
        this.keyFieldName = keyFieldName;
    }
    public boolean hasDependentKey() {
        return dependentKey;
    }
    public void setDependentKey(boolean dependentKey) {
        this.dependentKey = dependentKey;
    }
    public String getDependentKeyFieldName() {
        return dependentKeyFieldName;
    }
    public void setDependentKeyFieldName(String dependentKeyFieldName) {
        this.dependentKeyFieldName = dependentKeyFieldName;
    }
    public boolean hasInverse() {
        return inverse;
    }
    public void setInverse(boolean inverse) {
        this.inverse = inverse;
    }
    public String getInverseColumnNameField() {
        return inverseColumnNameField;
    }
    public void setInverseColumnNameField(String inverseColumnNameField) {
        this.inverseColumnNameField = inverseColumnNameField;
    }
    public boolean hasSuperColumn() {
        return hasSuperColumn;
    }
    public void setHasSuperColumn(boolean hasSuperColumn) {
        this.hasSuperColumn = hasSuperColumn;
    }
    public String getInverseColumnFamilySuffix() {
        return inverseColumnFamilySuffix;
    }
    public void setInverseColumnFamilySuffix(String inverseColumnFamilySuffix) {
        this.inverseColumnFamilySuffix = inverseColumnFamilySuffix;
    }
}
