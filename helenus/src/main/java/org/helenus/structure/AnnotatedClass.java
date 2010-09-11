package org.helenus.structure;

import java.util.HashMap;
import java.util.Map;

public class AnnotatedClass {

    private Class<?> clazz;
    private String columnFamilyName;
    private Map<String, AnnotatedField> fields = new HashMap<String, AnnotatedField>();
    private String keyFieldName;

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
}
