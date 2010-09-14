package org.helenus;

import java.io.Serializable;
import java.util.List;

import me.prettyprint.cassandra.service.spring.HectorTemplate;

public interface EntityManager {

    void init();

    <T> T getById(Class<T> clazz, Serializable id);

    <T> T persist(T e);

    <T> List<T> getByPropertyValue(Class<T> clazz,
            String propertyName, Object value);

    HectorTemplate getHectorTemplate();

    <T, K> List<T> getList(Class<T> clazz, K... ids);

    <T, K> List<T> getList(Class<T> clazz, K id, boolean inverse,
            Object startColumnName, int count);

}