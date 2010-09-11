package org.helenus;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import me.prettyprint.cassandra.model.AbstractSliceQuery;
import me.prettyprint.cassandra.model.ColumnSlice;
import me.prettyprint.cassandra.model.HColumn;
import me.prettyprint.cassandra.model.HectorTransportException;
import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.model.Mutator;
import me.prettyprint.cassandra.model.NotFoundException;
import me.prettyprint.cassandra.model.OrderedRows;
import me.prettyprint.cassandra.model.Row;
import me.prettyprint.cassandra.model.Serializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.Cluster;
import me.prettyprint.cassandra.service.spring.HectorTemplate;
import me.prettyprint.hector.api.query.ColumnQuery;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.KsDef;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.StringUtils;
import org.helenus.annotation.CassandraColumn;
import org.helenus.annotation.CassandraColumnFamily;
import org.helenus.annotation.Key;
import org.helenus.annotation.SecondaryIndex;
import org.helenus.structure.AnnotatedClass;
import org.helenus.structure.AnnotatedField;
import org.scannotation.AnnotationDB;
import org.scannotation.ClasspathUrlFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityManager {

    private HectorTemplate hectorTemplate;

    private static final Logger logger = LoggerFactory.getLogger(EntityManager.class);

    private boolean dropKeyspace;

    private Map<Class<?>, AnnotatedClass> classes = new HashMap<Class<?>, AnnotatedClass>();

    static {
        //TODO
        //ConvertUtils.register(Joda time converter);
    }

    public EntityManager() {
        this(findEntityClasses());
    }

    public EntityManager(Set<Class<?>> entityClasses) {
        for (Class<?> clazz : entityClasses) {
            AnnotatedClass ac = new AnnotatedClass();
            ac.setClazz(clazz);
            String columnFamilyName = clazz.getAnnotation(CassandraColumnFamily.class).name();
            if (columnFamilyName.isEmpty()) {
                columnFamilyName = clazz.getSimpleName().toLowerCase();
            }
            ac.setColumnFamilyName(columnFamilyName);

            Field[] fields = clazz.getDeclaredFields();
            Map<String, AnnotatedField> annotatedFields = new HashMap<String, AnnotatedField>();
            for (Field field : fields) {
                AnnotatedField af = new AnnotatedField();
                af.setField(field);
                CassandraColumn column = field.getAnnotation(CassandraColumn.class);
                boolean isKey = field.isAnnotationPresent(Key.class);
                boolean isSecondaryIndex = field.isAnnotationPresent(SecondaryIndex.class);

                String columnName = getColumnName(field, column);
                af.setColumnName(columnName);

                af.setKey(isKey);
                af.setSecondaryIndex(isSecondaryIndex);

                if (isSecondaryIndex) {
                    String secondaryIndexName = field.getAnnotation(SecondaryIndex.class).name();
                    if (secondaryIndexName.isEmpty()) {
                        secondaryIndexName = StringUtils.capitalize(field.getName());
                    }
                    af.setSecondaryIndexName(secondaryIndexName);
                }

                if (column != null || isKey) {
                    annotatedFields.put(field.getName(), af);
                }

                if (isKey) {
                    ac.setKeyFieldName(field.getName());
                }
            }
            ac.setFields(annotatedFields);
            classes.put(clazz, ac);
        }
    }

    private String getColumnName(Field field, CassandraColumn column) {
        if (column == null) {
            return null;
        }
        String columnName = column.name();
        if (columnName.isEmpty()) {
            columnName = field.getName();
        }
        return columnName;
    }

    public void init() {
        // validate the schema
        Cluster cluster = hectorTemplate.getCluster();
        String keyspaceName = hectorTemplate.getFactory().getKeyspace();
        CassandraClient cClient = cluster.borrowClient();

        Cassandra.Client client = cClient.getCassandra();
        try {
            if (dropKeyspace) {
                client.system_drop_keyspace(keyspaceName);
            }
        } catch (Exception ex) {
            logger.error("Unable to drop keyspace", ex);
        }
        try {
            cClient.getKeyspace(keyspaceName);
        } catch (NotFoundException ex) {
            KsDef def = new KsDef();
            def.setName(keyspaceName);
            def.setStrategy_class(hectorTemplate.getFactory().getReplicationStrategyClass());
            def.setReplication_factor(hectorTemplate.getFactory().getReplicationFactor());

            try {
                List<CfDef> cfDefs = new ArrayList<CfDef>(classes.size());
                for (AnnotatedClass clazz : classes.values()) {
                    String cfName = clazz.getColumnFamilyName();
                    CfDef cfDef = new CfDef(keyspaceName, cfName);
                    cfDefs.add(cfDef);
                    String validationClass = BytesType.class.getName();
                    for (AnnotatedField field : clazz.getFields().values()) {
                        if (field.isSecondaryIndex()) {
                            ColumnDef cd = new ColumnDef();
                            cd.setName(StringSerializer.get().toBytes(field.getColumnName()));
                            cd.setIndex_type(IndexType.KEYS);
                            cd.setIndex_name(field.getSecondaryIndexName());
                            cd.setValidation_class(validationClass);
                            cfDef.addToColumn_metadata(cd);
                        }
                    }
                    //client.system_add_column_family(cfDef);
                }
                def.setCf_defs(cfDefs);
                client.system_add_keyspace(def);
            } catch (Exception e) {
                logger.error("Problem creating keyspace and column families", e);
                throw new RuntimeException(e);
            }
        } catch (HectorTransportException ex) {
            logger.error("Problem getting keyspace", ex);
        }

    }

    private static Set<Class<?>> findEntityClasses() {
        Set<Class<?>> entityClasses = new HashSet<Class<?>>();

        AnnotationDB annotationDb = new AnnotationDB();
        URL[] urls = ClasspathUrlFinder.findClassPaths();
        try {
            annotationDb.scanArchives(urls);
            Map<String, Set<String>> index = annotationDb.getClassIndex();

            if (index != null) {
                Set<String> strSet = index.keySet();
                if (strSet != null) {
                    for (String str : strSet) {
                        Class<?> clazz = ClassUtils.getClass(str);
                        if (clazz.isAnnotationPresent(CassandraColumnFamily.class)) {
                            entityClasses.add(clazz);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return entityClasses;
    }

    public <T> T getById(Class<T> clazz, Serializable id) {
        AnnotatedClass meta = getAnnotatedClass(clazz);

        String key = id.toString();

        try {
            T instance = clazz.newInstance();

            for (AnnotatedField field : meta.getFields().values()) {
                ColumnQuery<String, String, String> columnQuery = hectorTemplate.createColumnQuery();
                columnQuery.setName(field.getColumnName()).setKey(key).setColumnFamily(meta.getColumnFamilyName());
                HColumn<String, String> column = columnQuery.execute().get();
                String stringValue = column.getValue();
                Object value = ConvertUtils.convert(stringValue, field.getField().getType());
                BeanUtils.setProperty(instance, field.getField().getName(), value);
            }

            return instance;
        } catch (Exception ex) {
            logger.error("Exception when getting object by id", ex);
            return null;
        }
    }

    private AnnotatedClass getAnnotatedClass(Class<?> clazz) {
        AnnotatedClass result = classes.get(clazz);
        if (result == null) {
            throw new IllegalStateException("Class " + clazz.getName()
                    + " is not mapped. Annotate with @CassandraColumnFamily");
        }

        return result;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T> T persist(T e) {
        AnnotatedClass meta = getAnnotatedClass(e.getClass());
        ValidatorFactory factory;
        try {
            Class clazz = Class.forName("org.hibernate.validator.HibernateValidator");
            factory = Validation.byProvider(clazz).configure().buildValidatorFactory();
        } catch (ClassNotFoundException ex) {
            factory = Validation.buildDefaultValidatorFactory();
        }
        Validator validator = factory.getValidator();
        Set constraintViolations = validator.validate(e);
        if (constraintViolations.size() > 0) {
            throw new ConstraintViolationException(constraintViolations);
        }

        Object key;
        try {
            key = BeanUtils.getProperty(e, meta.getKeyFieldName());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        // if it's a new object, insert. otherwise - update
        if (key == null) {
            logger.debug("Inserting a new entity of type " + e.getClass());
            return insert(e, meta);
        } else {
            return null;
        }

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <T> T insert(T e, AnnotatedClass meta) {

        Object key = UUID.randomUUID();

        Set<HColumn<String, String>> columns = new HashSet<HColumn<String, String>>();
        try {
            for (AnnotatedField field : meta.getFields().values()) {
                if (!field.isKey()) {
                    String value = ConvertUtils.convert(BeanUtils.getProperty(e, field.getField().getName()));
                    //cassandra not accepting null values
                    if (value == null) {
                        value = "";
                    }
                    columns.add(hectorTemplate.createColumn(field.getColumnName(), value));
                }

            }
            Mutator mutator = hectorTemplate.createMutator();
            String cfName = meta.getColumnFamilyName();
            for (HColumn<String,String> column : columns) {
                logger.debug("Adding insertion for key: " + key + ", column: " + column + ", cfName:" + cfName);
                mutator.addInsertion(key, cfName, column);
            }

            mutator.execute();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return e;
    }

    public <K, T,V> List<T> getByPropertyValue(Class<T> clazz, String propertyName, V value) {
        String columnName = "";
        AnnotatedClass meta = getAnnotatedClass(clazz);

        AnnotatedField fld = meta.getFields().get(propertyName);
        if (!fld.isSecondaryIndex()) {
            throw new IllegalArgumentException("There is no secondary index defined for this property");
        }

        columnName = fld.getColumnName();

        Serializer<V> serializer = SerializerTypeInferer.getSerializer(value);
        IndexedSlicesQuery<K, String, V> query = hectorTemplate.createIndexSlicesQuery(serializer);
        query.setColumnFamily(meta.getColumnFamilyName());
        fillColumnNames(query, meta);
        query.addEqualsExpression(columnName, value);

        OrderedRows<K, String, V> rows = query.execute().get();

        List<T> result = new ArrayList<T>(rows.getCount());

        try {
            for (Row<K, String, V> row : rows) {
                T entity = clazz.newInstance();
                ColumnSlice<String, V> slice = row.getColumnSlice();
                for (AnnotatedField af : meta.getFields().values()) {
                    if (!af.isKey()) {
                        HColumn<String, V> column = slice.getColumnByName(af.getColumnName());
                        BeanUtils.setProperty(entity, af.getField().getName(), column.getValue());
                    } else {
                        BeanUtils.setProperty(entity, af.getField().getName(), row.getKey());
                    }
                }
                result.add(entity);
            }
        } catch (InstantiationException ex) {
            throw new RuntimeException(ex);
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        } catch (InvocationTargetException ex) {
            throw new RuntimeException(ex);
        }

        return result;

    }

    private <K, V, T> void fillColumnNames(AbstractSliceQuery<K, String, V, T> query, AnnotatedClass meta) {
        String[] columnNames = new String[meta.getFields().size()];
        int i = 0;
        for (AnnotatedField field : meta.getFields().values()) {
            columnNames[i++] = field.getColumnName();
        }
        query.setColumnNames(columnNames);
    }

    public HectorTemplate getHectorTemplate() {
        return hectorTemplate;
    }

    public void setHectorTemplate(HectorTemplate cassandraManager) {
        this.hectorTemplate = cassandraManager;
    }

    public boolean isDropKeyspace() {
        return dropKeyspace;
    }

    public void setDropKeyspace(boolean dropKeyspace) {
        this.dropKeyspace = dropKeyspace;
    }
}
