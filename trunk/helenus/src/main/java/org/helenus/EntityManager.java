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
import me.prettyprint.cassandra.model.HSuperColumn;
import me.prettyprint.cassandra.model.HectorTransportException;
import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.model.Mutator;
import me.prettyprint.cassandra.model.NotFoundException;
import me.prettyprint.cassandra.model.OrderedRows;
import me.prettyprint.cassandra.model.Row;
import me.prettyprint.cassandra.model.Serializer;
import me.prettyprint.cassandra.model.SliceQuery;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
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
import org.helenus.annotation.CassandraColumnName;
import org.helenus.annotation.CassandraDependentKey;
import org.helenus.annotation.CassandraInverseColumnFamily;
import org.helenus.annotation.CassandraInverseColumnName;
import org.helenus.annotation.CassandraKey;
import org.helenus.annotation.CassandraSecondaryIndex;
import org.helenus.annotation.CassandraSuperColumn;
import org.helenus.annotation.CassandraSuperColumnName;
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
            CassandraColumnFamily cf = clazz.getAnnotation(CassandraColumnFamily.class);
            if (cf == null) {
                throw new IllegalStateException("You must not provide classes that are not mapped with @CassandraColumnFamily");
            }
            String columnFamilyName = cf.name();
            CassandraInverseColumnFamily inverseCf = clazz.getAnnotation(CassandraInverseColumnFamily.class);
            if (inverseCf != null) {
                ac.setInverse(true);
                ac.setInverseColumnFamilySuffix(inverseCf.suffix());
            }

            if (columnFamilyName.isEmpty()) {
                columnFamilyName = clazz.getSimpleName().toLowerCase();
            }
            ac.setColumnFamilyName(columnFamilyName);

            Field[] fields = clazz.getDeclaredFields();
            Map<String, AnnotatedField> annotatedFields = new HashMap<String, AnnotatedField>();
            for (Field field : fields) {
                AnnotatedField af = new AnnotatedField();
                af.setField(field);

                if (field.isAnnotationPresent(CassandraInverseColumnName.class)) {
                    ac.setInverseColumnNameField(field.getName());
                }

                CassandraColumn column = field.getAnnotation(CassandraColumn.class);
                CassandraSuperColumn superColumn = field.getAnnotation(CassandraSuperColumn.class);
                CassandraColumnName columnNameAnnot = field.getAnnotation(CassandraColumnName.class);
                CassandraSuperColumnName superColumnNameAnnot = field.getAnnotation(CassandraSuperColumnName.class);

                if (column != null && superColumn != null) {
                    throw new IllegalStateException("You can't have one field annotated with both @CassandraColumn and @CassandraSuperColumn. Class/Field: " + ac.getClazz().getName() + "/" + field.getName());
                }
                if (columnNameAnnot != null && superColumnNameAnnot != null) {
                    throw new IllegalStateException("You can't have one field annotated with both @CassandraColumnName and @CassandraSuperColumnName. Class/Field: " + ac.getClazz().getName() + "/" + field.getName());
                }

                boolean isKey = field.isAnnotationPresent(CassandraKey.class);
                boolean isDependentKey = field.isAnnotationPresent(CassandraDependentKey.class);

                if (isDependentKey && !entityClasses.contains(field.getType())) {
                    throw new IllegalStateException("Dependent keys can only be defined to other mapped entities. Class/Field: " + clazz.getName() + "/" + field.getName());
                }

                if (isKey || isDependentKey) {
                    ac.setKeyFieldName(field.getName());
                    ac.setDependentKey(true);
                    continue;
                }
                boolean isSecondaryIndex = field.isAnnotationPresent(CassandraSecondaryIndex.class);

                String columnName = getColumnName(field, column);
                String superColumnName = getSuperColumnName(field, superColumn);
                String columnNameField = columnNameAnnot != null ? columnNameAnnot.field() : null;
                String superColumnNameField = superColumnNameAnnot != null ? superColumnNameAnnot.field() : null;

                af.setColumnName(columnName);
                af.setSuperColumnName(superColumnName);
                af.setColumnNameField(columnNameField);
                af.setSuperColumnNameField(superColumnNameField);

                if (column != null) {
                    if (!column.targetSuperColumnField().isEmpty()) {
                        af.setSuperColumnParentName(column.targetSuperColumnField());
                    }
                }
                if (isSecondaryIndex) {
                    String secondaryIndexName = field.getAnnotation(CassandraSecondaryIndex.class).name();
                    if (secondaryIndexName.isEmpty()) {
                        secondaryIndexName = StringUtils.capitalize(field.getName());
                    }
                    af.setSecondaryIndexName(secondaryIndexName);
                }

                if (superColumn != null || superColumnNameField != null) {
                    ac.setHasSuperColumn(true);
                }

                if (column != null || superColumn != null
                        || columnNameField != null
                        || superColumnNameField != null) {
                    annotatedFields.put(field.getName(), af);
                }
            }
            ac.setFields(annotatedFields);
            classes.put(clazz, ac);
        }

        try {
            for (AnnotatedClass ac : classes.values()) {
                if (ac.hasDependentKey()) {
                    Field field = ac.getClazz().getDeclaredField(ac.getKeyFieldName());
                    AnnotatedClass targetMeta = classes.get(field.getType());
                    ac.setDependentKeyFieldName(targetMeta.getKeyFieldName());
                }

                if (ac.hasInverse()) {
                    int superColumns = 0;
                    int columns = 0;
                    for (AnnotatedField field : ac.getFields().values()) {
                        if (field.isSuperColumn()) {
                            superColumns++;
                        } else {
                            columns ++;
                        }
                    }
                    // for inverse, only one super column def is allowed,
                    // or if no super column exists, only one column def
                    // (this does not mean the CF will have one column -
                    // it means the object has one field based on which
                    // columns are generated
                    if (superColumns > 1 || (superColumns == 0 && columns > 1)) {
                        throw new IllegalStateException("For an inverse definition you must have only one SuperColumn, or if you don't have a SuperColumn, you must have only one Column");
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getSuperColumnName(Field field,
            CassandraSuperColumn superColumn) {
        if (superColumn == null) {
            return null;
        }
        String columnName = superColumn.name();
        if (columnName.isEmpty()) {
            columnName = field.getName();
        }
        return columnName;
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

        Object key = getKey(e, meta);

        if (key == null) {
            logger.debug("Inserting a new entity of type " + e.getClass());
            return persist(e, meta, UUID.randomUUID());
        } else {
            return persist(e, meta, key);
        }
    }

    private Object getKey(Object entity, AnnotatedClass meta) {
        try {
            if (!meta.hasDependentKey()) {
                return BeanUtils.getProperty(entity, meta.getKeyFieldName());
            } else {
                Object keyHoldingObject = BeanUtils.getProperty(entity, meta.getKeyFieldName());
                Object key = BeanUtils.getProperty(keyHoldingObject, meta.getDependentKeyFieldName());
                if (key == null) {
                    throw new IllegalStateException("The dependee of a dependent key must be set. Class/field: " + meta.getClazz().getName() + " / " + meta.getKeyFieldName());
                } else {
                    return key;
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <T> T persist(T e, AnnotatedClass meta, Object key) {

        Set<HColumn<String, String>> columns = new HashSet<HColumn<String, String>>();
        Set<HSuperColumn<?, ?, ?>> superColumns = new HashSet<HSuperColumn<?, ?, ?>>();

        try {
            for (AnnotatedField field : meta.getFields().values()) {
                String value = ConvertUtils.convert(BeanUtils.getProperty(e, field.getField().getName()));
                //cassandra not accepting null values
                if (value == null) {
                    value = "";
                }
                // Nasty parameterization with Object, but no other way
                if (field.isSuperColumn()) {
                    List<HColumn<Object,Object>> columnsOfSuperColumn = new ArrayList<HColumn<Object,Object>>();
                    for (AnnotatedField columnCandidate : meta.getFields().values()) {
                        HColumn column = hectorTemplate.createColumn(columnCandidate.getColumnName(), value);
                        columnsOfSuperColumn.add(column);
                    }
                    String name = getSuperColumnName(e, field);
                    HSuperColumn<?,?,?> sc = hectorTemplate.<Object, Object, Object>createSuperColumn(name, columnsOfSuperColumn);
                    superColumns.add(sc);
                } else if (!field.hasSuperColumnParent()){ // otherwise they are already created above

                    String name = getColumnName(e, field);

                    columns.add(hectorTemplate.createColumn(name, value));
                }
            }
            Mutator mutator = hectorTemplate.createMutator();
            String cfName = meta.getColumnFamilyName();
            for (HColumn<String,String> column : columns) {
                logger.debug("Adding insertion for key: " + key + ", column: " + column.getName() + ", cfName:" + cfName);
                mutator.addInsertion(key, cfName, column);
            }

            for (HSuperColumn<?, ?, ?> superColumn : superColumns) {
                logger.debug("Adding insertion for key: " + key + ", superColumn: " + superColumn.getName() + ", cfName:" + cfName);
                mutator.addInsertion(key, cfName, superColumn);
            }

            mutator.execute();

            handleInverse(e, key, meta, cfName, mutator);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return e;
    }

    private <T> String getColumnName(T e, AnnotatedField field)
            throws IllegalAccessException, InvocationTargetException,
            NoSuchMethodException {
        String name = field.getColumnName();
        // ie if this has been annotated with @CassandraColumnName
        if (name == null) {
            String fieldName = field.getColumnNameField();
            Object target = BeanUtils.getProperty(e, field.getField().getName());
            name = BeanUtils.getProperty(target, fieldName);
        }
        return name;
    }

    private <T> String getSuperColumnName(T e, AnnotatedField field)
            throws IllegalAccessException, InvocationTargetException,
            NoSuchMethodException {
        String name = field.getSuperColumnName();
        // ie if this has been annotated with @CassandraSuperColumnName
        if (name == null) {
            String fieldName = field.getSuperColumnNameField();
            Object target = BeanUtils.getProperty(e, field.getField().getName());
            name = BeanUtils.getProperty(target, fieldName);
        }
        return name;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <T> void handleInverse(T e, Object key, AnnotatedClass meta,
            String cfName, Mutator mutator) throws IllegalAccessException,
            InvocationTargetException, NoSuchMethodException {

        cfName = cfName + meta.getInverseColumnFamilySuffix();

        if (meta.hasInverse()) {
            Object columnName = BeanUtils.getProperty(e, meta.getInverseColumnNameField());
            Object value = null;
            for (AnnotatedField field : meta.getFields().values()) {
                if (field.isSuperColumn()) {
                    value = getSuperColumnName(e, field);
                    break;
                } else if (!meta.hasSuperColumn()) {
                    value = getColumnName(e, field);
                    break;
                }
            }
            HColumn<?,?> column = hectorTemplate.createColumn(columnName, value);

            mutator.insert(key, cfName, column);
        }
    }

    public <T,V> List<T> getByPropertyValue(Class<T> clazz, String propertyName, V value) {
        String columnName = "";
        AnnotatedClass meta = getAnnotatedClass(clazz);

        AnnotatedField fld = meta.getFields().get(propertyName);
        if (!fld.isSecondaryIndex()) {
            throw new IllegalArgumentException("There is no secondary index defined for this property");
        }

        columnName = fld.getColumnName();

        Serializer<V> serializer = SerializerTypeInferer.getSerializer(value);
        IndexedSlicesQuery<UUID, String, V> query = hectorTemplate.createIndexSlicesQuery(UUIDSerializer.get(), serializer);
        query.setColumnFamily(meta.getColumnFamilyName());
        fillColumnNames(query, meta);
        query.addEqualsExpression(columnName, value);

        OrderedRows<UUID, String, V> rows = query.execute().get();

        List<T> result = new ArrayList<T>(rows.getCount());

        try {
            for (Row<UUID, String, V> row : rows) {
                T entity = clazz.newInstance();
                ColumnSlice<String, V> slice = row.getColumnSlice();
                for (AnnotatedField af : meta.getFields().values()) {
                    HColumn<String, V> column = slice.getColumnByName(af.getColumnName());
                    BeanUtils.setProperty(entity, af.getField().getName(), column.getValue());
                }
                BeanUtils.setProperty(entity, meta.getKeyFieldName(), row.getKey());
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

    public <T> List<T> getList(Class<T> clazz, Object id, boolean inverse, int start,
            int count) {
        AnnotatedClass meta = classes.get(clazz);
        SliceQuery query = hectorTemplate.createSliceQuery(null, null, null);
        //TODO return;
        return null;
    }
}
