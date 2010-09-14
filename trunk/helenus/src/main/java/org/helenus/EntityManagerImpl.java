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
import me.prettyprint.cassandra.model.MultigetSliceQuery;
import me.prettyprint.cassandra.model.Mutator;
import me.prettyprint.cassandra.model.NotFoundException;
import me.prettyprint.cassandra.model.Row;
import me.prettyprint.cassandra.model.Rows;
import me.prettyprint.cassandra.model.Serializer;
import me.prettyprint.cassandra.model.SliceQuery;
import me.prettyprint.cassandra.model.SuperSliceQuery;
import me.prettyprint.cassandra.serializers.BytesSerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.TypeInferringSerializer;
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

public class EntityManagerImpl implements EntityManager {

    private HectorTemplate hectorTemplate;

    private static final Logger logger = LoggerFactory.getLogger(EntityManagerImpl.class);

    private boolean dropKeyspace;

    private Map<Class<?>, AnnotatedClass> classes = new HashMap<Class<?>, AnnotatedClass>();

    static {
        //TODO
        //ConvertUtils.register(Joda time converter);
    }

    public EntityManagerImpl() {
        this(findEntityClasses());
    }

    public EntityManagerImpl(Set<Class<?>> entityClasses) {
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
                    ac.setDependentKey(isDependentKey);
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
                    if (targetMeta == null) {
                        throw new IllegalStateException("No mapped columnFamily with type " + field.getType() + " required by Class/field: " + ac.getClazz().getName() + "/" + field.getName());
                    }
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

    /* (non-Javadoc)
     * @see org.helenus.IEntityManager#init()
     */
    @Override
    public void init() {
        // validate the schema
        Cluster cluster = hectorTemplate.getCluster();
        String keyspaceName = hectorTemplate.getKeyspace();
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
            def.setStrategy_class(hectorTemplate.getReplicationStrategyClass());
            def.setReplication_factor(hectorTemplate.getReplicationFactor());

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

                    if (clazz.hasInverse()) {
                        CfDef inverseCfDef = new CfDef(keyspaceName, clazz.getColumnFamilyName() + clazz.getInverseColumnFamilySuffix());
                        cfDefs.add(inverseCfDef);
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

    /* (non-Javadoc)
     * @see org.helenus.IEntityManager#getById(java.lang.Class, java.io.Serializable)
     */
    @Override
    public <T> T getById(Class<T> clazz, Serializable id) {
        AnnotatedClass meta = getAnnotatedClass(clazz);

        String key = id.toString();

        try {
            T instance = clazz.newInstance();

            for (AnnotatedField field : meta.getFields().values()) {
                ColumnQuery<String, String, byte[]> columnQuery = hectorTemplate.createColumnQuery(BytesSerializer.get());
                columnQuery.setName(field.getColumnName()).setKey(key).setColumnFamily(meta.getColumnFamilyName());
                HColumn<String, byte[]> column = columnQuery.execute().get();
                byte[] bytesValue = column.getValue();
                Object value = SerializerTypeInferer.getSerializer(field.getField().getType()).fromBytes(bytesValue);
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

    /* (non-Javadoc)
     * @see org.helenus.IEntityManager#persist(T)
     */
    @Override
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

        Set<HColumn<String, byte[]>> columns = new HashSet<HColumn<String, byte[]>>();
        Set<HSuperColumn<?, ?, ?>> superColumns = new HashSet<HSuperColumn<?, ?, ?>>();

        try {
            for (AnnotatedField field : meta.getFields().values()) {
                //TODO don't use string conversion, but serialization
                byte[] value = TypeInferringSerializer.get().toBytes(BeanUtils.getProperty(e, field.getField().getName()));
                //cassandra not accepting null values
                if (value == null) {
                    value = new byte[0];
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
            for (HColumn<String,byte[]> column : columns) {
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

    /* (non-Javadoc)
     * @see org.helenus.IEntityManager#getByPropertyValue(java.lang.Class, java.lang.String, java.lang.Object)
     */
    @Override
    public <T> List<T> getByPropertyValue(Class<T> clazz, String propertyName, Object value) {
        AnnotatedClass meta = getAnnotatedClass(clazz);

        AnnotatedField fld = meta.getFields().get(propertyName);
        if (!fld.isSecondaryIndex()) {
            throw new IllegalArgumentException("There is no secondary index defined for this property");
        }

        String  columnName = fld.getColumnName();

        IndexedSlicesQuery<UUID, String> query = hectorTemplate.createIndexSlicesQuery(UUIDSerializer.get(), StringSerializer.get());
        query.setColumnFamily(meta.getColumnFamilyName());
        fillColumnNames(query, meta);
        query.addEqualsExpression(columnName, SerializerTypeInferer.getSerializer(value).toBytes(value));

        List<T> result = getResultList(clazz, meta, query);

        return result;

    }

    private <K, V, T> List<T> getResultList(Class<T> clazz, AnnotatedClass meta,
            AbstractSliceQuery<K, String, V, ? extends Rows<K, String,V>> query) {
        Rows<K, String, V> rows = query.execute().get();

        List<T> result = new ArrayList<T>(rows.getCount());

        try {
            for (Row<K, String, V> row : rows) {
                T entity = clazz.newInstance();
                ColumnSlice<String, V> slice = row.getColumnSlice();
                for (AnnotatedField af : meta.getFields().values()) {
                    HColumn<String, V> column = slice.getColumnByName(af.getColumnName());
                    V value = column.getValue();
                    if (value instanceof byte[]) {
                        value = SerializerTypeInferer.<V>getSerializer(af.getField().getType()).fromBytes((byte[]) value);
                    }
                    BeanUtils.setProperty(entity, af.getField().getName(), value);
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

    /* (non-Javadoc)
     * @see org.helenus.IEntityManager#getHectorTemplate()
     */
    @Override
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

    /* (non-Javadoc)
     * @see org.helenus.IEntityManager#getList(java.lang.Class, K)
     */
    @Override
    public <T, K> List<T> getList(Class<T> clazz, K... ids) {
        AnnotatedClass meta = classes.get(clazz);

        MultigetSliceQuery<K, String, byte[]> query = hectorTemplate.createMultigetSliceQuery(TypeInferringSerializer.<K>get(), StringSerializer.get(), BytesSerializer.get());
        query.setColumnFamily(meta.getColumnFamilyName());
        fillColumnNames(query, meta);
        query.setKeys(ids);

        return getResultList(clazz, meta, query);

    }
    /* (non-Javadoc)
     * @see org.helenus.IEntityManager#getList(java.lang.Class, K, boolean, java.lang.Object, int)
     */
    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T, K> List<T> getList(Class<T> clazz, K id, boolean inverse, Object startColumnName, int count) {
        AnnotatedClass meta = classes.get(clazz);
        Serializer serializer = SerializerTypeInferer.getSerializer(startColumnName);

        if (meta.hasSuperColumn()) {
            SuperSliceQuery<K, byte[], byte[], byte[]> query = hectorTemplate.createSuperSliceQuery();
            query.setKey(id).setRange(serializer.toBytes(startColumnName), null, false, count);
            setColumnFamilyName(query, meta, inverse);
        } else {
            SliceQuery<K, byte[], byte[]> query = hectorTemplate.createSliceQuery();
            query.setKey(id).setRange(serializer.toBytes(startColumnName), null, false, count);
            setColumnFamilyName(query, meta, inverse);

            ColumnSlice<byte[], byte[]> cs = query.execute().get();

            if (!inverse) {
                for (HColumn<byte[], byte[]> column : cs.getColumns()) {

                }
            }
        }

        return null;
    }

    private void setColumnFamilyName(AbstractSliceQuery query, AnnotatedClass meta, boolean inverse){
        String cfName = meta.getColumnFamilyName();
        if (meta.hasInverse() && inverse) {
            cfName +=  meta.getInverseColumnFamilySuffix();
        }
        query.setColumnFamily(cfName);
    }
}
