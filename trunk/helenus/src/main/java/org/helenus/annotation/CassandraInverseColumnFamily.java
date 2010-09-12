package org.helenus.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The class annotated with @CassandraInverseColumnFamily should also be
 * annotated with @ColumnFamily. "Inverse" here means that an inverse column
 * family will be created and supported, with a key equal to the (should be 1)
 * @(Super)ColumnName defined on this class, a column name taken from the field
 * annotated with @CassandraInverseColumnName, and a value - the key of the
 * current class;
 *
 * @author Bozhidar Bozhanov
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface CassandraInverseColumnFamily {

    String suffix() default "Inverse";
}
