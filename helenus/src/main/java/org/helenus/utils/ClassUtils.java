package org.helenus.utils;

import java.security.AccessController;
import java.security.PrivilegedAction;

public final class ClassUtils {

    public static Class<?> getClass(String name) {
        Class<?> clazz = null;

        try {
            ClassLoader loader = ClassUtils.getCurrentClassLoader();
            clazz = loader.loadClass(name);

            return clazz;

        } catch (ClassNotFoundException e) {
            try {
                clazz = ClassUtils.class.getClassLoader().loadClass(name);

                return clazz;

            } catch (ClassNotFoundException e1) {
                try {
                    clazz = ClassLoader.getSystemClassLoader().loadClass(name);

                    return clazz;

                } catch (ClassNotFoundException e2) {
                    return null;
                }

            }
        }
    }

    public static ClassLoader getCurrentClassLoader() {
        ClassLoader loader = AccessController
                .doPrivileged(new PrivilegedAction<ClassLoader>() {
                    public ClassLoader run() {
                        try {
                            return Thread.currentThread()
                                    .getContextClassLoader();

                        } catch (Exception e) {
                            return null;
                        }
                    }
                });

        if (loader == null) {
            loader = ClassUtils.class.getClassLoader();
        }

        return loader;
    }
}
