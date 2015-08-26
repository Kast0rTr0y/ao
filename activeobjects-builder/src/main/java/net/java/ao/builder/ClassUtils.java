package net.java.ao.builder;

public final class ClassUtils {
    private ClassUtils() {
    }

    public static Class loadClass(String className) {
        try {
            return ClassUtils.class.getClassLoader().loadClass(className);
        } catch (ClassNotFoundException ignored) {
            return null;
        }
    }
}
