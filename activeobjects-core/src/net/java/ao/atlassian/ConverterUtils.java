package net.java.ao.atlassian;

import net.java.ao.ActiveObjectsException;

final class ConverterUtils {
    static final int MAX_LENGTH = 30;

    static String checkLength(String name, String errorMsg) {
        if (enforceLength() && name != null && name.length() > MAX_LENGTH) {
            throw new ActiveObjectsException(errorMsg);
        }
        return name;
    }

    private static boolean enforceLength() {
        return Boolean.valueOf(System.getProperty("ao.atlassian.enforce.length", "true"));
    }
}
