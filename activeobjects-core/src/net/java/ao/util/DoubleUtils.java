package net.java.ao.util;

import net.java.ao.ActiveObjectsException;

/**
 * @since version
 */
public class DoubleUtils {
    public static final double MAX_VALUE = 3.40282347e+38;
    //the smallest possible ACTUAL number
    public static final double MIN_VALUE = -MAX_VALUE;

    public static Double checkDouble(Double d) {
        if (d.compareTo(MAX_VALUE) > 0) {
            throw new ActiveObjectsException("The max value of double allowed with Active Objects is " + MAX_VALUE + ", checked double is " + d);
        }

        if (d.compareTo(MIN_VALUE) < 0) {
            throw new ActiveObjectsException("The min value of double allowed with Active Objects is " + MIN_VALUE + ", checked double is " + d);
        }
        return d;
    }
}
