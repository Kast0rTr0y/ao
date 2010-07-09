package net.java.ao;

/**
 * Inspired from the {@link Runnable} interface, a bit genericised.
 * @param <T> the type to return from the only method.
 */
public interface Command<T>
{
    T run() throws Exception;
}
