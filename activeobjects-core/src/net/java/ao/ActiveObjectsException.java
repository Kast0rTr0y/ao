package net.java.ao;

/**
 * This is a generic Active Objects exception.
 */
public class ActiveObjectsException extends RuntimeException {
    public ActiveObjectsException() {
    }

    public ActiveObjectsException(String message) {
        super(message);
    }

    public ActiveObjectsException(String message, Throwable cause) {
        super(message, cause);
    }

    public ActiveObjectsException(Throwable cause) {
        super(cause);
    }
}
