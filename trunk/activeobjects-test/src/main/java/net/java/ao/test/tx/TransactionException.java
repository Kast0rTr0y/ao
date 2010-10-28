package net.java.ao.test.tx;

/**
 * Used when there is an issue with a transaction.
 */
public final class TransactionException extends RuntimeException
{
    public TransactionException(Throwable cause)
    {
        super(cause);
    }
}
