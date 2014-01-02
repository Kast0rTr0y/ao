package net.java.ao.schema.ddl;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An SQL statement representing some stage of schema modification.
 * This may optionally have a corresponding "undo" action which can be executed to roll
 * back this modification; this will only be done if the modification succeeded but then
 * some later action failed.
 */
public final class SQLAction
{
    private final String statement;
    private final SQLAction undoAction;
    
    private SQLAction(String statement, SQLAction undoAction)
    {
        this.statement = checkNotNull(statement);
        this.undoAction = undoAction;
    }
    
    public static SQLAction of(CharSequence statement)
    {
        return new SQLAction(statement.toString(), null);
    }
    
    public SQLAction withUndoAction(SQLAction undoAction)
    {
        return new SQLAction(this.statement, undoAction);
    }
    
    public String getStatement()
    {
        return statement;
    }
    
    public SQLAction getUndoAction()
    {
        return undoAction;
    }

    @Override
    public String toString()
    {
        return "SQLAction{" +
                "statement='" + statement + '\'' +
                ", undoAction=" + undoAction +
                '}';
    }
}
