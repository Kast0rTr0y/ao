package net.java.ao.schema.ddl;

import com.google.common.collect.ImmutableList;

/**
 * An ordered list of SQL statements representing some stage of schema modification.
 * This may optionally have a corresponding "undo" action which can be executed to roll
 * back this modification; this will only be done if the modification succeeded but then
 * some later action failed.  Actions should normally consist only of a single statement;
 * use multiple statements only if they are guaranteed to all succeed or all fail together.
 */
public final class SQLAction
{
    private final ImmutableList<String> statements;
    private final SQLAction undoAction;
    
    private SQLAction(Iterable<String> statements, SQLAction undoAction)
    {
        this.statements = ImmutableList.copyOf(statements);
        this.undoAction = undoAction;
    }
    
    public static SQLAction of(CharSequence statement)
    {
        return new SQLAction(ImmutableList.of(statement.toString()), null);
    }
    
    public static SQLAction of(Iterable<String> statements)
    {
        return new SQLAction(statements, null);
    }
    
    public SQLAction withUndoAction(SQLAction undoAction)
    {
        return new SQLAction(this.statements, undoAction);
    }
    
    public Iterable<String> getStatements()
    {
        return statements;
    }
    
    public SQLAction getUndoAction()
    {
        return undoAction;
    }
}
