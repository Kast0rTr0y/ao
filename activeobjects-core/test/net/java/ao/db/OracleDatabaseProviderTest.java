package net.java.ao.db;

import com.google.common.base.Function;
import net.java.ao.DatabaseProvider;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.schema.ddl.DDLActionType;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLTable;
import org.junit.Test;

import java.io.IOException;

import static net.java.ao.DatabaseProviders.*;

public final class OracleDatabaseProviderTest extends DatabaseProviderTest
{
    @Override
    protected String getDatabase()
    {
        return "oracle";
    }

    @Override
    protected DatabaseProvider getDatabaseProvider()
    {
        return getOracleDatabaseProvider();
    }

    private Function<DatabaseProvider, DDLAction> createActionAddIntegerColumn = new Function<DatabaseProvider, DDLAction>()
    {
        public DDLAction apply(DatabaseProvider db)
        {
            DDLTable table = new DDLTable();
            table.setName("company");

            DDLField field = new DDLField();
            field.setName("name");
            field.setType(db.getTypeManager().getType(Integer.class));
            field.setNotNull(true);

            DDLAction back = new DDLAction(DDLActionType.ALTER_ADD_COLUMN);
            back.setField(field);
            back.setTable(table);

            return back;
        }
    };

    private Function<DatabaseProvider, DDLAction> createActionAddLongColumn = new Function<DatabaseProvider, DDLAction>()
    {
        public DDLAction apply(DatabaseProvider db)
        {
            DDLTable table = new DDLTable();
            table.setName("company");

            DDLField field = new DDLField();
            field.setName("name");
            field.setType(db.getTypeManager().getType(Long.class));
            field.setNotNull(true);

            DDLAction back = new DDLAction(DDLActionType.ALTER_ADD_COLUMN);
            back.setField(field);
            back.setTable(table);

            return back;
        }
    };
}
