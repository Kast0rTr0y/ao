package net.java.ao.db;

import com.google.common.base.Function;
import net.java.ao.DatabaseProvider;
import net.java.ao.schema.StringLength;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.schema.ddl.DDLActionType;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLTable;
import org.junit.Test;

import java.io.IOException;

import static net.java.ao.DatabaseProviders.getOracleDatabaseProvider;
import static net.java.ao.types.TypeQualifiers.qualifiers;

public final class OracleDatabaseProviderTest extends DatabaseProviderTest {
    @Override
    protected String getDatabase() {
        return "oracle";
    }

    @Override
    protected DatabaseProvider getDatabaseProvider() {
        return getOracleDatabaseProvider();
    }

    private Function<DatabaseProvider, DDLAction> createActionAlterColumnSpecialCharacters = new Function<DatabaseProvider, DDLAction>() {
        public DDLAction apply(DatabaseProvider db) {
            DDLTable table = new DDLTable();
            table.setName("company");

            DDLField oldField = new DDLField();
            oldField.setName("name_1");
            oldField.setType(db.getTypeManager().getType(String.class, qualifiers().stringLength(StringLength.MAX_LENGTH)));
            oldField.setNotNull(true);
            table.setFields(new DDLField[]{oldField});

            DDLField field = new DDLField();
            field.setName("name_1");
            field.setType(db.getTypeManager().getType(String.class, qualifiers().stringLength(StringLength.UNLIMITED)));
            field.setNotNull(true);

            DDLAction back = new DDLAction(DDLActionType.ALTER_CHANGE_COLUMN);
            back.setOldField(oldField);
            back.setField(field);
            back.setTable(table);

            return back;
        }
    };

    @Test
    public void testRenderActionAlterStringColumnWithSpecialCharacters() throws IOException {
        testRenderAction("alter-string-special-characters-column.sql", createActionAlterColumnSpecialCharacters, getDatabaseProvider());
    }
}
