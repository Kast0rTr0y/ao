package net.java.ao.it;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import net.java.ao.DBParam;
import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.Preload;
import net.java.ao.Query;
import net.java.ao.schema.Indexed;
import net.java.ao.schema.NotNull;
import net.java.ao.schema.StringLength;
import net.java.ao.schema.Table;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import org.junit.Test;

import java.util.Date;

@Data(TestClobError.TestClobErrorDatabaseUpdater.class)
public final class TestClobError extends ActiveObjectsIntegrationTest {
    public static final class TestClobErrorDatabaseUpdater implements DatabaseUpdater {
        @Override
        public void update(EntityManager entityManager) throws Exception {
            entityManager.migrate(AoProjectActivity.class);
        }
    }

    @Preload
    @Table("ProjectActivity")
    public interface AoProjectActivity extends Entity {

        @NotNull
        @Indexed
        Integer getProjectId();

        @NotNull
        @Indexed
        String getActivityType();

        @NotNull
        Date getDate();

        @Indexed
        @NotNull
        Integer getActivityRepoId();

        @Indexed
        @NotNull
        Integer getActivityStashUserId();

        String getRepositoryName();

        String getBranchId();

        String getPreviousBranchId();

        Long getPullRequestId();

        Long getCommentId();

        @StringLength(StringLength.UNLIMITED)
        String getCommentText();

        @StringLength(StringLength.UNLIMITED)
        String getDiffAsHtml();

        Integer getReplyToUserId();

        String getCommitId();

        String getFromHash();

        String getToHash();

        Integer getNumOfCommits();

        @Indexed
        Integer getRepositoryForkId();

        String getRepositoryForkName();

    }

    @Test
    public void testVarbinaryNtext() throws Exception {
        entityManager.create(AoProjectActivity.class,
                new DBParam("COMMENT_TEXT", BIG_CLOB.get()),
                new DBParam("DIFF_AS_HTML", BIG_CLOB.get()),
                new DBParam("PROJECT_ID", 11),
                new DBParam("REPOSITORY_FORK_ID", 11),
                new DBParam("REPOSITORY_FORK_NAME", "dsf"),
                new DBParam("REPOSITORY_NAME", "dsf"),
                new DBParam("BRANCH_ID", "dsfdsg"),
                new DBParam("REPLY_TO_USER_ID", 42),
                new DBParam("PREVIOUS_BRANCH_ID", "dsfdsg"),
                new DBParam("COMMENT_ID", 11L),
                new DBParam("PULL_REQUEST_ID", 11L),
                new DBParam("NUM_OF_COMMITS", 11),
                new DBParam("COMMIT_ID", "dsfsdf"),
                new DBParam("FROM_HASH", "dsfsdf"),
                new DBParam("TO_HASH", "dsfsdf"),
                new DBParam("ACTIVITY_TYPE", "ACTIVE"),
                new DBParam("DATE", new Date()),
                new DBParam("ACTIVITY_REPO_ID", 42),
                new DBParam("ACTIVITY_STASH_USER_ID", 777)
        );
        System.out.println("Number of entities: " + entityManager.count(AoProjectActivity.class,
                Query.select("COMMENT_TEXT").distinct()
                        .where("COMMENT_TEXT is not null")
        ));
        AoProjectActivity[] activity = entityManager.find(AoProjectActivity.class);
        System.out.println("Saved: " + activity[0].getCommentText().length());
        System.out.println("extract: " + activity[0].getCommentText().substring(0, 100));

    }

    private static final Supplier<String> BIG_CLOB = Suppliers.memoize(new Supplier<String>() {
        @Override
        public String get() {
            final int size = 800100;
            final StringBuilder sb = new StringBuilder(size);
            for (int i = 0; i < size / 10; i++) {
                sb.append("фывапролдж");
            }
            return sb.append(size).toString();
        }
    });
}
