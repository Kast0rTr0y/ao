package net.java.ao.it;

import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.ManyToMany;
import net.java.ao.Preload;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import org.junit.Test;

import java.sql.SQLException;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;

/**
 * Test for ACTIVEOBJECTS-65, where using {@link ManyToMany} relationships with {@link Preload} can lead to
 * {@link SQLException} saying "Column not found '*'"
 */
@Data(TestManyToManyWithPreload.ManyToManyWithPreloadDatabaseUpdater.class)
public final class TestManyToManyWithPreload extends ActiveObjectsIntegrationTest {
    @Test
    public void getManyElementsWithPreload() throws Exception {
        final User[] users = entityManager.find(User.class);
        assertEquals(1, users.length);

        final Post[] posts = users[0].getPosts();
        assertEquals(2, posts.length);
        checkSqlNotExecuted(new Callable<Void>() {
            public Void call() throws Exception {
                posts[0].getTitle();
                posts[1].getTitle();
                return null;
            }
        });
    }

    static interface User extends Entity {
        @ManyToMany(UserPost.class)
        Post[] getPosts();
    }

    static interface UserPost extends Entity {
        Post getPost();

        void setPost(Post post);

        User getUser();

        void setUser(User user);
    }

    @Preload
    static interface Post extends Entity {
        String getTitle();

        void setTitle(String title);

        @ManyToMany(UserPost.class)
        User[] getUsers();
    }

    public static final class ManyToManyWithPreloadDatabaseUpdater implements DatabaseUpdater {
        public void update(EntityManager entityManager) throws Exception {
            entityManager.migrate(User.class, Post.class, UserPost.class);

            final User user = entityManager.create(User.class);
            user.save();

            addPost(entityManager, "post1", user);
            addPost(entityManager, "post2", user);
        }

        private void addPost(EntityManager entityManager, String title, User user) throws SQLException {
            final Post p1 = entityManager.create(Post.class);
            p1.setTitle(title);
            p1.save();

            final UserPost up1 = entityManager.create(UserPost.class);
            up1.setUser(user);
            up1.setPost(p1);
            up1.save();
        }
    }
}
