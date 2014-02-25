package net.java.ao.it;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import net.java.ao.RawEntity;
import net.java.ao.it.model.Author;
import net.java.ao.it.model.Book;
import net.java.ao.it.model.Chair;
import net.java.ao.it.model.Comment;
import net.java.ao.it.model.Commentable;
import net.java.ao.it.model.Company;
import net.java.ao.it.model.Distribution;
import net.java.ao.it.model.EmailAddress;
import net.java.ao.it.model.Magazine;
import net.java.ao.it.model.Message;
import net.java.ao.it.model.MotivationGenerator;
import net.java.ao.it.model.Nose;
import net.java.ao.it.model.Pen;
import net.java.ao.it.model.Person;
import net.java.ao.it.model.PersonImpl;
import net.java.ao.it.model.PersonLegalDefence;
import net.java.ao.it.model.Photo;
import net.java.ao.it.model.Post;
import net.java.ao.it.model.Profession;
import net.java.ao.it.model.Select;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.DbUtils;
import net.java.ao.test.jdbc.Data;

import static net.java.ao.it.DatabaseProcessor.AddressData;
import static net.java.ao.it.DatabaseProcessor.BookData;
import static net.java.ao.it.DatabaseProcessor.CompanyData;
import static net.java.ao.it.DatabaseProcessor.MagazineData;
import static net.java.ao.it.DatabaseProcessor.MessageData;
import static net.java.ao.it.DatabaseProcessor.PenData;
import static net.java.ao.it.DatabaseProcessor.PersonData;
import static net.java.ao.it.DatabaseProcessor.PersonLegalDefenceData;
import static net.java.ao.it.DatabaseProcessor.PhotoCommentData;
import static net.java.ao.it.DatabaseProcessor.PhotoData;
import static net.java.ao.it.DatabaseProcessor.PostCommentData;
import static net.java.ao.it.DatabaseProcessor.PostData;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
@Data(DatabaseProcessor.class)
public final class EntityIntegrationTest extends ActiveObjectsIntegrationTest
{
    @Test
    public void testGetEntityManagerOnEntity() throws Exception
    {
        assertEquals(entityManager, getPerson().getEntityManager());
    }

    @Test
    public void testToStringWithRegularNamedEntity() throws Exception
    {
        assertEquals(getTableName(Person.class, false) + " {" + getFieldName(Person.class, "getID") + " = " + PersonData.getId() + "}", getPerson().toString());
    }

    @Test
    public void testToStringWithEntityNamedWithKeyword() throws Exception
    {
        int selectId = getSelectId();
        assertEquals(getTableName(Select.class, false) + " {" + getFieldName(Select.class, "getID") + " = " + selectId + "}", entityManager.get(Select.class, selectId).toString());
    }

    @Test
    public void testEntityAccessors() throws Exception
    {
        checkPersonData(getPerson());
    }

    @Test
    public void testValueStore() throws Exception
    {
        final Person person = getPerson();

        // first time sql IS executed
        checkSqlExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                checkPersonData(person);
                return null;
            }
        });

        // second time they should be retrieved from the value store
        checkSqlNotExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                checkPersonData(person);
                return null;
            }
        });
    }

    @Test
    public void testUncachableFieldIsNotInValueStore() throws Exception
    {
        final Company company = getCompany();
        for (int i = 0; i < 2; i++)
        {
            checkSqlExecutedWhenAccessingImageField(company);
        }
    }

    private void checkSqlExecutedWhenAccessingImageField(final Company company) throws Exception
    {
        checkSqlExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                IOUtils.closeQuietly(company.getImage());
                return null;
            }
        });
    }

    @Test
    public void testByteArrayBlobAccessor() throws Exception
    {
        assertArrayEquals(PersonData.IMAGE, getPerson().getImage());
    }

    @Test
    public void testInputStreamBlobAccessor() throws Exception
    {
        final InputStream companyImg = getCompany().getImage();
        try
        {
            assertArrayEquals(CompanyData.IMAGES[0], IOUtils.toByteArray(companyImg));
        }
        finally
        {
            IOUtils.closeQuietly(companyImg);
        }
    }

    @Test
    public void testPolymorphicAccessor() throws Exception
    {
        final Comment postComment = entityManager.get(Comment.class, PostCommentData.getIds()[0]);
        final Commentable post = postComment.getCommentable();

        assertTrue(post instanceof Post);
        assertEquals(PostData.getId(), post.getID());

        final Comment photoComment = entityManager.get(Comment.class, PhotoCommentData.getIds()[0]);
        final Commentable photo = photoComment.getCommentable();

        assertTrue(photo instanceof Photo);
        assertEquals(PhotoData.getId(), post.getID());

        assertNull(entityManager.create(Comment.class).getCommentable());
    }

    @Test
    public void testFieldAccessorAreInValueStoreBeforeBeingPersisted() throws Exception
    {
        final Company company = entityManager.create(Company.class);

        checkCompanyFields(company, "Another company name", true);
        checkCompanyFields(company, "Yet Another company name", false);
        checkCompanyFields(company, null, true);
    }

    private void checkCompanyFields(final Company company, final String companyName, final boolean companyIsCool) throws Exception
    {
        checkSqlNotExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                company.setName(companyName);
                company.setCool(companyIsCool);
                return null;
            }
        });
        assertEquals(companyName, company.getName());
        assertEquals(companyIsCool, company.isCool());
    }

    @Test
    public void testSave() throws Exception
    {
        final Company company = entityManager.create(Company.class);

        checkCompanyIsSavedToDatabase(company, "Another company name", true);
        checkCompanyIsSavedToDatabase(company, "Yet Another company name", false);
        checkCompanyIsSavedToDatabase(company, null, true);


        final Person person = getPerson();

        checkPersonIsSavedToDatabase(person, Profession.MUSICIAN);
    }

    private void checkCompanyIsSavedToDatabase(final Company company, final String companyName, final boolean companyIsCool) throws Exception
    {
        company.setName(companyName);
        company.setCool(companyIsCool);

        checkSqlExecutedWhenSaving(company);

        executeStatement("SELECT " + escapeFieldName(Company.class, "getName") + ", " + escapeFieldName(Company.class, "isCool")
                + " FROM " + getTableName(Company.class) + " WHERE " + escapeFieldName(Company.class, "getCompanyID") + " = ?",
                new DbUtils.StatementCallback()
                {
                    public void setParameters(PreparedStatement statement) throws Exception
                    {
                        statement.setLong(1, company.getCompanyID());
                    }

                    public void processResult(ResultSet resultSet) throws Exception
                    {
                        if (resultSet.next())
                        {
                            assertEquals(companyName, resultSet.getString(getFieldName(Company.class, "getName")));
                            assertEquals(companyIsCool, resultSet.getBoolean(getFieldName(Company.class, "isCool")));
                        }
                        else
                        {
                            fail("No company found in database with company ID " + company.getCompanyID());
                        }
                    }
                });
    }

    private void checkPersonIsSavedToDatabase(final Person person, final Profession musician) throws Exception
    {
        person.setProfession(musician);

        checkSqlExecutedWhenSaving(person);

        executeStatement("SELECT " + escapeFieldName(Person.class, "getProfession") + " FROM " + getTableName(Person.class) + " WHERE " + escapeFieldName(Person.class, "getID") + " = ?",
                new DbUtils.StatementCallback()
                {
                    public void setParameters(PreparedStatement statement) throws Exception
                    {
                        statement.setInt(1, person.getID());
                    }

                    public void processResult(ResultSet resultSet) throws Exception
                    {
                        if (resultSet.next())
                        {
                            assertEquals(musician.name(), resultSet.getString(getFieldName(Person.class, "getProfession")));
                        }
                        else
                        {
                            fail("No person found in database with ID " + person.getID());
                        }
                    }
                });
    }

    @Test
    public void testTransientFieldsNotInValueStore() throws Exception
    {
        final int newAge = 25;

        final Person person = getPerson();
        person.setAge(newAge);
        checkSqlExecutedWhenSaving(person);

        checkSqlExecutedWhenGettingAge(newAge, person);
        checkSqlExecutedWhenGettingAge(newAge, person);
    }

    private void checkSqlExecutedWhenGettingAge(final int newAge, final Person person) throws Exception
    {
        checkSqlExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                assertEquals(newAge, person.getAge());
                return null;
            }
        });
    }

    @Test
    public void testByteArrayField() throws Exception
    {
        final byte[] newImage = "some random new image".getBytes("utf-8");
        final Person person = getPerson();

        person.setImage(newImage);
        checkSqlExecutedWhenSaving(person);

        entityManager.flushAll(); // just to be sure

        assertArrayEquals(newImage, person.getImage());
    }

    @Test
    public void testInputStreamField() throws Exception
    {
        final byte[] newImage = "some random new image".getBytes("utf-8");
        final Company company = getCompany();

        final InputStream is = new ByteArrayInputStream(newImage);
        try
        {
            company.setImage(is);
            checkSqlExecutedWhenSaving(company);

            entityManager.flushAll(); // just to be sure

            assertArrayEquals(newImage, IOUtils.toByteArray(company.getImage()));
        }
        finally
        {
            IOUtils.closeQuietly(is);
        }
    }

    @Test
    public void testPolymorphicField() throws Exception
    {
        checkCommentPolymorphicFields(createPost(), Post.class);
        checkCommentPolymorphicFields(createPhoto(), Photo.class);
        checkCommentPolymorphicFields(null, null); // if standalone, then there is no discriminant
    }

    private void checkCommentPolymorphicFields(final Commentable commentable, final Class<? extends RawEntity<?>> discriminant) throws Exception
    {
        final Comment comment = entityManager.create(Comment.class);
        comment.setTitle("My Temp Test Comment");
        comment.setText("Here's some test text");
        comment.setCommentable(commentable);
        comment.save();

        executeStatement("SELECT " + escapeFieldName(Comment.class, "getCommentable") + ", " + escapePolyFieldName(Comment.class, "getCommentable")
                + " FROM " + getTableName(Comment.class) + " WHERE " + escapeFieldName(Comment.class, "getID") + " = ?",
                new DbUtils.StatementCallback()
                {
                    public void setParameters(PreparedStatement statement) throws Exception
                    {
                        statement.setInt(1, comment.getID());
                    }

                    public void processResult(ResultSet resultSet) throws Exception
                    {
                        if (resultSet.next())
                        {
                            if (commentable != null)
                            {
                                assertEquals(commentable.getID(), resultSet.getInt(1));
                                assertEquals(entityManager.getPolymorphicTypeMapper().convert(discriminant), resultSet.getString(2));
                            }
                            else
                            {
                                assertNull(resultSet.getString(1));
                                assertNull(resultSet.getString(2));
                            }
                        }
                        else
                        {
                            fail("No comment found in database with ID " + comment.getID());
                        }
                    }
                });
    }

    @Test
    public void testPropertyChangeListenerIsNotFiredOnSet() throws Exception
    {
        final AtomicReference<String> propertyName = new AtomicReference<String>();
        final AtomicReference<String> propertyValue = new AtomicReference<String>();

        final Person person = getPersonWithPropertyChangeListener(propertyName, propertyValue);

        person.setFirstName("Daniel");
        assertNull(propertyName.get());
        assertNull(propertyValue.get());
    }

    @Test
    public void testPropertyChangeListenerIsFiredOnSave() throws Exception
    {
        final AtomicReference<String> propertyName = new AtomicReference<String>();
        final AtomicReference<String> propertyValue = new AtomicReference<String>();

        final Person person = getPersonWithPropertyChangeListener(propertyName, propertyValue);

        person.setFirstName("Daniel");
        person.save();

        assertEquals(getFieldName(Person.class, "getFirstName"), propertyName.get());
        assertEquals("Daniel", propertyValue.get());
    }

    private Person getPersonWithPropertyChangeListener(AtomicReference<String> propertyName, AtomicReference<String> propertyValue) throws SQLException
    {
        final Person person = getPerson();
        person.addPropertyChangeListener(getPropertyChangeListener(propertyName, propertyValue));
        return person;
    }

    private PropertyChangeListener getPropertyChangeListener(final AtomicReference<String> propertyName, final AtomicReference<String> propertyValue)
    {
        return new PropertyChangeListener()
        {
            public void propertyChange(PropertyChangeEvent evt)
            {
                propertyName.set(evt.getPropertyName());
                propertyValue.set((String) evt.getNewValue());
            }
        };
    }

    @Test
    public void testAccessNullPrimitive() throws Exception
    {
        final Company company = entityManager.create(Company.class);
        try
        {
            assertFalse(company.isCool());
        }
        catch (NullPointerException e)
        {
            fail("We shouldn't get an NPE here!");
        }
    }

    @Test
    public void testSetFieldsToNull() throws SQLException
    {
        final Company company = entityManager.create(Company.class);
        company.setName(null);
        company.save();

        entityManager.flush(company);

        assertNull(company.getName());

        company.setAddressInfo(null);
        company.save();

        entityManager.flush(company);
        assertNull(company.getAddressInfo());
    }

    @Test
    public void testReservedOperations() throws Exception
    {
        final String where = "Some test criteria";
        final boolean and = false;

        final Select select = entityManager.create(Select.class);
        select.setWhere(where);
        select.setAnd(and);
        select.save();

        entityManager.flushAll();

        assertEquals(where, select.getWhere());
        assertEquals(and, select.isAnd());
    }

    @Test
    public void testGenerator() throws Exception
    {
        final Company company = entityManager.create(Company.class);
        executeStatement("SELECT " + escapeFieldName(Company.class, "getMotivation") + " FROM " + getTableName(Company.class)
                + " WHERE " + escapeFieldName(Company.class, "getCompanyID") + " = ?",
                new DbUtils.StatementCallback()
                {
                    public void setParameters(PreparedStatement statement) throws Exception
                    {
                        statement.setLong(1, company.getCompanyID());
                    }

                    public void processResult(ResultSet resultSet) throws Exception
                    {
                        if (resultSet.next())
                        {
                            assertEquals(MotivationGenerator.MOTIVATION, resultSet.getString("motivation"));
                        }
                        else
                        {
                            fail("Unable to find INSERTed company row");
                        }
                    }
                });
    }

    @Test
    public void testDelete() throws Exception
    {
        final Company company = entityManager.create(Company.class);

        checkSqlExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                entityManager.delete(company);
                return null;
            }
        });

        executeStatement("SELECT " + escapeFieldName(Company.class, "getCompanyID") + " FROM " + getTableName(Company.class)
                + " WHERE " + escapeFieldName(Company.class, "getCompanyID") + " = ?",
                new DbUtils.StatementCallback()
                {
                    public void setParameters(PreparedStatement statement) throws Exception
                    {
                        statement.setLong(1, company.getCompanyID());
                    }

                    public void processResult(ResultSet resultSet) throws Exception
                    {
                        assertFalse("Row was not deleted", resultSet.next());
                    }
                });
    }

    @Test
    public void testDefinedImplementationIsUsed() throws Exception
    {
        final Person person = getPerson();
        checkSqlNotExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                PersonImpl.enableOverride = true;
                try
                {
                    assertEquals(PersonImpl.LAST_NAME, person.getLastName());
                }
                finally
                {
                    PersonImpl.enableOverride = false;
                }
                return null;
            }
        });
    }

    @Test
    public void testNotNullSoftCheck() throws Exception
    {
        final Message message = entityManager.get(Message.class, MessageData.getIds()[0]);
        try
        {
            message.setContents(null);
            fail("Should have thrown IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    @Test
    public void testOneToOneRetrievalId() throws Exception
    {
        final Company company = getCompany();
        final Person person = getPerson();

        assertEquals(company.getCompanyID(), person.getCompany().getCompanyID());
    }

    @Test
    public void testOneToOneRetrievalPreload() throws Exception
    {
        final Person person = getPerson();
        final Nose nose = person.getNose();
        checkSqlNotExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                nose.getLength();
                return null;
            }
        });
    }

    @Test
    public void testOneToManyRetrievalIds() throws Exception
    {
        final Person person = getPerson();
        final Pen[] pens = person.getPens();

        assertEquals(PenData.getIds().length, pens.length);

        for (Pen pen : pens)
        {
            boolean found = false;
            for (int id : PenData.getIds())
            {
                if (pen.getID() == id)
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                fail("Unable to find id " + pen.getID());
            }
        }
    }

    @Test
    public void testOneToManyRetrievalPreload() throws Exception
    {
        entityManager.flushAll();

        final Person person = getPerson();
        for (final Pen pen : person.getPens())
        {
            checkSqlNotExecuted(new Callable<Void>()
            {
                public Void call() throws Exception
                {
                    pen.getWidth();
                    return null;
                }
            });
        }
    }

    @Test
    public void testManyToManyRetrievalIds() throws Exception
    {
        final Person person = getPerson();
        final PersonLegalDefence[] defences = person.getPersonLegalDefences();

        assertEquals(PersonLegalDefenceData.getIds().length, defences.length);

        for (PersonLegalDefence defence : defences)
        {
            boolean found = false;
            for (int id : PersonLegalDefenceData.getIds())
            {
                if (defence.getID() == id)
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                fail("Unable to find id=" + defence.getID());
            }
        }
    }

    @Test
    public void testManyToManyRetrievalPreload() throws Exception
    {
        entityManager.flushAll();

        final Person person = getPerson();

        for (final PersonLegalDefence defence : person.getPersonLegalDefences())
        {
            checkSqlNotExecuted(new Callable<Void>()
            {
                public Void call() throws Exception
                {
                    defence.getSeverity();
                    return null;
                }
            });
        }

        for (final Chair chair : person.getChairs())
        {
            checkSqlNotExecuted(new Callable<Void>()
            {
                public Void call() throws Exception
                {
                    chair.getColour();
                    return null;
                }
            });
        }
    }

    @Test
    public void testPolymorphicOneToManyRetrievalIDs() throws Exception
    {
        final Post post = getPost();
        final Comment[] postComments = post.getComments();

        assertEquals(PostCommentData.getIds().length, postComments.length);

        for (Comment comment : postComments)
        {
            boolean found = false;
            for (int id : PostCommentData.getIds())
            {
                if (comment.getID() == id)
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                fail("Unable to find id=" + comment.getID());
            }
        }

        final Photo photo = getPhoto();
        final Comment[] photoComments = photo.getComments();

        assertEquals(PhotoCommentData.getIds().length, photoComments.length);

        for (Comment comment : photoComments)
        {
            boolean found = false;
            for (int id : PhotoCommentData.getIds())
            {
                if (comment.getID() == id)
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                fail("Unable to find id=" + comment.getID());
            }
        }
    }

    @Test
    public void testPolymorphicOneToManyRetrievalPreload() throws Exception
    {
        entityManager.flushAll();
        for (final Comment comment : getPost().getComments())
        {
            checkSqlNotExecuted(new Callable<Void>()
            {
                public Void call() throws Exception
                {
                    comment.getTitle();
                    return null;
                }
            });
        }
    }

    @Test
    public void testPolymorphicManyToManyRetrievalIDs() throws Exception
    {
        for (int i = 0; i < BookData.getIds().length; i++)
        {
            final Book book = entityManager.get(Book.class, BookData.getIds()[i]);
            Author[] authors = book.getAuthors();

            assertEquals(BookData.AUTHOR_IDS[i].length, authors.length);

            for (Author author : authors)
            {
                boolean found = false;
                for (int id : BookData.AUTHOR_IDS[i])
                {
                    if (author.getID() == id)
                    {
                        found = true;
                        break;
                    }
                }

                if (!found)
                {
                    fail("Unable to find id=" + author.getID());
                }
            }
        }

        for (int i = 0; i < MagazineData.getIds().length; i++)
        {
            final Magazine magazine = entityManager.get(Magazine.class, MagazineData.getIds()[i]);
            final Author[] authors = magazine.getAuthors();

            assertEquals(MagazineData.AUTHOR_IDS[i].length, authors.length);

            for (Author author : authors)
            {
                boolean found = false;
                for (int id : MagazineData.AUTHOR_IDS[i])
                {
                    if (author.getID() == id)
                    {
                        found = true;
                        break;
                    }
                }

                if (!found)
                {
                    fail("Unable to find id=" + author.getID());
                }
            }
        }

        for (int i = 0; i < BookData.getIds().length; i++)
        {
            final Book book = entityManager.get(Book.class, BookData.getIds()[i]);
            final Distribution[] distributions = book.getDistributions();

            assertEquals(BookData.DISTRIBUTION_IDS[i].length, distributions.length);

            for (Distribution distribution : distributions)
            {
                boolean found = false;
                for (int j = 0; j < BookData.DISTRIBUTION_IDS[i].length; j++)
                {
                    if (distribution.getID() == BookData.DISTRIBUTION_IDS[i][j] && distribution.getEntityType().equals(BookData.DISTRIBUTION_TYPES[i][j]))
                    {
                        found = true;
                        break;
                    }
                }

                if (!found)
                {
                    fail("Unable to find id=" + distribution.getID()
                            + ", type=" + entityManager.getPolymorphicTypeMapper().convert(distribution.getEntityType()));
                }
            }
        }

        for (int i = 0; i < MagazineData.getIds().length; i++)
        {
            final Magazine magazine = entityManager.get(Magazine.class, MagazineData.getIds()[i]);
            final Distribution[] distributions = magazine.getDistributions();

            assertEquals(MagazineData.DISTRIBUTION_IDS[i].length, distributions.length);

            for (Distribution distribution : distributions)
            {
                boolean found = false;
                for (int j = 0; j < MagazineData.DISTRIBUTION_IDS[i].length; j++)
                {
                    if (distribution.getID() == MagazineData.DISTRIBUTION_IDS[i][j] && distribution.getEntityType().equals(MagazineData.DISTRIBUTION_TYPES[i][j]))
                    {
                        found = true;
                        break;
                    }
                }

                if (!found)
                {
                    fail("Unable to find id=" + distribution.getID()
                            + ", type=" + entityManager.getPolymorphicTypeMapper().convert(distribution.getEntityType()));
                }
            }
        }
    }

    @Test
    public void testPolymorphicManyToManyRetrievalPreload() throws Exception
    {
        entityManager.flushAll();

        final Book book = entityManager.get(Book.class, BookData.getIds()[0]);

        for (final Author author : book.getAuthors())
        {
            checkSqlNotExecuted(new Callable<Void>()
            {
                public Void call() throws Exception
                {
                    author.getName();
                    return null;
                }
            });
        }
    }

    @Test
    public void testMultiPathPolymorphicOneToManyRetrievalIDs() throws Exception
    {
        final EmailAddress address = entityManager.get(EmailAddress.class, AddressData.getIds()[0]);
        final Message[] messages = address.getMessages();

        assertEquals(MessageData.getIds().length, messages.length);

        for (Message message : messages)
        {
            boolean found = false;
            for (int id : MagazineData.getIds())
            {
                if (message.getID() == id)
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                fail("Unable to find id=" + message.getID());
            }
        }
    }

    // ensure that ManyToOne child entities are retained i.e. not wrapped or recreated in a new entity proxy
    @Test
    public void testManyToOneNotReProxied() throws Exception
    {
        final Person person = getPerson();
        final Company company1 = person.getCompany();
        final Company company2 = person.getCompany();

        // explicitly use double equals here so that we test that the objects returned are the same
        assertTrue("ManyToOne relationship returned re-proxied entity", company1 == company2);

        // ensure that we aren't repopulating the same object
        checkSqlNotExecuted(new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                person.getCompany();
                return null;
            }
        });
    }

    // ensure that OneToOne child entities are retained i.e. not wrapped or recreated in a new entity proxy
    @Test
    public void testOneToOneNotReProxied() throws Exception
    {
        final Person person = getPerson();
        final Nose nose1 = person.getNose();
        final Nose nose2 = person.getNose();

        // explicitly use double equals here so that we test that the objects returned are the same
        assertTrue("OneToOne relationship returned re-proxied entity", nose1 == nose2);

        // ensure that we aren't repopulating the same object
        checkSqlNotExecuted(new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                person.getNose();
                return null;
            }
        });
    }

    // ensure that OneToMany child entities are retained i.e. not wrapped or recreated in a new entity proxy
    @Test
    public void testOneToManyNotReProxied() throws Exception
    {
        final Person person = getPerson();
        final Pen[] pens1 = person.getPens();
        final Pen[] pens2 = person.getPens();

        // explicitly use double equals here so that we test that the arrays are the same
        assertTrue("OneToMany relationship returned re-proxied entity", pens1 == pens2);

        // ensure that we aren't repopulating the same objects
        checkSqlNotExecuted(new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                person.getPens();
                return null;
            }
        });
    }

    // ensure that ManyToMany child entities are retained i.e. not wrapped or recreated in a new entity proxy
    @Test
    public void testManyToManyNotReProxied() throws Exception
    {
        final Person person = getPerson();
        final Chair[] chairs1 = person.getChairs();
        final Chair[] chairs2 = person.getChairs();

        // explicitly use double equals here so that we test that the arrays are the same
        assertTrue("ManyToMany relationship returned re-proxied entity", chairs1 == chairs2);

        // ensure that we aren't repopulating the same objects
        checkSqlNotExecuted(new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                person.getChairs();
                return null;
            }
        });
    }

    private Person getPerson() throws SQLException
    {
        return entityManager.get(Person.class, PersonData.getId());
    }

    private Company getCompany() throws SQLException
    {
        return entityManager.get(Company.class, CompanyData.getIds()[0]);
    }

    private Post getPost() throws SQLException
    {
        return entityManager.get(Post.class, PostData.getId());
    }

    private Photo getPhoto() throws SQLException
    {
        return entityManager.get(Photo.class, PhotoData.getId());
    }

    private void checkPersonData(Person person)
    {
        assertNotNull(person);
        assertEquals(PersonData.FIRST_NAME, person.getFirstName());
        assertEquals(PersonData.LAST_NAME, person.getLastName());
        assertEquals(PersonData.PROFESSION, person.getProfession());

        assertEquals(CompanyData.getIds()[0], person.getCompany().getCompanyID());
        assertEquals(CompanyData.NAMES[0], person.getCompany().getName());
        assertEquals(false, person.getCompany().isCool());
    }

    private int getSelectId() throws Exception
    {
        final Select select = entityManager.create(Select.class);
        select.save();
        return select.getID();
    }

    private Post createPost() throws Exception
    {
        final Post post = entityManager.create(Post.class);
        post.setTitle("My Temp Test Title");
        post.save();
        return post;
    }

    private Photo createPhoto() throws Exception
    {
        Photo photo = entityManager.create(Photo.class);
        photo.save();
        return photo;
    }
}
