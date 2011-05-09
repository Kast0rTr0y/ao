package net.java.ao.it;

import net.java.ao.DBParam;
import net.java.ao.EntityManager;
import net.java.ao.it.model.Address;
import net.java.ao.it.model.Author;
import net.java.ao.it.model.Authorship;
import net.java.ao.it.model.Book;
import net.java.ao.it.model.PostalAddress;
import net.java.ao.it.model.UserBase;
import net.java.ao.test.jdbc.DatabaseUpdater;
import net.java.ao.types.ClassType;
import net.java.ao.it.model.Comment;
import net.java.ao.it.model.Commentable;
import net.java.ao.it.model.Company;
import net.java.ao.it.model.Distribution;
import net.java.ao.it.model.EmailAddress;
import net.java.ao.it.model.Magazine;
import net.java.ao.it.model.Message;
import net.java.ao.it.model.Nose;
import net.java.ao.it.model.OnlineDistribution;
import net.java.ao.it.model.Pen;
import net.java.ao.it.model.Person;
import net.java.ao.it.model.PersonLegalDefence;
import net.java.ao.it.model.PersonSuit;
import net.java.ao.it.model.Photo;
import net.java.ao.it.model.Post;
import net.java.ao.it.model.PrintDistribution;
import net.java.ao.it.model.Profession;
import net.java.ao.it.model.Publication;
import net.java.ao.it.model.PublicationToDistribution;
import net.java.ao.it.model.Select;
import net.java.ao.types.TypeManager;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;

/**
 *
 */
public final class DatabaseProcessor implements DatabaseUpdater
{
    static
    {
        TypeManager.getInstance().addType(new ClassType());
    }

    public void update(EntityManager entityManager) throws Exception
    {
        // creating the schema
        entityManager.migrate(PersonSuit.class, Pen.class, Comment.class, Photo.class, Post.class, Nose.class, Authorship.class,
                Book.class, Magazine.class, PublicationToDistribution.class, PrintDistribution.class, OnlineDistribution.class,
                Message.class, EmailAddress.class, PostalAddress.class, Select.class, UserBase.class);

        addData(entityManager);

        entityManager.flushAll(); // clear all caches
    }

    private void addData(EntityManager entityManager) throws Exception
    {
        final Company[] companies = addCompanies(entityManager);
        final Person person = addPerson(entityManager, companies[0]);
        addPens(entityManager, person);
        addPersonLegalDefences(entityManager, person);

        final Post post = addPost(entityManager);
        addPostComments(entityManager, post);

        final Photo photo = addPhoto(entityManager);
        addPhotoComments(entityManager, photo);

        final Address[] addresses = addAddresses(entityManager);
        addMessages(entityManager, addresses[0], addresses[1]);

        addBooks(entityManager);
        addMagazines(entityManager);
    }

    private Company[] addCompanies(EntityManager entityManager) throws Exception
    {
        final Company[] companies = new Company[CompanyData.ids.length];
        for (int i = 0; i < companies.length; i++)
        {
            // the image
            final InputStream is = new ByteArrayInputStream(CompanyData.IMAGES[i]);

            final Company company = entityManager.create(Company.class);
            company.setName(CompanyData.NAMES[i]);
            company.setCool(CompanyData.COOLS[i]);
            company.setImage(is);
            company.save();

            is.close(); // for good measure, no need to worry to much about finally etc. here

            // set the company data
            CompanyData.ids[i] = company.getCompanyID();
            companies[i] = company;
        }
        return companies;
    }

    public Person addPerson(EntityManager entityManager, Company company) throws Exception
    {
        final Person person = entityManager.create(Person.class);
        person.setFirstName(PersonData.FIRST_NAME);
        person.setLastName(PersonData.LAST_NAME);
        person.setCompany(company);
        person.setProfession(PersonData.PROFESSION);
        person.setImage(PersonData.IMAGE);
        person.save();

        // save the id
        PersonData.id = person.getID();

        return person;
    }

    private Pen[] addPens(EntityManager entityManager, Person person) throws Exception
    {
        final Pen[] pens = new Pen[PenData.ids.length];
        for (int i = 0; i < PenData.ids.length; i++)
        {
            final Pen pen = entityManager.create(Pen.class);
            pen.setPerson(person);
            pen.setWidth(PenData.WIDTHS[i]);
            pen.save();

            PenData.ids[i] = pen.getID();
        }
        return pens;
    }

    private PersonLegalDefence[] addPersonLegalDefences(EntityManager entityManager, Person person) throws Exception
    {
        final PersonLegalDefence[] personLegalDefences = new PersonLegalDefence[PersonLegalDefenceData.ids.length];
        for (int i = 0; i < PersonLegalDefenceData.ids.length; i++)
        {
            final PersonLegalDefence personLegalDefence = entityManager.create(PersonLegalDefence.class);
            personLegalDefence.setSeverity(PersonLegalDefenceData.SEVERITIES[i]);
            personLegalDefence.save();

            PersonLegalDefenceData.ids[i] = personLegalDefence.getID();

            final PersonSuit personSuit = entityManager.create(PersonSuit.class);
            personSuit.setPerson(person);
            personSuit.setPersonLegalDefence(personLegalDefence);
            personSuit.save();

            PersonSuitData.ids[i] = personSuit.getID();
        }
        return personLegalDefences;
    }

    private Post addPost(EntityManager entityManager) throws Exception
    {
        final Post post = entityManager.create(Post.class);
        post.setTitle(PostData.TITLE);
        post.save();

        // save the id
        PostData.id = post.getID();

        return post;
    }

    private Comment[] addPostComments(EntityManager entityManager, Post post) throws Exception
    {
        final Comment[] comments = new Comment[PostCommentData.ids.length];
        for (int i = 0; i < PostCommentData.ids.length; i++)
        {
            final Comment comment = createComment(entityManager, post, PostCommentData.TITLES[i], PostCommentData.TEXTS[i]);
            PostCommentData.ids[i] = comment.getID();
            comments[i] = comment;
        }
        return comments;
    }

    private Photo addPhoto(EntityManager entityManager) throws Exception
    {
        final Photo photo = entityManager.create(Photo.class);
        photo.save();

        PhotoData.id = photo.getID(); // save the id
        return photo;
    }

    private Comment[] addPhotoComments(EntityManager entityManager, Photo photo) throws Exception
    {
        final Comment[] comments = new Comment[PhotoCommentData.ids.length];
        for (int i = 0; i < PhotoCommentData.ids.length; i++)
        {
            final Comment comment = createComment(entityManager, photo, PhotoCommentData.TITLES[i], PhotoCommentData.TEXTS[i]);
            PhotoCommentData.ids[i] = comment.getID();
            comments[i] = comment;
        }
        return comments;
    }

    private Address[] addAddresses(EntityManager entityManager) throws Exception
    {
        final Address[] addresses = new Address[AddressData.getIds().length];
        for (int i = 0; i < addresses.length; i++)
        {
            final EmailAddress address = entityManager.create(EmailAddress.class);
            address.setEmail(AddressData.EMAILS[i]);
            address.save();

            AddressData.ids[i] = address.getID();
            addresses[i] = address;
        }
        return addresses;
    }

    private void addMessages(EntityManager entityManager, Address address1, Address address2) throws Exception
    {
        final Message message1 = addMessage(entityManager, address1, address2, MessageData.CONTENTS[0]);
        MessageData.ids[0] = message1.getID();

        final Message message2 = addMessage(entityManager, address2, address1, MessageData.CONTENTS[1]);
        MessageData.ids[1] = message2.getID();
    }

    private Message addMessage(EntityManager entityManager, Address to, Address from, final String content) throws Exception
    {
        final Message message = entityManager.create(Message.class, new DBParam("contents", content));
        message.setTo(to);
        message.setFrom(from);
        message.save();
        return message;
    }

    private Book[] addBooks(EntityManager entityManager) throws Exception
    {
        final Book[] books = new Book[BookData.ids.length];
        for (int i = 0; i < BookData.ids.length; i++)
        {
            final Book book = entityManager.create(Book.class);
            book.setTitle(BookData.TITLES[i]);
            book.setHardcover(BookData.COVERS[i]);
            book.save();

            BookData.ids[i] = book.getID();

            for (int j = 0; j < BookData.AUTHOR_IDS[i].length; j++)
            {
                final Author author = addAuthor(entityManager, book, "Book author", j);
                BookData.AUTHOR_IDS[i][j] = author.getID();
            }

            final Distribution print = addPrintDistribution(entityManager, book, 10003);
            final Distribution online = addOnlineDistribution(entityManager, book, "http://amazon.example.com");

            BookData.DISTRIBUTION_IDS[i][0] = print.getID();
            BookData.DISTRIBUTION_TYPES[i][0] = print.getEntityType();

            BookData.DISTRIBUTION_IDS[i][1] = online.getID();
            BookData.DISTRIBUTION_TYPES[i][1] = online.getEntityType();

            books[i] = book;
        }
        return books;
    }

    private Magazine[] addMagazines(EntityManager entityManager) throws Exception
    {
        final Magazine[] magazines = new Magazine[MagazineData.ids.length];
        for (int i = 0; i < MagazineData.ids.length; i++)
        {
            final Magazine magazine = entityManager.create(Magazine.class);
            magazine.setTitle(MagazineData.TITLES[i]);
            magazine.save();

            MagazineData.ids[i] = magazine.getID();

            for (int j = 0; j < MagazineData.AUTHOR_IDS[i].length; j++)
            {
                final Author author = addAuthor(entityManager, magazine, "Magazine author", j);
                MagazineData.AUTHOR_IDS[i][j] = author.getID();
            }

            final Distribution print = addPrintDistribution(entityManager, magazine, 9007);
            final Distribution online = addOnlineDistribution(entityManager, magazine, "http://amazon.example.com");

            MagazineData.DISTRIBUTION_IDS[i][0] = print.getID();
            MagazineData.DISTRIBUTION_TYPES[i][0] = print.getEntityType();

            MagazineData.DISTRIBUTION_IDS[i][1] = online.getID();
            MagazineData.DISTRIBUTION_TYPES[i][1] = online.getEntityType();

            magazines[i] = magazine;
        }
        return magazines;
    }

    private Distribution addPrintDistribution(EntityManager entityManager, Publication publication, int copies) throws Exception
    {
        PrintDistribution distribution = entityManager.create(PrintDistribution.class);
        distribution.setCopies(copies);
        distribution.save();

        PublicationToDistribution ptd = entityManager.create(PublicationToDistribution.class);
        ptd.setDistribution(distribution);
        ptd.setPublication(publication);
        ptd.save();

        return distribution;
    }

    private Distribution addOnlineDistribution(EntityManager entityManager, Publication publication, String url) throws Exception
    {
        OnlineDistribution distribution = entityManager.create(OnlineDistribution.class);
        distribution.setURL(new URL(url));
        distribution.save();

        PublicationToDistribution ptd = entityManager.create(PublicationToDistribution.class);
        ptd.setDistribution(distribution);
        ptd.setPublication(publication);
        ptd.save();

        return distribution;
    }

    private Author addAuthor(EntityManager entityManager, Publication publication, String title, int index) throws Exception
    {
        final Author author = entityManager.create(Author.class);
        author.setName(title + " " + publication.getID() + ":" + index);
        author.save();

        final Authorship authorship = entityManager.create(Authorship.class);
        authorship.setPublication(publication);
        authorship.setAuthor(author);
        authorship.save();

        return author;
    }

    private Comment createComment(EntityManager entityManager, Commentable commentable, String title, String text) throws Exception
    {
        Comment comment = entityManager.create(Comment.class);
        comment.setTitle(title);
        comment.setText(text);
        comment.setCommentable(commentable);
        comment.save();
        return comment;
    }

    public static final class CompanyData
    {
        static long[] ids = {-1L, -1L};
        public static final String[] NAMES = {"My Company 0", "My Company 1"};
        public static final boolean[] COOLS = {false, true};
        public static final byte[][] IMAGES = {getImage("company image 0"), getImage("company image 1")};

        public static long[] getIds()
        {
            return ids;
        }
    }

    public static final class PersonData
    {
        static int id = -1;
        public static final String FIRST_NAME = "Daniel";
        public static final String LAST_NAME = "Spiewak";
        public static final Profession PROFESSION = Profession.DEVELOPER;
        public static final byte[] IMAGE = getImage("person image");

        public static int getId()
        {
            return id;
        }
    }

    public static final class AddressData
    {
        static int[] ids = {-1, -1};
        public static final String[] EMAILS = {"email1@example.com", "email2@example.com"};

        public static int[] getIds()
        {
            return ids;
        }
    }

    public static final class PenData
    {
        static int[] ids = {-1, -1, -1};
        public static final double[] WIDTHS = {0.3, 0.5, 0.7};

        public static int[] getIds()
        {
            return ids;
        }
    }

    public static final class PersonLegalDefenceData
    {
        static int[] ids = {-1, -1, -1};
        public static final int[] SEVERITIES = {2, 5, 7};

        public static int[] getIds()
        {
            return ids;
        }
    }

    public static final class PostData
    {
        static int id = -1;
        public static final String TITLE = "Test Post";

        public static int getId()
        {
            return id;
        }
    }

    public static final class PostCommentData
    {
        static int[] ids = {-1, -1, -1, -1};
        public static final String[] TITLES = {
                "Post comment title 0",
                "Post comment title 1",
                "Post comment title 2",
                "Post comment title 3"};
        public static final String[] TEXTS = {
                "Commenting on a post 0",
                "Commenting on a post 1",
                "Commenting on a post 2",
                "Commenting on a post 3"};

        public static int[] getIds()
        {
            return ids;
        }
    }

    public static final class PhotoData
    {
        static int id = -1;

        public static int getId()
        {
            return id;
        }
    }

    public static final class PhotoCommentData
    {
        static int[] ids = {-1, -1};
        public static final String[] TITLES = {"Photo comment title 0", "Photo comment title 1"};
        public static final String[] TEXTS = {"Commenting on a photo 0", "Commenting on a photo 1"};

        public static int[] getIds()
        {
            return ids;
        }
    }

    public static final class MessageData
    {
        static int[] ids = {-1, -1};
        public static final String[] CONTENTS = {"Some message content 0", "Some message content 1"};

        public static int[] getIds()
        {
            return ids;
        }
    }

    public static final class BookData
    {
        static int[] ids = {-1, -1};
        public static int[][] AUTHOR_IDS = {{-1, -1}, {-1, -1, -1}};
        public static int[][] DISTRIBUTION_IDS = {{-1, -1}, {-1, -1}};
        public static Class[][] DISTRIBUTION_TYPES = new Class[2][2];
        public static final String[] TITLES = {"Book title 0", "Book title 1"};
        public static final boolean[] COVERS = {true, false};

        public static int[] getIds()
        {
            return ids;
        }
    }

    public static final class MagazineData
    {
        static int[] ids = {-1, -1};
        public static final int[][] AUTHOR_IDS = {{-1, -1, -1}, {-1, -1}};
        public static int[][] DISTRIBUTION_IDS = {{-1, -1}, {-1, -1}};
        public static Class[][] DISTRIBUTION_TYPES = new Class[2][2];

        public static final String[] TITLES = {"Magazine title 0", "Magazine title 1"};

        public static int[] getIds()
        {
            return ids;
        }
    }

    public static final class AuthorData
    {
        static int[] ids = {-1, -1, -1, -1};

        public static int[] getIds()
        {
            return ids;
        }
    }

    public static final class PersonSuitData
    {
        static int[] ids = new int[PersonLegalDefenceData.ids.length];

        public static int[] getIds()
        {
            return ids;
        }
    }

    private static byte[] getImage(String fakeImage)
    {
        final byte[] img;
        try
        {
            img = fakeImage.getBytes("UTF-8");
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
        return img;
    }
}
