package net.java.ao;

import junit.framework.JUnit4TestAdapter;
import junit.framework.Test;
import net.java.ao.test.config.JdbcConfiguration;
import test.schema.Author;
import test.schema.Authorship;
import test.schema.Book;
import test.schema.Comment;
import test.schema.Company;
import test.schema.Distribution;
import test.schema.EmailAddress;
import test.schema.Magazine;
import test.schema.Message;
import test.schema.Nose;
import test.schema.OnlineDistribution;
import test.schema.Pen;
import test.schema.Person;
import test.schema.PersonLegalDefence;
import test.schema.PersonSuit;
import test.schema.Photo;
import test.schema.Post;
import test.schema.PostalAddress;
import test.schema.PrintDistribution;
import test.schema.PublicationToDistribution;
import test.schema.Select;
import test.schema.UserBase;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * Copyright 2007 Daniel Spiewak
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 *	    http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @author Daniel Spiewak
 */
public abstract class TestUtilities {
	private static int priorID = -1;	// ugly hack for postgresql and oracle

	private static String uri = JdbcConfiguration.get().getUrl();

	public static Test asTest(Class<?> clazz) {
		return new JUnit4TestAdapter(clazz);
	}

	public static DataStruct setUpEntityManager(EntityManager manager) throws SQLException {

        DataStruct back = new DataStruct();

        manager.migrate(PersonSuit.class, Pen.class, Comment.class, Photo.class, Post.class, Nose.class,
                Authorship.class, Book.class, Magazine.class,
                PublicationToDistribution.class, PrintDistribution.class, OnlineDistribution.class,
                Message.class, EmailAddress.class, PostalAddress.class, Select.class, UserBase.class);

        final String authorTableName = getTableName(manager, Author.class);
        final String authorshipTableName = getTableName(manager, Authorship.class);
        final String bookTableName = getTableName(manager, Book.class);
        final String commentTableName = getTableName(manager, Comment.class);
        final String companyTableName = getTableName(manager, Company.class);
        final String emailAddressTableName = getTableName(manager, EmailAddress.class);
        final String magazineTableName = getTableName(manager, Magazine.class);
        final String messageTableName = getTableName(manager, Message.class);
        final String noseTableName = getTableName(manager, Nose.class);
        final String penTableName = getTableName(manager, Pen.class);
        final String personTableName = getTableName(manager, Person.class);
        final String personLegalDefenceTableName = getTableName(manager, PersonLegalDefence.class);
        final String personSuitTableName = getTableName(manager, PersonSuit.class);
        final String photoTableName = getTableName(manager, Photo.class);
        final String postTableName = getTableName(manager, Post.class);
        final String publicationToDistributionTableName = getTableName(manager, PublicationToDistribution.class);

		//_________________________________________________________________________________________
		Connection conn = manager.getProvider().getConnection();
		try {
			PreparedStatement stmt = prepareStatement(conn, "INSERT INTO " + companyTableName
					+ " (" + processId(manager, "companyID") + ", " + processId(manager, "name") + ", "
					+ processId(manager, "cool") + ", " + processId(manager, "image") + ") VALUES (?,?,?,?)");

			stmt.setLong(1, back.companyID = System.currentTimeMillis());
			stmt.setString(2, "Company Name");
			stmt.setBoolean(3, false);

			InputStream imageStream = TestUtilities.class.getResourceAsStream("/icon.png");
			stmt.setBinaryStream(4, imageStream, imageStream.available());

			stmt.executeUpdate();

			imageStream.close();

			Thread.sleep(10);

			int index = 0;
			back.coolCompanyIDs = new long[3];

			stmt = prepareStatement(conn, "INSERT INTO " + companyTableName + " ("
					+ processId(manager, "companyID") + ", " + processId(manager, "name") + ", "
					+ processId(manager, "cool") +") VALUES (?,?,?)");

			stmt.setLong(1, back.coolCompanyIDs[index++] = System.currentTimeMillis());
			stmt.setString(2, "Cool Company");
			stmt.setBoolean(3, true);

			stmt.executeUpdate();

			Thread.sleep(10);

			stmt.setLong(1, back.coolCompanyIDs[index++] = System.currentTimeMillis());
			stmt.setString(2, "Cool Company");
			stmt.setBoolean(3, true);

			stmt.executeUpdate();

			Thread.sleep(10);

			stmt.setLong(1, back.coolCompanyIDs[index++] = System.currentTimeMillis());
			stmt.setString(2, "Cool Company");
			stmt.setBoolean(3, true);

			stmt.executeUpdate();

			stmt.close();

			assignPriorID(manager, conn, "person");

			stmt = prepareStatement(conn, "INSERT INTO " + personTableName
					+ " (" + processId(manager, "firstName") + ", " + processId(manager, "profession")
					+ ", " + processId(manager, "companyID") + ", " + processId(manager, "image") + ") VALUES (?, ?, ?, ?)");

			stmt.setString(1, "Daniel");
			stmt.setInt(2, 0);
			stmt.setLong(3, back.companyID);

			imageStream = TestUtilities.class.getResourceAsStream("/icon.png");
			stmt.setBinaryStream(4, imageStream, imageStream.available());

			stmt.executeUpdate();

			imageStream.close();

			back.personID = getPriorID(conn, stmt);
			stmt.close();

			assignPriorID(manager, conn, "nose");

			stmt = prepareStatement(conn, "INSERT INTO " + noseTableName
					+ " (" + processId(manager, "length") + ", " + processId(manager, "personID") + ") VALUES (?,?)");

			stmt.setInt(1, 2);
			stmt.setInt(2, back.personID);
			stmt.executeUpdate();

			back.noseID = getPriorID(conn, stmt);

			stmt.close();

			assignPriorID(manager, conn, "pen");

			back.penIDs = new int[3];
			stmt = prepareStatement(conn, "INSERT INTO " + penTableName
					+ " (" + processId(manager, "width") + ", " + processId(manager, "personID")
					+ ") VALUES (?,?)");

			index = 0;

			stmt.setDouble(1, 0.5);
			stmt.setInt(2, back.personID);
			stmt.executeUpdate();

			back.penIDs[index++] = getPriorID(conn, stmt);

			assignPriorID(manager, conn, "pen");

			stmt.setDouble(1, 0.7);
			stmt.setInt(2, back.personID);
			stmt.executeUpdate();

			back.penIDs[index++] = getPriorID(conn, stmt);

			assignPriorID(manager, conn, "pen");

			stmt.setDouble(1, 1);
			stmt.setInt(2, back.personID);
			stmt.executeUpdate();

			back.penIDs[index++] = getPriorID(conn, stmt);

			stmt.close();

			assignPriorID(manager, conn, "personDefence");

			back.defenceIDs = new int[3];
			stmt = prepareStatement(conn, "INSERT INTO " + personLegalDefenceTableName + " ("
					+ processId(manager, "severity") + ") VALUES (?)");

			index = 0;

			stmt.setInt(1, 5);
			stmt.executeUpdate();

			back.defenceIDs[index++] = getPriorID(conn, stmt);

			assignPriorID(manager, conn, "personDefence");

			stmt.setInt(1, 7);
			stmt.executeUpdate();

			back.defenceIDs[index++] = getPriorID(conn, stmt);

			assignPriorID(manager, conn, "personDefence");

			stmt.setInt(1, 1);
			stmt.executeUpdate();

			back.defenceIDs[index++] = getPriorID(conn, stmt);

			stmt.close();

			assignPriorID(manager, conn, "personSuit");

			back.suitIDs = new int[3];
			stmt = prepareStatement(conn, "INSERT INTO " + personSuitTableName
					+ " (" + processId(manager, "personID") + ", " + processId(manager, "personLegalDefenceID") + ") VALUES (?,?)");

			index = 0;

			stmt.setInt(1, back.personID);
			stmt.setInt(2, back.defenceIDs[0]);
			stmt.executeUpdate();

			back.suitIDs[index++] = getPriorID(conn, stmt);

			assignPriorID(manager, conn, "personSuit");

			stmt.setInt(1, back.personID);
			stmt.setInt(2, back.defenceIDs[1]);
			stmt.executeUpdate();

			back.suitIDs[index++] = getPriorID(conn, stmt);

			assignPriorID(manager, conn, "personSuit");

			stmt.setInt(1, back.personID);
			stmt.setInt(2, back.defenceIDs[2]);
			stmt.executeUpdate();

			back.suitIDs[index++] = getPriorID(conn, stmt);

			stmt.close();

			assignPriorID(manager, conn, "post");

			stmt = prepareStatement(conn, "INSERT INTO " + postTableName + " (" + processId(manager, "title") + ") VALUES (?)");

			stmt.setString(1, "Test Post");
			stmt.executeUpdate();

			back.postID = getPriorID(conn, stmt);

			stmt.close();

			assignPriorID(manager, conn, "photo");

			stmt = prepareStatement(conn, "INSERT INTO " + photoTableName + " (" + processId(manager, "depth") + ") VALUES (?)");

			stmt.setInt(1, 256);

			stmt.executeUpdate();

			back.photoID = getPriorID(conn, stmt);

			stmt.close();

			assignPriorID(manager, conn, "comment");

			stmt = prepareStatement(conn, "INSERT INTO " + commentTableName
					+ " (" + processId(manager, "title") + ", " + processId(manager, "text")
					+ ", " + processId(manager, "commentableID") + ", " + processId(manager, "commentableType")
					+ ") VALUES (?,?,?,?)");

			back.postCommentIDs = new int[3];
			back.photoCommentIDs = new int[2];

			int postCommentIndex = 0;
			int photoCommentIndex = 0;

			index = 1;
			stmt.setString(index++, "Test Post Comment 1");
			stmt.setString(index++, "Here's some test text");
			stmt.setInt(index++, back.postID);
			stmt.setString(index++, manager.getPolymorphicTypeMapper().convert(Post.class));
			stmt.executeUpdate();

			back.postCommentIDs[postCommentIndex++] = getPriorID(conn, stmt);

			assignPriorID(manager, conn, "comment");

			index = 1;
			stmt.setString(index++, "Test Post Comment 2");
			stmt.setString(index++, "Here's some test text");
			stmt.setInt(index++, back.postID);
			stmt.setString(index++, manager.getPolymorphicTypeMapper().convert(Post.class));
			stmt.executeUpdate();

			back.postCommentIDs[postCommentIndex++] = getPriorID(conn, stmt);

			assignPriorID(manager, conn, "comment");

			index = 1;
			stmt.setString(index++, "Test Photo Comment 1");
			stmt.setString(index++, "Here's some test text");
			stmt.setInt(index++, back.photoID);
			stmt.setString(index++, manager.getPolymorphicTypeMapper().convert(Photo.class));
			stmt.executeUpdate();

			back.photoCommentIDs[photoCommentIndex++] = getPriorID(conn, stmt);

			assignPriorID(manager, conn, "comment");

			index = 1;
			stmt.setString(index++, "Test Post Comment 3");
			stmt.setString(index++, "Here's some test text");
			stmt.setInt(index++, back.postID);
			stmt.setString(index++, manager.getPolymorphicTypeMapper().convert(Post.class));
			stmt.executeUpdate();

			back.postCommentIDs[postCommentIndex++] = getPriorID(conn, stmt);

			assignPriorID(manager, conn, "comment");

			index = 1;
			stmt.setString(index++, "Test Photo Comment 2");
			stmt.setString(index++, "Here's some test text");
			stmt.setInt(index++, back.photoID);
			stmt.setString(index++, manager.getPolymorphicTypeMapper().convert(Photo.class));
			stmt.executeUpdate();

			back.photoCommentIDs[photoCommentIndex++] = getPriorID(conn, stmt);

			stmt.close();

			back.bookIDs = new int[2];
			index = 0;

			assignPriorID(manager, conn, "book");

			stmt = prepareStatement(conn, "INSERT INTO " + bookTableName
					+ " (" + processId(manager, "title") + ", " + processId(manager, "hardcover") + ") VALUES (?,?)");

			stmt.setString(1, "Test Book 1");
			stmt.setBoolean(2, true);
			stmt.executeUpdate();

			back.bookIDs[index++] = getPriorID(conn, stmt);

			assignPriorID(manager, conn, "book");

			stmt.setString(1, "Test Book 2");
			stmt.setBoolean(2, true);
			stmt.executeUpdate();

			back.bookIDs[index++] = getPriorID(conn, stmt);

			stmt.close();

			back.magazineIDs = new int[2];
			index = 0;

			assignPriorID(manager, conn, "magazine");

			stmt = prepareStatement(conn, "INSERT INTO " + magazineTableName + " (" + processId(manager, "title") + ") VALUES (?)");

			stmt.setString(1, "Test Magazine 1");
			stmt.executeUpdate();

			back.magazineIDs[index++] = getPriorID(conn, stmt);

			assignPriorID(manager, conn, "magazine");

			stmt.setString(1, "Test Magazine 2");
			stmt.executeUpdate();

			back.magazineIDs[index++] = getPriorID(conn, stmt);

			stmt.close();

			back.bookAuthorIDs = new int[2][3];
			index = 0;

			for (int i = 0; i < back.bookIDs.length; i++) {
				for (int subIndex = 0; subIndex < back.bookAuthorIDs[0].length; subIndex++) {
					assignPriorID(manager, conn, "author");

					stmt = prepareStatement(conn, "INSERT INTO " + authorTableName
							+ " (" + processId(manager, "name") + ") VALUES (?)");

					stmt.setString(1, "Test Book Author " + (subIndex + 1));
					stmt.executeUpdate();

					back.bookAuthorIDs[i][subIndex] = getPriorID(conn, stmt);

					stmt.close();

					stmt = prepareStatement(conn, "INSERT INTO " + authorshipTableName
							+ " (" + processId(manager, "publicationID") + ", " + processId(manager, "publicationType")
							+ ", " + processId(manager, "authorID") + ") VALUES (?,?,?)");

					stmt.setInt(1, back.bookIDs[i]);
					stmt.setString(2, manager.getPolymorphicTypeMapper().convert(Book.class));
					stmt.setInt(3, back.bookAuthorIDs[i][subIndex]);
					stmt.executeUpdate();

					stmt.close();
				}
			}

			back.magazineAuthorIDs = new int[2][3];

			for (int i = 0; i < back.magazineIDs.length; i++) {
				for (int subIndex = 0; subIndex < back.magazineAuthorIDs[0].length; subIndex++) {
					assignPriorID(manager, conn, "author");

					stmt = prepareStatement(conn, "INSERT INTO " + authorTableName
							+ " (" + processId(manager, "name") + ") VALUES (?)");

					stmt.setString(1, "Test Magazine Author " + (subIndex + 1));
					stmt.executeUpdate();

					back.magazineAuthorIDs[i][subIndex] = getPriorID(conn, stmt);

					stmt.close();

					stmt = prepareStatement(conn, "INSERT INTO " + authorshipTableName
							+ " (" + processId(manager, "publicationID") + ", " + processId(manager, "publicationType")
							+ ", " + processId(manager, "authorID") + ") VALUES (?,?,?)");

					stmt.setInt(1, back.magazineIDs[i]);
					stmt.setString(2, manager.getPolymorphicTypeMapper().convert(Magazine.class));
					stmt.setInt(3, back.magazineAuthorIDs[i][subIndex]);
					stmt.executeUpdate();

					stmt.close();
				}
			}

			back.bookDistributionIDs = new int[2][5];
			back.bookDistributionTypes = new Class[2][5];

			for (int i = 0; i < back.bookIDs.length; i++) {
				for (int subIndex = 0; subIndex < back.bookDistributionIDs[0].length; subIndex++) {
					Class<? extends Distribution> distType = (subIndex % 2 == 0 ? PrintDistribution.class
							: OnlineDistribution.class);
					String distTableName = manager.getTableNameConverter().getName(distType);
					String params = null;

					if (distType == PrintDistribution.class) {
						params = " (" + processId(manager, "copies") + ") VALUES (?)";
					} else if (distType == OnlineDistribution.class) {
						params = " (" + processId(manager, "url") + ") VALUES (?)";
					}

					back.bookDistributionTypes[i][subIndex] = distType;

					assignPriorID(manager, conn, distTableName);

					stmt = prepareStatement(conn, "INSERT INTO " + processId(manager, distTableName) + ' ' + params);

					if (distType == PrintDistribution.class) {
						stmt.setInt(1, 20);
					} else if (distType == OnlineDistribution.class) {
						stmt.setString(1, "http://www.google.com");
					}
					stmt.executeUpdate();

					back.bookDistributionIDs[i][subIndex] = getPriorID(conn, stmt);

					stmt.close();

					stmt = prepareStatement(conn, "INSERT INTO " + publicationToDistributionTableName +
							" (" + processId(manager, "publicationID") + ", " + processId(manager, "publicationType")
							+ ", " + processId(manager, "distributionID") + ", " + processId(manager, "distributionType") + ") VALUES (?,?,?,?)");

					stmt.setInt(1, back.bookIDs[i]);
					stmt.setString(2, manager.getPolymorphicTypeMapper().convert(Book.class));
					stmt.setInt(3, back.bookDistributionIDs[i][subIndex]);
					stmt.setString(4, manager.getPolymorphicTypeMapper().convert(distType));
					stmt.executeUpdate();

					stmt.close();
				}
			}

			back.magazineDistributionIDs = new int[2][12];
			back.magazineDistributionTypes = new Class[2][12];

			for (int i = 0; i < back.magazineIDs.length; i++) {
				for (int subIndex = 0; subIndex < back.magazineDistributionIDs[0].length; subIndex++) {
					Class<? extends Distribution> distType = (subIndex % 2 == 0 ? PrintDistribution.class
							: OnlineDistribution.class);
					String distTableName = manager.getTableNameConverter().getName(distType);
					String params = null;

					if (distType == PrintDistribution.class) {
						params = " (" + processId(manager, "copies") + ") VALUES (?)";
					} else if (distType == OnlineDistribution.class) {
						params = " (" + processId(manager, "url") + ") VALUES (?)";
					}

					back.magazineDistributionTypes[i][subIndex] = distType;

					assignPriorID(manager, conn, distTableName);

					stmt = prepareStatement(conn, "INSERT INTO " + processId(manager, distTableName) + ' ' + params);

					if (distType == PrintDistribution.class) {
						stmt.setInt(1, 20);
					} else if (distType == OnlineDistribution.class) {
						stmt.setString(1, "http://www.google.com");
					}
					stmt.executeUpdate();

					back.magazineDistributionIDs[i][subIndex] = getPriorID(conn, stmt);

					stmt.close();

					stmt = prepareStatement(conn, "INSERT INTO " + publicationToDistributionTableName +
							" (" + processId(manager, "publicationID") + ", " + processId(manager, "publicationType")
							+ ", " + processId(manager, "distributionID") + ", " + processId(manager, "distributionType")
							+ ") VALUES (?,?,?,?)");

					stmt.setInt(1, back.magazineIDs[i]);
					stmt.setString(2, manager.getPolymorphicTypeMapper().convert(Magazine.class));
					stmt.setInt(3, back.magazineDistributionIDs[i][subIndex]);
					stmt.setString(4, manager.getPolymorphicTypeMapper().convert(distType));
					stmt.executeUpdate();

					stmt.close();

					back.addressIDs = new int[2];

					stmt = prepareStatement(conn, "INSERT INTO " + emailAddressTableName
							+ " (" + processId(manager, "email") + ") VALUES (?)");

					assignPriorID(manager, conn, "emailAddress");

					stmt.setString(1, "djspiewak@gmail.com");
					stmt.executeUpdate();

					back.addressIDs[0] = getPriorID(conn, stmt);

					assignPriorID(manager, conn, "emailAddress");

					stmt.setString(1, "daniel@danielspiewak.org");
					stmt.executeUpdate();

					back.addressIDs[1] = getPriorID(conn, stmt);

					stmt.close();

					back.messageIDs = new int[3];

					String contentsName = processId(manager, "contents");

					stmt = prepareStatement(conn, "INSERT INTO " + messageTableName
							+ " (" + contentsName
							+ ", " + processId(manager, "fromID") + ", " + processId(manager, "fromType")
							+ ", " + processId(manager, "toID") + ", " + processId(manager, "toType") + ")"
							+ " VALUES (?,?,?,?,?)");

					assignPriorID(manager, conn, "message");

					stmt.setString(1, "Hi there");
					stmt.setInt(2, back.addressIDs[0]);
					stmt.setString(3, manager.getPolymorphicTypeMapper().convert(EmailAddress.class));
					stmt.setInt(4, back.addressIDs[1]);
					stmt.setString(5, manager.getPolymorphicTypeMapper().convert(EmailAddress.class));

					stmt.executeUpdate();

					back.messageIDs[0] = getPriorID(conn, stmt);

					assignPriorID(manager, conn, "message");

					stmt.setString(1, "Yo dude");
					stmt.setInt(2, back.addressIDs[1]);
					stmt.setString(3, manager.getPolymorphicTypeMapper().convert(EmailAddress.class));
					stmt.setInt(4, back.addressIDs[0]);
					stmt.setString(5, manager.getPolymorphicTypeMapper().convert(EmailAddress.class));

					stmt.executeUpdate();

					back.messageIDs[1] = getPriorID(conn, stmt);

					assignPriorID(manager, conn, "message");

					stmt.setString(1, "Email is fun");
					stmt.setInt(2, back.addressIDs[0]);
					stmt.setString(3, manager.getPolymorphicTypeMapper().convert(EmailAddress.class));
					stmt.setInt(4, back.addressIDs[1]);
					stmt.setString(5, manager.getPolymorphicTypeMapper().convert(EmailAddress.class));

					stmt.executeUpdate();

					back.messageIDs[2] = getPriorID(conn, stmt);

					stmt.close();
				}
			}

			for (int i = 0; i < 3; i++) {	// add some extra, unrelated distributions
				Class<? extends Distribution> distType = (i % 2 == 0 ? PrintDistribution.class
						: OnlineDistribution.class);
				String distTableName = manager.getTableNameConverter().getName(distType);
				String params = null;

				if (distType == PrintDistribution.class) {
					params = " (" + processId(manager, "copies") + ") VALUES (?)";
				} else if (distType == OnlineDistribution.class) {
					params = " (" + processId(manager, "url") + ") VALUES (?)";
				}

				stmt = prepareStatement(conn, "INSERT INTO " + processId(manager, distTableName) + ' ' + params);

				if (distType == PrintDistribution.class) {
					stmt.setInt(1, 20);
				} else if (distType == OnlineDistribution.class) {
					stmt.setString(1, "http://www.dzone.com");
				}
				stmt.executeUpdate();

				stmt.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
		} catch (Throwable e) {
			e.printStackTrace();
		} finally {
			conn.close();
		}

		return back;
	}

    private static String getTableName(EntityManager manager, Class<? extends RawEntity<?>> entityType)
    {
        String commentableTableName = manager.getTableNameConverter().getName(entityType);
        commentableTableName = processId(manager, commentableTableName);
        return commentableTableName;
    }

    private static String processId(EntityManager manager, String id)
    {
        return manager.getProvider().processID(id);
    }

    private static final PreparedStatement prepareStatement(Connection conn, String sql) throws SQLException {
		if (uri.startsWith("jdbc:hsqldb") || uri.startsWith("jdbc:postgres")) {
			return conn.prepareStatement(sql);
		}

		return conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
	}

	private static final void assignPriorID(EntityManager manager, Connection conn, String table) throws SQLException {
		priorID = -1;

		if (uri.startsWith("jdbc:postgres")) {
			PreparedStatement stmt = conn.prepareStatement("SELECT NEXTVAL('\"" + table + "_id_seq\"')");

			ResultSet res = stmt.executeQuery();

			if (res.next()) {
				priorID = res.getInt(1) + 1;
			}

			res.close();
			stmt.close();
		} else if (uri.startsWith("jdbc:oracle")) {
			String sequence = processId(manager, table + "_id_seq");
			String sql = "SELECT " + sequence + ".NEXTVAL FROM dual";

			PreparedStatement stmt = conn.prepareStatement(sql);

			ResultSet res = stmt.executeQuery();

			if (res.next()) {
				priorID = res.getInt(1) + 1;
			}

			res.close();
			stmt.close();
		}
	}

	private static final int getPriorID(Connection conn, PreparedStatement stmt) throws SQLException {
		int back = -1;

		if (uri.startsWith("jdbc:hsqldb")) {
			PreparedStatement ident = conn.prepareStatement("CALL IDENTITY()");
			ResultSet res = ident.executeQuery();

			if (res.next()) {
				back = res.getInt(1);
			}

			res.close();
			ident.close();
		} else if (uri.startsWith("jdbc:postgres") || uri.startsWith("jdbc:oracle")) {
			back = priorID;
		} else {
			ResultSet res = stmt.getGeneratedKeys();
			if (res.next()) {
				back = res.getInt(1);
			}
		}

		return back;
	}

	public static final void tearDownEntityManager(EntityManager manager) throws SQLException {
		Connection conn = manager.getProvider().getConnection();
		try {
			Statement stmt = conn.createStatement();

			String suffix = "";

			if (uri.startsWith("jdbc:oracle")) {
				suffix = " PURGE";
			}

            final String authorTableName = getTableName(manager, Author.class);
            final String authorshipTableName = getTableName(manager, Authorship.class);
            final String bookTableName = getTableName(manager, Book.class);
            final String commentTableName = getTableName(manager, Comment.class);
            final String companyTableName = getTableName(manager, Company.class);
            final String emailAddressTableName = getTableName(manager, EmailAddress.class);
            final String magazineTableName = getTableName(manager, Magazine.class);
            final String messageTableName = getTableName(manager, Message.class);
            final String noseTableName = getTableName(manager, Nose.class);
            final String onlineDistributionTableName = getTableName(manager, OnlineDistribution.class);
            final String penTableName = getTableName(manager, Pen.class);
            final String personTableName = getTableName(manager, Person.class);
            final String personLegalDefenceTableName = getTableName(manager, PersonLegalDefence.class);
            final String personSuitTableName = getTableName(manager, PersonSuit.class);
            final String photoTableName = getTableName(manager, Photo.class);
            final String postTableName = getTableName(manager, Post.class);
            final String postalAddressTableName = getTableName(manager, PostalAddress.class);
            final String printDistributionTableName = getTableName(manager, PrintDistribution.class);
            final String publicationToDistributionTableName = getTableName(manager, PublicationToDistribution.class);
            final String selectTableName = getTableName(manager, Select.class);

			//_____________________________________________________________________________________
			stmt.executeUpdate("DELETE FROM " + penTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + personSuitTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + personLegalDefenceTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + noseTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + personTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + companyTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + commentTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + postTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + photoTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + authorshipTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + authorTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + bookTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + magazineTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + publicationToDistributionTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + printDistributionTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + onlineDistributionTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + messageTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + emailAddressTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + postalAddressTableName + suffix);
			stmt.executeUpdate("DELETE FROM " + selectTableName + suffix);

			stmt.close();
		} finally {
			conn.close();
		}
	}
}
