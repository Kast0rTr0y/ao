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
package net.java.ao;

import net.java.ao.builder.EntityManagerBuilder;
import net.java.ao.schema.CamelCaseFieldNameConverter;
import net.java.ao.schema.CamelCaseTableNameConverter;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.PluralizedNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.UnderscoreTableNameConverter;
import net.java.ao.test.Configuration;
import net.java.ao.types.ClassType;
import net.java.ao.types.TypeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import test.schema.Book;
import test.schema.Distribution;
import test.schema.EmailAddress;
import test.schema.Magazine;
import test.schema.OnlineDistribution;
import test.schema.Photo;
import test.schema.Post;
import test.schema.PostalAddress;
import test.schema.PrintDistribution;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Daniel Spiewak
 */
@RunWith(Parameterized.class)
public abstract class DataTest {
	private static final Map<String, DataStruct> prepared = new HashMap<String, DataStruct>();

	protected final EntityManager manager;

    /**
     * A helper object to test whether any SQL calls have been issued or not
     */
    protected final Sql sql;

	protected int personID;
	protected int noseID;
	protected long companyID;

	protected int[] penIDs;
	protected int[] defenceIDs;
	protected int[] suitIDs;

	protected long[] coolCompanyIDs;

	protected int postID;
	protected int photoID;

	protected int[] postCommentIDs;
	protected int[] photoCommentIDs;

	protected int[] bookIDs;
	protected int[] magazineIDs;

	protected int[][] bookAuthorIDs;
	protected int[][] magazineAuthorIDs;

	protected int[][] bookDistributionIDs;
	protected Class<? extends Distribution>[][] bookDistributionTypes;

	protected int[][] magazineDistributionIDs;
	protected Class<? extends Distribution>[][] magazineDistributionTypes;

	protected int[] addressIDs;
	protected int[] messageIDs;

	public DataTest(int ordinal, TableNameConverter tableConverter, FieldNameConverter fieldConverter) throws SQLException {
		String uri = Configuration.get().getUriPrefix();
		String suffix = Configuration.get().getUriSuffix();
		String user = Configuration.get().getUserName();
		String pass = Configuration.get().getPassword();

        manager = EntityManagerBuilder.url(uri + '_' + ordinal + suffix).username(user).password(pass).build();
        sql = new Sql(manager.getEventManager());

		manager.setTableNameConverter(tableConverter);
		manager.setFieldNameConverter(fieldConverter);

		manager.setPolymorphicTypeMapper(new DefaultPolymorphicTypeMapper(Photo.class,
				Post.class, Book.class, Magazine.class, PrintDistribution.class, OnlineDistribution.class,
				EmailAddress.class, PostalAddress.class));
	}

	@Before
	public void setup() throws SQLException {
		prepareData(this);
	}

	@After
	public void tearDown() {
		manager.getProvider().dispose();
	}

	private static void prepareData(DataTest test) throws SQLException {
		EntityManager manager = test.manager;

		if (prepared.containsKey(manager.getProvider().getURI())) {
			applyStruct(test, prepared.get(manager.getProvider().getURI()));
			return;
		}

		try {
			TestUtilities.tearDownEntityManager(manager);
		} catch (Throwable t) {}

		DataStruct data = TestUtilities.setUpEntityManager(manager);
		applyStruct(test, data);
		prepared.put(manager.getProvider().getURI(), data);
	}

	private static void applyStruct(DataTest test, DataStruct data) {
		test.personID = data.personID;
		test.noseID = data.noseID;
		test.companyID = data.companyID;
		test.penIDs = data.penIDs;
		test.defenceIDs = data.defenceIDs;
		test.suitIDs = data.suitIDs;
		test.coolCompanyIDs = data.coolCompanyIDs;
		test.postID = data.postID;
		test.photoID = data.photoID;
		test.postCommentIDs = data.postCommentIDs;
		test.photoCommentIDs = data.photoCommentIDs;
		test.bookIDs = data.bookIDs;
		test.magazineIDs = data.magazineIDs;
		test.bookAuthorIDs = data.bookAuthorIDs;
		test.magazineAuthorIDs = data.magazineAuthorIDs;
		test.bookDistributionIDs = data.bookDistributionIDs;
		test.bookDistributionTypes = data.bookDistributionTypes;
		test.magazineDistributionIDs = data.magazineDistributionIDs;
		test.magazineDistributionTypes = data.magazineDistributionTypes;
		test.addressIDs = data.addressIDs;
		test.messageIDs = data.messageIDs;
	}

	@Parameters
	public static Collection<Object[]> data() {
		CamelCaseTableNameConverter camelCaseTableNameConverter = new CamelCaseTableNameConverter();
		UnderscoreTableNameConverter underscoreTableNameConverter = new UnderscoreTableNameConverter(false);
		UnderscoreTableNameConverter underscoreTableNameConverter2 = new UnderscoreTableNameConverter(true);

		PluralizedNameConverter pluralizedCamelNameConverter = new PluralizedNameConverter(camelCaseTableNameConverter);
		PluralizedNameConverter pluralizedUnderscore2NameConverter = new PluralizedNameConverter(underscoreTableNameConverter2);

		CamelCaseFieldNameConverter camelCaseFieldNameConverter = new CamelCaseFieldNameConverter();
//		UnderscoreFieldNameConverter underscoreFieldNameConverter = new UnderscoreFieldNameConverter(false);
//		UnderscoreFieldNameConverter underscoreFieldNameConverter2 = new UnderscoreFieldNameConverter(true);

		// try all combinations, just for fun
		return Arrays.asList(new Object[][] {
			{0, camelCaseTableNameConverter, camelCaseFieldNameConverter},
//			{camelCaseTableNameConverter, underscoreFieldNameConverter},
//			{camelCaseTableNameConverter, underscoreFieldNameConverter2},

			{1, underscoreTableNameConverter, camelCaseFieldNameConverter},
//			{underscoreTableNameConverter, underscoreFieldNameConverter},
//			{underscoreTableNameConverter, underscoreFieldNameConverter2},

			{2, pluralizedCamelNameConverter, camelCaseFieldNameConverter},
//			{pluralizedCamelNameConverter, underscoreFieldNameConverter},
//			{pluralizedCamelNameConverter, underscoreFieldNameConverter2},

			{3, pluralizedUnderscore2NameConverter, camelCaseFieldNameConverter}
//			{pluralizedUnderscore2NameConverter, underscoreFieldNameConverter},
//			{pluralizedUnderscore2NameConverter, underscoreFieldNameConverter2}
		});
	}

	@BeforeClass
	public static void classSetup() throws SQLException {
		TypeManager.getInstance().addType(new ClassType());
	}
}
