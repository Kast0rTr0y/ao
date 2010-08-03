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
import net.java.ao.schema.*;
import net.java.ao.test.Configuration;
import net.java.ao.types.ClassType;
import net.java.ao.types.TypeManager;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import test.schema.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static net.java.ao.TestUtilities.setUpEntityManager;

/**
 * @author Daniel Spiewak
 */
@RunWith(Parameterized.class)
public abstract class DataTest {
	private static final Map<String, DataStruct> PREPARED_DATA = new HashMap<String, DataStruct>();

    protected final String connectionUrl;
	protected final EntityManager manager;
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

        connectionUrl = getConnectionUrl(ordinal);
        manager = getEntityManager(connectionUrl, tableConverter, fieldConverter);
        sql = new Sql(manager.getEventManager());

        manager.setPolymorphicTypeMapper(new DefaultPolymorphicTypeMapper(Photo.class, Post.class, Book.class,
                Magazine.class, PrintDistribution.class, OnlineDistribution.class, EmailAddress.class, PostalAddress.class));

        // prepare the test data and cache it
        if (!PREPARED_DATA.containsKey(connectionUrl))
        {
            PREPARED_DATA.put(connectionUrl, setUpEntityManager(manager));
        }
    }

    private String getConnectionUrl(int ordinal)
    {
        final Configuration conf = Configuration.get();
        return new StringBuilder()
                .append(conf.getUriPrefix())
                .append('_')
                .append(ordinal)
                .append(conf.getUriSuffix())
                .toString();
    }

    private EntityManager getEntityManager(String url, TableNameConverter tableConverter, FieldNameConverter fieldConverter)
    {
        final Configuration conf = Configuration.get();
        return EntityManagerBuilder.url(url).username(conf.getUserName()).password(conf.getPassword()).auto()
                .tableNameConverter(tableConverter)
                .fieldNameConverter(fieldConverter)
                .build();
    }

    @Before
	public void setUp() throws SQLException
    {
        if (PREPARED_DATA.containsKey(connectionUrl))
        {
            applyStruct(this, PREPARED_DATA.get(connectionUrl));
        }
        else
        {
            throw new RuntimeException("Could not find any prepared data for this test");
        }
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
