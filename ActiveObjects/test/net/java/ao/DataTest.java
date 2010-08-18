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
import net.java.ao.test.config.JdbcConfiguration;
import net.java.ao.test.config.Parameters;
import net.java.ao.test.config.ParametersLoader;
import net.java.ao.types.ClassType;
import net.java.ao.types.TypeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import test.schema.*;

import java.sql.SQLException;

import static net.java.ao.TestUtilities.setUpEntityManager;

/**
 * @author Daniel Spiewak
 */
public abstract class DataTest {
	private static DataStruct preparedData = null;

	protected EntityManager manager;
    protected Sql sql;

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

    @Before
	public final void setUp() throws SQLException
    {
        manager = getEntityManager();
        sql = new Sql(manager.getEventManager());
        
        if (preparedData == null)
        {
            preparedData =  setUpEntityManager(manager);
        }
        applyStruct(this, preparedData);
    }

    @After
    public final void tearDown() throws Exception
    {
        if (!JdbcConfiguration.get().getUrl().startsWith("jdbc:hsql"))
        {
            manager.getProvider().dispose();
        }
    }

    private EntityManager getEntityManager()
    {
        final JdbcConfiguration conf = JdbcConfiguration.get();
        final Parameters parameters = ParametersLoader.get();

        final EntityManager manager = EntityManagerBuilder.url(conf.getUrl()).username(conf.getUsername()).password(conf.getPassword()).auto()
                .tableNameConverter(parameters.getTableNameConverter())
                .fieldNameConverter(parameters.getFieldNameConverter())
                .build();
        manager.setPolymorphicTypeMapper(new DefaultPolymorphicTypeMapper(Photo.class, Post.class, Book.class,
                Magazine.class, PrintDistribution.class, OnlineDistribution.class, EmailAddress.class, PostalAddress.class));
        return manager;
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

	@BeforeClass
	public static void classSetup() throws SQLException {
		TypeManager.getInstance().addType(new ClassType());
	}
}
