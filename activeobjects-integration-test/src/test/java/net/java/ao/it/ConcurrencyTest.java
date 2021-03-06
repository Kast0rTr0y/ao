package net.java.ao.it;

import net.java.ao.DBParam;
import net.java.ao.it.model.Company;
import net.java.ao.it.model.Person;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.NonTransactional;
import org.junit.Test;

import java.net.URL;

import static org.junit.Assert.assertFalse;

/**
 *
 */
@Data(DatabaseProcessor.class)
public final class ConcurrencyTest extends ActiveObjectsIntegrationTest {
    private static final int NUMBER_OF_THREADS = 50;

    @Test
    @NonTransactional
    public void testConcurrency() throws Throwable {
        final Thread[] threads = new Thread[NUMBER_OF_THREADS];
        final Throwable[] exceptions = new Throwable[threads.length];

        final Person person = createPerson();
        final Company company = createCompany();
        try {
            for (int i = 0; i < threads.length; i++) {
                final int threadNum = i;
                threads[i] = new Thread("Concurrency Test " + i) {
                    @Override
                    public void run() {
                        // a fair-few interleaved instructions
                        try {
                            person.setAge(threadNum);
                            person.save();

                            company.setName(getName());
                            company.save();

                            company.getName();
                            assertFalse(company.isCool());

                            person.setFirstName(getName());
                            person.setLastName("Spiewak");
                            person.setCompany(company);
                            person.save();

                            person.getNose();
                            person.getFirstName();
                            person.getAge();

                            person.setFirstName("Daniel");
                            person.save();

                            company.getImage();
                        } catch (Throwable t) {
                            exceptions[threadNum] = t;
                        }
                    }
                };
            }

            for (Thread thread : threads) {
                thread.start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            int exceptionCount = 0;
            for (Throwable e : exceptions) {
                if (e != null) {
                    exceptionCount++;
                }
            }
            for (Throwable e : exceptions) {
                if (e != null) {
                    throw new RuntimeException(exceptionCount + " threads failed. First failure: ", e);
                }
            }
        } finally {
            entityManager.delete(person);
            entityManager.delete(company);
        }
    }

    private Person createPerson() throws Exception {
        return entityManager.create(Person.class, new DBParam(getFieldName(Person.class, "getURL"), new URL("http://www.howtogeek.com")));
    }

    private Company createCompany() throws Exception {
        return entityManager.create(Company.class);
    }
}
