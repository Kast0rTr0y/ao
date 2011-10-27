package net.java.ao.benchmark;

import net.java.ao.benchmark.model.Person;
import net.java.ao.benchmark.model.PersonWithPreload;
import net.java.ao.benchmark.util.Report;
import net.java.ao.benchmark.util.ReportPrinter;
import net.java.ao.benchmark.util.StopWatch;
import net.java.ao.benchmark.util.WikiReportPrinter;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.converters.NameConverters;
import net.java.ao.test.converters.UpperCaseFieldNameConverter;
import net.java.ao.test.converters.UpperCaseTableNameConverter;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.NonTransactional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.*;

@Data(BenchmarkDatabaseUpdater.class)
@NameConverters(table = UpperCaseTableNameConverter.class, field = UpperCaseFieldNameConverter.class)
public final class Benchmark extends ActiveObjectsIntegrationTest
{
    private static final int NUMBER_OF_PERSONS = 10 * 1000;

    private final StopWatch<Integer> insertStopWatch = new StopWatch<Integer>("Inserts");
    private final StopWatch<Integer> updateStopWatch = new StopWatch<Integer>("Updates");
    private final StopWatch<Integer> findStopWatch = new StopWatch<Integer>("Find");
    private final StopWatch<String> findLoopStopWatch = new StopWatch<String>("Find loop");
    private final StopWatch<Integer> deleteStopWatch = new StopWatch<Integer>("Delete");

    @Before
    public void warmUp()
    {
        checkNotNull(entityManager);
        entityManager.flushAll();
    }

    @Test
    @NonTransactional
    public void runWithoutPreload() throws Exception
    {
        run(Person.class);
    }

    @Test
    @NonTransactional
    public void runWithPreload() throws Exception
    {
        run(PersonWithPreload.class);
    }

    private void run(Class<? extends Person> personClass) throws Exception
    {
        System.out.printf("*** %s : %s ***\n", personClass.getName(), NUMBER_OF_PERSONS);

        for (int i = 0; i < 2; i++) // make sure things are correctly warmed up
        {
            entityManager.flushAll();
            final List<Integer> pks = insertPersons(personClass);
            updatePersons(personClass, pks);
            final Person[] all = findPersons(personClass);
            deletePersons(all);
        }
    }

    private int warmUpInsertPersons(Class<? extends Person> personClass) throws SQLException
    {
        final int warmUpNumber = 10;
        for (int i = 0; i < warmUpNumber; i++)
        {
            insertPerson(personClass, i);
        }
        entityManager.flushAll();
        return warmUpNumber;
    }

    private List<Integer> insertPersons(Class<? extends Person> personClass) throws SQLException
    {
        final int warmUpNumber = warmUpInsertPersons(personClass);
        final int lastInsertIndex = warmUpNumber + NUMBER_OF_PERSONS;
        final List<Integer> personsKey = new ArrayList<Integer>(NUMBER_OF_PERSONS);

        insertStopWatch.start();
        for (int i = warmUpNumber; i < lastInsertIndex; i++)
        {
            personsKey.add(insertPerson(personClass, i));
        }
        insertStopWatch.stop();
        return personsKey;
    }

    private void updatePersons(Class<? extends Person> personClass, List<Integer> pks) throws SQLException
    {
        updateStopWatch.start();
        for (Integer i : pks)
        {
            updatePerson(personClass, i);
        }
        updateStopWatch.stop();
    }

    private <P extends Person> P[] findPersons(Class<P> personClass) throws Exception
    {
        entityManager.flushAll();

        findStopWatch.start();
        final P[] all = entityManager.find(personClass);
        findStopWatch.stop();

        findLoopStopWatch.start();
        for (Person person : all)
        {
            findLoopStopWatch.lap(new StringBuilder()
                    .append(person.getID())
                    .append("-")
                    .append(person.getFirstName())
                    .append("-")
                    .append(person.getLastName()).toString());
        }
        findLoopStopWatch.stop();
        return all;
    }

    private <P extends Person> void deletePersons(P[] persons) throws Exception
    {
        deleteStopWatch.start();
        for (P person : persons)
        {
            entityManager.delete(person);
            deleteStopWatch.lap(person.getID());
        }
        deleteStopWatch.stop();
    }


    private <P extends Person> int insertPerson(Class<P> personClass, int i) throws SQLException
    {
        final Person p = entityManager.create(personClass);
        p.setFirstName("firstName " + i);
        p.setLastName("lastName" + i);
        p.save();
        insertStopWatch.lap(i);
        return p.getID();
    }

    private void updatePerson(Class<? extends Person> personClass, int i) throws SQLException
    {
        final Person p = entityManager.get(personClass, i);
        {
            p.setFirstName(p.getFirstName() + "#updated");
            p.setLastName(p.getLastName() + "#updated");
        }
        updateStopWatch.lap(i);
    }

    @After
    public void results()
    {
        ReportPrinter printer = newReportPrinter();

        printer.print(newReport(insertStopWatch));
        printer.print(newReport(updateStopWatch));
        printer.print(newReport(findStopWatch));
        printer.print(newReport(findLoopStopWatch));
        printer.print(newReport(deleteStopWatch));

        System.out.println();
    }

    private static ReportPrinter newReportPrinter()
    {
//        return new PrettyPrintReportPrinter();
        return new WikiReportPrinter();
    }

    private static Report newReport(StopWatch<?> stopWatch)
    {
        return stopWatch.getReport();
    }
}
