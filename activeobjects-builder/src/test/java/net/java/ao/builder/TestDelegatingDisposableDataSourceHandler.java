package net.java.ao.builder;

import static org.mockito.Mockito.*;

import net.java.ao.Disposable;
import net.java.ao.DisposableDataSource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.sql.DataSource;

@RunWith(MockitoJUnitRunner.class)
public class TestDelegatingDisposableDataSourceHandler
{
    private @Mock DataSource datasource;
    private @Mock Disposable disposable;

    @Test
    public void testInvokeDispose() throws Exception
    {
        DisposableDataSource disposableDatasource = DelegatingDisposableDataSourceHandler.newInstance(datasource, disposable);

        disposableDatasource.dispose();

        verify(disposable).dispose();
        verifyNoMoreInteractions(datasource);
    }

    @Test
    public void testInvokeDatasourceMethod() throws Exception
    {
        DisposableDataSource disposableDatasource = DelegatingDisposableDataSourceHandler.newInstance(datasource, disposable);
        disposableDatasource.getConnection();

        verify(datasource).getConnection();
    }
}
