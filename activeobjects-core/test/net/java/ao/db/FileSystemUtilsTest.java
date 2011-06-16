package net.java.ao.db;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public final class FileSystemUtilsTest
{
    @Test
    public void checkIsCaseSensitiveDoesNotCreateMultipleTempFiles()
    {
        final int initialNumberOfTempFiles = getNumberOfTempFiles();

        FileSystemUtils.isCaseSensitive();

        assertEquals(initialNumberOfTempFiles + 1, getNumberOfTempFiles());

        FileSystemUtils.isCaseSensitive();

        assertEquals(initialNumberOfTempFiles + 1, getNumberOfTempFiles()); // no temp file added the second time
    }

    private int getNumberOfTempFiles()
    {
        return getTempDir().list().length;
    }

    private File getTempDir()
    {
        return new File(System.getProperty("java.io.tmpdir"));
    }
}
