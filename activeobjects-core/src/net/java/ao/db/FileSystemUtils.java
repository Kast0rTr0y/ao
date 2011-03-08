package net.java.ao.db;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.Writer;
import java.util.Locale;

import static com.google.common.base.Suppliers.memoize;

final class FileSystemUtils
{
    static boolean isCaseSensitive()
    {
        return memoize(new Supplier<Boolean>()
        {
            public Boolean get()
            {
                try
                {
                    final File tmp = File.createTempFile("active_objects", "tmp");
                    final File tmpWithUpperCaseName = new File(tmp.getParent(), toUpperCase(tmp.getName()));
                    if (!tmpWithUpperCaseName.exists())
                    {
                        return false;
                    }
                    return !sameContent(tmp, tmpWithUpperCaseName);
                }
                catch (IOException e)
                {
                    throw new IllegalStateException(e);
                }
            }
        }).get();
    }

    private static boolean sameContent(File tmp, File tmpWithUpperCaseName) throws IOException
    {
        final String data = FileSystemUtils.class.getName();
        write(tmp, data);
        return read(tmpWithUpperCaseName).equals(data);
    }

    private static String toUpperCase(String name)
    {
        return name.toUpperCase(Locale.US);
    }

    // methods below have been "copied" from commons io 2.0.1

    private static void write(File file, CharSequence data) throws IOException
    {
        String str = data == null ? null : data.toString();
        writeStringToFile(file, str);
    }

    private static void writeStringToFile(File file, String data) throws IOException
    {
        OutputStream out = null;
        try
        {
            out = openOutputStream(file);
            out.write(data.getBytes());
        }
        finally
        {
            closeQuietly(out);
        }
    }

    private static String read(File file) throws IOException
    {
        InputStream in = null;
        try
        {
            in = openInputStream(file);
            return toString(in);
        }
        finally
        {
            closeQuietly(in);
        }
    }

    private static FileInputStream openInputStream(File file) throws IOException
    {
        if (file.exists())
        {
            if (file.isDirectory())
            {
                throw new IOException("File '" + file + "' exists but is a directory");
            }
            if (!file.canRead())
            {
                throw new IOException("File '" + file + "' cannot be read");
            }
        }
        else
        {
            throw new FileNotFoundException("File '" + file + "' does not exist");
        }
        return new FileInputStream(file);
    }

    private static String toString(InputStream input) throws IOException
    {
        StringBuilderWriter sw = new StringBuilderWriter();
        copy(new InputStreamReader(input), sw);
        return sw.toString();
    }

    private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;

    private static long copy(Reader input, Writer output) throws IOException
    {
        char[] buffer = new char[DEFAULT_BUFFER_SIZE];
        long count = 0;
        int n = 0;
        while (-1 != (n = input.read(buffer)))
        {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

    private static FileOutputStream openOutputStream(File file) throws IOException
    {
        if (file.exists())
        {
            if (file.isDirectory())
            {
                throw new IOException("File '" + file + "' exists but is a directory");
            }
            if (!file.canWrite())
            {
                throw new IOException("File '" + file + "' cannot be written to");
            }
        }
        else
        {
            File parent = file.getParentFile();
            if (parent != null && !parent.exists())
            {
                if (!parent.mkdirs())
                {
                    throw new IOException("File '" + file + "' could not be created");
                }
            }
        }
        return new FileOutputStream(file);
    }

    private static void closeQuietly(Closeable closeable)
    {
        try
        {
            if (closeable != null)
            {
                closeable.close();
            }
        }
        catch (IOException ioe)
        {
            // ignore
        }
    }

    private static final class StringBuilderWriter extends Writer implements Serializable
    {
        private final StringBuilder builder;

        /** Construct a new {@link StringBuilder} instance with default capacity. */
        public StringBuilderWriter()
        {
            this.builder = new StringBuilder();
        }

        /**
         * Construct a new {@link StringBuilder} instance with the specified capacity.
         *
         * @param capacity The initial capacity of the underlying {@link StringBuilder}
         */
        public StringBuilderWriter(int capacity)
        {
            this.builder = new StringBuilder(capacity);
        }

        /**
         * Construct a new instance with the specified {@link StringBuilder}.
         *
         * @param builder The String builder
         */
        public StringBuilderWriter(StringBuilder builder)
        {
            this.builder = (builder != null ? builder : new StringBuilder());
        }

        /**
         * Append a single character to this Writer.
         *
         * @param value The character to append
         * @return This writer instance
         */
        @Override
        public Writer append(char value)
        {
            builder.append(value);
            return this;
        }

        /**
         * Append a character sequence to this Writer.
         *
         * @param value The character to append
         * @return This writer instance
         */
        @Override
        public Writer append(CharSequence value)
        {
            builder.append(value);
            return this;
        }

        /**
         * Append a portion of a character sequence to the {@link StringBuilder}.
         *
         * @param value The character to append
         * @param start The index of the first character
         * @param end The index of the last character + 1
         * @return This writer instance
         */
        @Override
        public Writer append(CharSequence value, int start, int end)
        {
            builder.append(value, start, end);
            return this;
        }

        /** Closing this writer has no effect. */
        @Override
        public void close()
        {
        }

        /** Flushing this writer has no effect. */
        @Override
        public void flush()
        {
        }


        /**
         * Write a String to the {@link StringBuilder}.
         *
         * @param value The value to write
         */
        @Override
        public void write(String value)
        {
            if (value != null)
            {
                builder.append(value);
            }
        }

        /**
         * Write a portion of a character array to the {@link StringBuilder}.
         *
         * @param value The value to write
         * @param offset The index of the first character
         * @param length The number of characters to write
         */
        @Override
        public void write(char[] value, int offset, int length)
        {
            if (value != null)
            {
                builder.append(value, offset, length);
            }
        }

        /**
         * Return the underlying builder.
         *
         * @return The underlying builder
         */
        public StringBuilder getBuilder()
        {
            return builder;
        }

        /**
         * Returns {@link StringBuilder#toString()}.
         *
         * @return The contents of the String builder.
         */
        @Override
        public String toString()
        {
            return builder.toString();
        }
    }
}
