package net.java.ao;

import org.apache.lucene.store.Directory;

public interface LuceneConfiguration
{
    Directory getIndexDirectory();
}
