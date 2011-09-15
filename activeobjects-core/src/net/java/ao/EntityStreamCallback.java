package net.java.ao;

public interface EntityStreamCallback<T extends RawEntity<K>, K>
{

    public void onRowRead(T t);
    
}
