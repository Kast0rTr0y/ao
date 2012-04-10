package net.java.ao.it.model;

public enum Deleted
{
    TRUE(true, "Y"),
    FALSE(false, "N");

    private final boolean b;
    private final String s;

    private Deleted(boolean b, String s)
    {
        this.b = b;
        this.s = s;
    }

    public boolean asBoolean()
    {
        return b;
    }

    public String asString()
    {
        return s;
    }

    public static Deleted fromString(String s)
    {
        return TRUE.s.equals(s) ? TRUE : FALSE;
    }
}
