package net.java.ao.schema;

import java.util.regex.Pattern;

final class UnderScoreUtils
{
    private static final Pattern WORD_PATTERN = Pattern.compile("([a-z\\d])([A-Z])");

    private UnderScoreUtils()
    {
    }

    static String camelCaseToUnderScore(String s)
    {
        return WORD_PATTERN.matcher(s).replaceAll("$1_$2");
    }
}
