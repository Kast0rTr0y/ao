package net.java.ao.schema;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.regex.Pattern.compile;

/**
 * <p>A simple table name converter which imposes a set of regular-expression
 * rules to pluralize names generated by the delegate converter.</p>
 *
 * <p>Unfortunately, due to complexities innate to the English language,
 * automatic pluralization is never very accurate. The sheer number of
 * irregular nouns, coupled with the different conjugations which can
 * unrecognizably mangle even an "every day" noun makes it extremely difficult
 * to define generic rules which can control pluralization. ActiveObjects
 * attempts to uphold the highest standards of quality in its pluralization
 * engine by imposing rigorous tests using a wide variety of dictionary words.
 * However, the pluralization should be considered experimental at best.</p>
 *
 * <p>To enable table name pluralization, simply set the name converter for
 * the {@link net.java.ao.EntityManager} in question to an instance of this class.</p>
 */
public final class PluralizedTableNameConverter extends TransformsTableNameConverter {
    private TableNameConverter delegate;

    public PluralizedTableNameConverter() {
        this(new ClassNameTableNameConverter());
    }

    public PluralizedTableNameConverter(CanonicalClassNameTableNameConverter delegateTableNameConverter) {
        super(transforms(), delegateTableNameConverter);
        this.delegate = delegateTableNameConverter;
    }

    private static List<Transform> transforms() {
        final OrderedProperties patterns = OrderedProperties.load("/net/java/ao/schema/englishPluralRules.properties");
        return Lists.transform(newArrayList(patterns), new Function<String, Transform>() {
            public Transform apply(String from) {
                return new PatternTransform(from, patterns.get(from));
            }
        });
    }

    /*
     * TODO why?!?!
     */
    public TableNameConverter getDelegate() {
        return delegate;
    }

    /**
     *
     */
    static final class PatternTransform implements Transform {
        private static final Pattern PLACE_HOLDER_PATTERN = compile("(\\{\\})");

        private final Pattern patternToMatch;
        private final String transformationPattern;

        PatternTransform(String patternToMatch, String transformationPattern) {
            checkNotNull(patternToMatch);
            this.patternToMatch = compile(patternToMatch, Pattern.CASE_INSENSITIVE);
            this.transformationPattern = checkNotNull(transformationPattern);
        }

        public boolean accept(String entityClassCanonicalName) {
            return patternToMatch.matcher(entityClassCanonicalName).matches();
        }

        public String apply(String entityClassCanonicalName) {
            return transform(patternToMatch, entityClassCanonicalName, transformationPattern);
        }

        static String transform(Pattern patternToMatch, String currentString, String transformationPattern) {
            final Matcher m = patternToMatch.matcher(currentString);
            if (m.matches()) {
                return replacePlaceHolders(transformationPattern, m.group(1));
            } else {
                return currentString;
            }
        }

        static String replacePlaceHolders(String stringWithPlaceHolders, String value) {
            return PLACE_HOLDER_PATTERN.matcher(stringWithPlaceHolders).replaceAll(value);
        }
    }
}
