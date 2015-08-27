package net.java.ao.schema;

import net.java.ao.RawEntity;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class PluralizedTableNameConverterTest {
    @Test
    public void getNameForEntities() {
        final PluralizedTableNameConverter converter = new PluralizedTableNameConverter();

        assertEquals("StraightRoads", converter.getName(StraightRoad.class));
        assertEquals("People", converter.getName(Person.class));
        assertEquals("Companies", converter.getName(Company.class));
    }

    @Test
    public void getNameForEntitiesWithUpperCaseUnderscoreTableNameConverterAsDelegate() {
        final PluralizedTableNameConverter converter = new PluralizedTableNameConverter(new UnderscoreTableNameConverter(Case.UPPER));

        assertEquals("STRAIGHT_ROADS", converter.getName(StraightRoad.class));
        assertEquals("PEOPLE", converter.getName(Person.class));
        assertEquals("COMPANIES", converter.getName(Company.class));
    }

    @Test
    public void getNameForEntitiesWithLowerCaseUnderscoreTableNameConverterAsDelegate() {
        final PluralizedTableNameConverter converter = new PluralizedTableNameConverter(new UnderscoreTableNameConverter(Case.LOWER));

        assertEquals("straight_roads", converter.getName(StraightRoad.class));
        assertEquals("people", converter.getName(Person.class));
        assertEquals("companies", converter.getName(Company.class));
    }

    @Test
    public void getNameForEntitiesWithCamelCaseTableNameConverterAsDelegate() {
        final PluralizedTableNameConverter converter = new PluralizedTableNameConverter(new CamelCaseTableNameConverter());

        assertEquals("straightRoads", converter.getName(StraightRoad.class));
        assertEquals("people", converter.getName(Person.class));
        assertEquals("companies", converter.getName(Company.class));
    }

    @Test
    public void getNameForASetOfCommonNames() {
        final PluralizedTableNameConverter converter = new PluralizedTableNameConverter();

        assertEquals("companies", converter.getName("company"));
        assertEquals("people", converter.getName("person"));
        assertEquals("roads", converter.getName("road"));
        assertEquals("trees", converter.getName("tree"));
        assertEquals("cards", converter.getName("card"));
        assertEquals("randomPeople", converter.getName("randomPerson"));
        assertEquals("sledges", converter.getName("sledge"));
        assertEquals("signs", converter.getName("sign"));
        assertEquals("records", converter.getName("record"));
        assertEquals("interns", converter.getName("intern"));
        assertEquals("traps", converter.getName("trap"));
        assertEquals("criteria", converter.getName("criterion"));
        assertEquals("cliffs", converter.getName("cliff"));
        assertEquals("calves", converter.getName("calf"));
        assertEquals("theMainTargets", converter.getName("theMainTarget"));
        assertEquals("flowers", converter.getName("flower"));
        assertEquals("hotels", converter.getName("hotel"));
        assertEquals("buildings", converter.getName("building"));
        assertEquals("flags", converter.getName("flag"));
        assertEquals("beautifulPaintings", converter.getName("beautifulPainting"));
        assertEquals("lights", converter.getName("light"));
        assertEquals("fences", converter.getName("fence"));
        assertEquals("curves", converter.getName("curve"));
        assertEquals("ditches", converter.getName("ditch"));
        assertEquals("distances", converter.getName("distance"));
        assertEquals("lines", converter.getName("line"));
        assertEquals("indents", converter.getName("indent"));
        assertEquals("poles", converter.getName("pole"));
        assertEquals("courses", converter.getName("course"));
        assertEquals("towers", converter.getName("tower"));
        assertEquals("hills", converter.getName("hill"));
        assertEquals("prognostications", converter.getName("prognostication"));
        assertEquals("fields", converter.getName("field"));
        assertEquals("stores", converter.getName("store"));
        assertEquals("fairs", converter.getName("fair"));
        assertEquals("stations", converter.getName("station"));
        assertEquals("groves", converter.getName("grove"));
        assertEquals("bridges", converter.getName("bridge"));
        assertEquals("stands", converter.getName("stand"));
        assertEquals("flights", converter.getName("flight"));
        assertEquals("walls", converter.getName("wall"));
        assertEquals("restrictions", converter.getName("restriction"));
        assertEquals("lots", converter.getName("lot"));
        assertEquals("glasses", converter.getName("glass"));
        assertEquals("entrances", converter.getName("entrance"));
        assertEquals("parks", converter.getName("park"));
        assertEquals("houses", converter.getName("house"));
        assertEquals("mice", converter.getName("mouse"));
        assertEquals("pots", converter.getName("pot"));
        assertEquals("boxes", converter.getName("box"));
        assertEquals("movements", converter.getName("movement"));
        assertEquals("stacks", converter.getName("stack"));
        assertEquals("cranes", converter.getName("crane"));
        assertEquals("directions", converter.getName("direction"));
        assertEquals("schools", converter.getName("school"));
        assertEquals("antenni", converter.getName("antenna"));
        assertEquals("horns", converter.getName("horn"));
        assertEquals("mosques", converter.getName("mosque"));
        assertEquals("stops", converter.getName("stop"));
        assertEquals("sports", converter.getName("sport"));
        assertEquals("stairs", converter.getName("stair"));
        assertEquals("camps", converter.getName("camp"));
        assertEquals("gasoline", converter.getName("gasoline"));
        assertEquals("dishes", converter.getName("dish"));
        assertEquals("slopes", converter.getName("slope"));
        assertEquals("rocks", converter.getName("rock"));
        assertEquals("clubs", converter.getName("club"));
        assertEquals("deals", converter.getName("deal"));
        assertEquals("spans", converter.getName("span"));
        assertEquals("pillers", converter.getName("piller"));
        assertEquals("boats", converter.getName("boat"));
        assertEquals("statues", converter.getName("statue"));
        assertEquals("hoppers", converter.getName("hopper"));
        assertEquals("trucks", converter.getName("truck"));
        assertEquals("numbers", converter.getName("number"));
        assertEquals("cameras", converter.getName("camera"));
        assertEquals("artwork", converter.getName("artwork"));
        assertEquals("beaches", converter.getName("beach"));
        assertEquals("terraces", converter.getName("terrace"));
        assertEquals("holes", converter.getName("hole"));
        assertEquals("banks", converter.getName("bank"));
        assertEquals("halls", converter.getName("hall"));
        assertEquals("regions", converter.getName("region"));
        assertEquals("data", converter.getName("data"));
    }

    @Test
    public void testPatternTransform() {
        final TransformsTableNameConverter.Transform transform = new PluralizedTableNameConverter.PatternTransform("(.*p)erson", "{}eople");

        assertEquals("Random", transform.apply("Random"));
        assertEquals("people", transform.apply("person"));
        assertEquals("People", transform.apply("Person"));
        assertEquals("SomePeople", transform.apply("SomePerson"));
        assertEquals("Somepeople", transform.apply("Someperson"));
        assertEquals("SomePersonIsYelling", transform.apply("SomePersonIsYelling"));
    }

    private static interface Person extends RawEntity<Object> {
    }

    private static interface Company extends RawEntity<Object> {
    }

    private static interface StraightRoad extends RawEntity<Object> {
    }
}
