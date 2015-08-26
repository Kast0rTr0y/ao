package net.java.ao.types;

import org.junit.Test;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

public class TypeQualifiersTest {
    TypeQualifiers q = TypeQualifiers.qualifiers();

    @Test
    public void testPrecisionCompatibility() {
        // COMPATIBLE if precisions match
        assertTrue("Expect compatibility when precision hasn't changed", TypeQualifiers.areCompatible(q.precision(10), q.precision(10)));
        assertTrue("Expect compatibility when neither DB or entity has a precision", TypeQualifiers.areCompatible(q, q));

        // COMPATIBLE if entity does not define a precision, but converse is INCOMPATIBLE (this means that the DB will preserve its constraint even if
        // that's been dropped by the entity, but this is a current limitation of SchemaReader's implementation)
        assertTrue("Expect compatibility when DB has a precision and entity does not have a precision", TypeQualifiers.areCompatible(q, q.precision(10)));
        assertFalse("Expect incompatibility when DB doesn't have a precision and entity does have a precision", TypeQualifiers.areCompatible(q.precision(10), q));

        // INCOMPATIBLE when precision has changed
        assertFalse("Expect incompatibility when DB has a greater precision than the entity", TypeQualifiers.areCompatible(q.precision(10), q.precision(20)));
        assertFalse("Expect incompatibility when DB has a lower precision than the entity", TypeQualifiers.areCompatible(q.precision(20), q.precision(10)));
    }

    @Test
    public void testScaleCompatibility() {
        // COMPATIBLE if scales match
        assertTrue("Expect compatibility when scale hasn't changed", TypeQualifiers.areCompatible(q.scale(10), q.scale(10)));
        assertTrue("Expect compatibility when neither DB or entity has a scale", TypeQualifiers.areCompatible(q, q));

        // COMPATIBLE if entity does not define a scale, but converse is INCOMPATIBLE (this means that the DB will preserve its constraint even if
        // that's been dropped by the entity, but this is a current limitation of SchemaReader's implementation)
        assertTrue("Expect compatibility when DB has a scale and entity does not have a scale", TypeQualifiers.areCompatible(q, q.scale(10)));
        assertFalse("Expect incompatibility when DB doesn't have a scale and entity does have a scale", TypeQualifiers.areCompatible(q.scale(10), q));

        // INCOMPATIBLE when scale has changed
        assertFalse("Expect incompatibility when DB has a greater scale than the entity", TypeQualifiers.areCompatible(q.scale(10), q.scale(20)));
        assertFalse("Expect incompatibility when DB has a lower scale than the entity", TypeQualifiers.areCompatible(q.scale(20), q.scale(10)));
    }

    @Test
    public void testStringLengthCompatibility() {
        // INCOMPATIBLE if there's a mismatch where either the entity or database specifies an UNLIMITED length constraint
        assertFalse("Expect incompatibility when DB has an unlimited string length and entity has a string length", TypeQualifiers.areCompatible(q.stringLength(100), q.stringLength(TypeQualifiers.UNLIMITED_LENGTH)));
        assertFalse("Expect incompatibility when DB does not have a string length and entity has an unlimited string length", TypeQualifiers.areCompatible(q.stringLength(TypeQualifiers.UNLIMITED_LENGTH), q));
        assertFalse("Expect incompatibility when DB has a string length and entity has an unlimited string length", TypeQualifiers.areCompatible(q.stringLength(TypeQualifiers.UNLIMITED_LENGTH), q.stringLength(100)));
        assertFalse("Expect incompatibility when DB has an unlimited string length and entity does not have a string length", TypeQualifiers.areCompatible(q, q.stringLength(TypeQualifiers.UNLIMITED_LENGTH)));
        //assertFalse("Expect incompatibility when DB has an unlimited string length but is sized between 450 and 767 and entity does not have a string length", TypeQualifiers.areCompatible(q, q.stringLength(480)));
        //assertFalse("Expect incompatibility when DB is sized between 450 and 767 and entity is unlimited", TypeQualifiers.areCompatible(q.stringLength(800), q.stringLength(480)));


        // COMPATIBLE in every other case (even when database returns a string length that's lower than the string length specified by the entity, because MySQL returns a string length
        // of 191 for default VARCHAR, and the default string length we get from the entity is always 255.
        assertTrue("Expect compatibility when DB has a greater string length than the entity", TypeQualifiers.areCompatible(q.stringLength(1), q.stringLength(100)));
        assertTrue("Expect compatibility when unlimited string length hasn't changed", TypeQualifiers.areCompatible(q.stringLength(TypeQualifiers.UNLIMITED_LENGTH), q.stringLength(TypeQualifiers.UNLIMITED_LENGTH)));
        assertTrue("Expect compatibility when string length hasn't changed", TypeQualifiers.areCompatible(q.stringLength(100), q.stringLength(100)));
        assertTrue("Expect compatibility when DB has a string length and entity does not have a string length", TypeQualifiers.areCompatible(q, q.stringLength(100)));
        assertTrue("Expect compatibility when neither DB or entity has a string length", TypeQualifiers.areCompatible(q, q));
        assertTrue("Expect incompatibility when DB doesn't have a string length and entity does have a string length", TypeQualifiers.areCompatible(q.stringLength(100), q));
        assertTrue("Expect compatibility when DB has a lower string length than the entity (due to limitation of MySQL - if this ever changes we should flip this assertion)", TypeQualifiers.areCompatible(q.stringLength(100), q.stringLength(1)));
    }
}