package net.java.ao.util;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.stream.Stream;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class StreamUtils {

    public static <T> Stream<T> ofNullable(@Nullable T t) {
        return ofOptional(Optional.ofNullable(t));
    }

    private static <T> Stream<T> ofOptional(Optional<T> optional) {
        return optional.map(Stream::of).orElseGet(Stream::empty);

    }
}
