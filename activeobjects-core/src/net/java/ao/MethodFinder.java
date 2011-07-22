/*
 * Copyright 2007 Daniel Spiewak
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 *	    http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.java.ao;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import net.java.ao.schema.FieldNameConverter;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;

import static com.google.common.base.Preconditions.*;

final class MethodFinder
{
    private final Map<AnnotatedMethods, Iterable<Method>> annotatedMethodsCache;
    private final Map<CounterPartMethod, Supplier<Method>> counterpartCache;

    MethodFinder()
    {
        this.annotatedMethodsCache = new MapMaker().makeComputingMap(new Function<AnnotatedMethods, Iterable<Method>>()
        {
            @Override
            public Iterable<Method> apply(AnnotatedMethods a)
            {
                return a.getAnnotatedMethods();
            }
        });

        this.counterpartCache = new MapMaker().makeComputingMap(new Function<CounterPartMethod, Supplier<Method>>()
        {
            @Override
            public Supplier<Method> apply(CounterPartMethod c)
            {
                return Suppliers.ofInstance(c.getCounterPartMethod());
            }
        });
    }

    public Iterable<Method> findAnnotatedMethods(Class<? extends Annotation> annotation, Class<?> type)
    {
        return annotatedMethodsCache.get(new AnnotatedMethods(annotation, type));
    }

    public Method findCounterPartMethod(FieldNameConverter converter, Method method)
    {
        return counterpartCache.get(new CounterPartMethod(converter, method)).get();
    }

    private final static Supplier<MethodFinder> INSTANCE_SUPPLIER = Suppliers.synchronizedSupplier(Suppliers.memoize(new Supplier<MethodFinder>()
    {
        @Override
        public MethodFinder get()
        {
            return new MethodFinder();
        }
    }));

    public static MethodFinder getInstance()
    {
        return INSTANCE_SUPPLIER.get();
    }

    private static final class AnnotatedMethods
    {
        private final Class<? extends Annotation> annotation;
        private final Class<?> type;

        AnnotatedMethods(Class<? extends Annotation> annotation, Class<?> type)
        {
            this.annotation = checkNotNull(annotation);
            this.type = checkNotNull(type);
        }

        Iterable<Method> getAnnotatedMethods()
        {
            final ImmutableList.Builder<Method> annotatedMethods = ImmutableList.builder();
            for (Method m : type.getMethods())
            {
                if (m.isAnnotationPresent(annotation))
                {
                    annotatedMethods.add(m);
                }
            }
            return annotatedMethods.build();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final AnnotatedMethods that = (AnnotatedMethods) o;

            if (annotation != null ? !annotation.equals(that.annotation) : that.annotation != null)
            {
                return false;
            }
            if (type != null ? !type.equals(that.type) : that.type != null)
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = annotation != null ? annotation.hashCode() : 0;
            result = 31 * result + (type != null ? type.hashCode() : 0);
            return result;
        }
    }

    private static final class CounterPartMethod
    {
        private final FieldNameConverter converter;
        private final Method method;

        CounterPartMethod(FieldNameConverter converter, Method method)
        {
            this.converter = converter;
            this.method = method;
        }

        Method getCounterPartMethod()
        {
            final String name = converter.getName(method);
            final Class<?> clazz = method.getDeclaringClass();

            for (Method other : clazz.getMethods())
            {
                final String otherName = converter.getName(other);
                if (!other.equals(method) && otherName != null && otherName.equals(name))
                {
                    return other;
                }
            }
            return null;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final CounterPartMethod that = (CounterPartMethod) o;

            if (converter != null ? !converter.equals(that.converter) : that.converter != null)
            {
                return false;
            }
            if (method != null ? !method.equals(that.method) : that.method != null)
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = converter != null ? converter.hashCode() : 0;
            result = 31 * result + (method != null ? method.hashCode() : 0);
            return result;
        }
    }
}
