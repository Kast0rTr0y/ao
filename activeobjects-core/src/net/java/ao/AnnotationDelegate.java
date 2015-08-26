/*
 * Copyright 2008 Daniel Spiewak
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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>WARNING: <i>Not</i> part of the public API.  This class is public only
 * to allow its use within other packages in the ActiveObjects library.</p>
 */
public final class AnnotationDelegate {
    private final Method method1, method2;

    public AnnotationDelegate(Method method1, Method method2) {
        this.method1 = checkNotNull(method1);
        this.method2 = method2;
    }

    /**
     * Returns the annotation for the specified methods starting by {@code method1}.
     *
     * @param annotationClass the annotation type to look up
     * @return this element's annotation for the specified annotation type if present on any of the methods, else {@code null}
     * @see Method#getAnnotation(Class)
     */
    public <T extends Annotation> T getAnnotation(Class<T> annotationClass) {
        final T a = method1.getAnnotation(annotationClass);
        if (a != null) {
            return a;
        }
        return method2 == null ? null : method2.getAnnotation(annotationClass);
    }

    /**
     * Returns true if an annotation for the specified type is present on any of the methods, else {@code false}.
     * This method is designed primarily for convenient access to marker annotations.
     *
     * @param annotationClass the Class object corresponding to the annotation type
     * @return {@code true} if an annotation for the specified annotation type is present on any of the methods, else {@code false}
     */
    public boolean isAnnotationPresent(Class<? extends Annotation> annotationClass) {
        return method1.isAnnotationPresent(annotationClass) || (method2 != null && method2.isAnnotationPresent(annotationClass));
    }
}
