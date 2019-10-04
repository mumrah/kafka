/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka;

import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.notification.RunListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;

public class JUnitRunListener extends RunListener {
    private static final Logger log = LoggerFactory.getLogger(JUnitRunListener.class);


    @SuppressWarnings("unchecked")
    public static Object changeAnnotationValue(Annotation annotation, String key, Object newValue){
        Object handler = Proxy.getInvocationHandler(annotation);
        Field f;
        try {
            f = handler.getClass().getDeclaredField("memberValues");
        } catch (NoSuchFieldException | SecurityException e) {
            throw new IllegalStateException(e);
        }
        f.setAccessible(true);
        Map<String, Object> memberValues;
        try {
            memberValues = (Map<String, Object>) f.get(handler);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
        Object oldValue = memberValues.get(key);

        if (oldValue == null || oldValue.getClass() != newValue.getClass()) {
            System.err.println(oldValue + " " + oldValue.getClass());
            throw new IllegalArgumentException();
        }
        memberValues.put(key,newValue);
        return oldValue;
    }

    static void printDescription(Description description) {
        String out = "Description[";
        out += "testClass=" + description.getTestClass() + ", ";
        out += "method=" + description.getMethodName() + ", ";
        out += "@Test=" + description.getAnnotation(Test.class) + ", ";
        out += "]";
        System.err.println(out);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void testRunStarted(Description description) throws Exception {
        System.err.print("testRunStarted ");

        printDescription(description);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void testStarted(Description description) throws Exception {
        System.err.print("testStarted before ");
        printDescription(description);

        Method method = null;
        for (Method m : description.getTestClass().getMethods()) {
            if (m.getName().equals(description.getMethodName())) {
                method = m;
                break;
            }
        }

        if (method != null) {
            Test annot = method.getAnnotation(Test.class);
            if (annot != null && annot.timeout() == 0) {
                System.err.println("Overriding timeout");
                changeAnnotationValue(annot, "timeout", 1L);
            }
        }

        System.err.print("testStarted after ");
        printDescription(description);
    }
}
