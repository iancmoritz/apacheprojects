/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apacheprojects.sparkwasm.lambda;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Object output stream that writes lambdas as {@link SerializedClosure} records.
 *
 * <p>Patched into Spark's {@code JavaSerializationStream} in place of {@link ObjectOutputStream} so
 * that closures shipped to the in-tab executor never travel as {@code
 * java.lang.invoke.SerializedLambda}, which CheerpJ cannot read back. Nested captured lambdas are
 * replaced as well, because their captured arguments are written through this same stream.
 */
public final class ClosureObjectOutputStream extends ObjectOutputStream {

  private static final ConcurrentHashMap<Class<?>, Object> WRITE_REPLACE =
      new ConcurrentHashMap<Class<?>, Object>();

  private static final Object NOT_A_LAMBDA = new Object();

  public ClosureObjectOutputStream(OutputStream out) throws IOException {
    super(out);
    enableReplaceObject(true);
  }

  @Override
  protected Object replaceObject(Object obj) throws IOException {
    if (obj == null) {
      return null;
    }
    if (obj instanceof SerializedLambda) {
      return substitute((SerializedLambda) obj);
    }
    Method writeReplace = writeReplace(obj.getClass());
    if (writeReplace == null) {
      return obj;
    }
    Object replacement;
    try {
      replacement = writeReplace.invoke(obj);
    } catch (Exception e) {
      throw new IOException("cannot capture lambda " + obj.getClass().getName(), e);
    }
    return replacement instanceof SerializedLambda
        ? substitute((SerializedLambda) replacement)
        : obj;
  }

  @Override
  protected void writeClassDescriptor(java.io.ObjectStreamClass desc) throws IOException {
    if (LambdaAdapterFactory.DEBUG && desc.getName().contains("$$Lambda$")) {
      Class<?> cl = desc.forClass();
      String detail = "unknown";
      if (cl != null) {
        try {
          cl.getDeclaredMethod("writeReplace");
          detail = "has writeReplace";
        } catch (NoSuchMethodException e) {
          detail = "no writeReplace";
        } catch (Throwable t) {
          detail = "lookup " + t;
        }
        detail += " synthetic=" + cl.isSynthetic();
      }
      System.out.println("LAMBDA escaped " + desc.getName() + " " + detail);
    }
    super.writeClassDescriptor(desc);
  }

  private static SerializedClosure substitute(SerializedLambda lambda) {
    SerializedClosure closure = new SerializedClosure(lambda);
    if (LambdaAdapterFactory.DEBUG) {
      System.out.println("LAMBDA capture " + closure);
    }
    return closure;
  }

  /** Returns the {@code writeReplace} method of a lambda class, or {@code null} for other classes. */
  private static Method writeReplace(Class<?> type) {
    Object cached = WRITE_REPLACE.get(type);
    if (cached == null) {
      cached = NOT_A_LAMBDA;
      if (type.isSynthetic() && !type.isArray()) {
        try {
          Method method = type.getDeclaredMethod("writeReplace");
          method.setAccessible(true);
          cached = method;
        } catch (NoSuchMethodException e) {
          cached = NOT_A_LAMBDA;
        } catch (RuntimeException e) {
          cached = NOT_A_LAMBDA;
        }
      }
      WRITE_REPLACE.putIfAbsent(type, cached);
    }
    return cached == NOT_A_LAMBDA ? null : (Method) cached;
  }
}
