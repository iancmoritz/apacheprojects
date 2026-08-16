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

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;

/**
 * Wire form of a lambda, written in place of {@code java.lang.invoke.SerializedLambda}.
 *
 * <p>CheerpJ 4.2 cannot read a JDK serialized lambda back: {@code SerializedLambda.readResolve}
 * throws {@link ArrayIndexOutOfBoundsException} before it reaches the capturing class's {@code
 * $deserializeLambda$} method, and the object {@code LambdaMetafactory.altMetafactory} returns when
 * called reflectively is unusable as well. Since a Spark closure is serialized and deserialized by
 * the same in-tab JVM, {@link ClosureObjectOutputStream} substitutes this record while writing, and
 * {@link #readResolve()} rebuilds an equivalent function through {@link LambdaAdapterFactory} while
 * reading -- so no serialized stream ever contains a JDK serialized lambda.
 */
public final class SerializedClosure implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String functionalInterfaceClass;
  private final String functionalInterfaceMethodName;
  private final String functionalInterfaceMethodSignature;
  private final int implMethodKind;
  private final String implClass;
  private final String implMethodName;
  private final String implMethodSignature;
  private final Object[] capturedArgs;

  SerializedClosure(SerializedLambda lambda) {
    this(
        lambda.getFunctionalInterfaceClass(),
        lambda.getFunctionalInterfaceMethodName(),
        lambda.getFunctionalInterfaceMethodSignature(),
        lambda.getImplMethodKind(),
        lambda.getImplClass(),
        lambda.getImplMethodName(),
        lambda.getImplMethodSignature(),
        capturedArgsOf(lambda));
  }

  SerializedClosure(
      String functionalInterfaceClass,
      String functionalInterfaceMethodName,
      String functionalInterfaceMethodSignature,
      int implMethodKind,
      String implClass,
      String implMethodName,
      String implMethodSignature,
      Object[] capturedArgs) {
    this.functionalInterfaceClass = functionalInterfaceClass;
    this.functionalInterfaceMethodName = functionalInterfaceMethodName;
    this.functionalInterfaceMethodSignature = functionalInterfaceMethodSignature;
    this.implMethodKind = implMethodKind;
    this.implClass = implClass;
    this.implMethodName = implMethodName;
    this.implMethodSignature = implMethodSignature;
    this.capturedArgs = capturedArgs;
  }

  private static Object[] capturedArgsOf(SerializedLambda lambda) {
    Object[] args = new Object[lambda.getCapturedArgCount()];
    for (int i = 0; i < args.length; i++) {
      args[i] = lambda.getCapturedArg(i);
    }
    return args;
  }

  private Object readResolve() {
    if (LambdaAdapterFactory.DEBUG) {
      System.out.println("LAMBDA resolve " + implClass + "." + implMethodName);
    }
    try {
      return LambdaAdapterFactory.create(
          functionalInterfaceClass,
          functionalInterfaceMethodName,
          functionalInterfaceMethodSignature,
          implMethodKind,
          implClass,
          implMethodName,
          implMethodSignature,
          capturedArgs == null ? new Object[0] : capturedArgs);
    } catch (Throwable t) {
      throw new IllegalStateException(
          "cannot rebuild lambda " + implClass + "." + implMethodName + implMethodSignature, t);
    }
  }

  @Override
  public String toString() {
    return "SerializedClosure["
        + implClass
        + "."
        + implMethodName
        + implMethodSignature
        + " as "
        + functionalInterfaceClass
        + "]";
  }
}
