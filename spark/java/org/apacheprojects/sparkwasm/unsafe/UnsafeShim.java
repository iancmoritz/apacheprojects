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

package org.apacheprojects.sparkwasm.unsafe;

import sun.misc.Unsafe;

/**
 * Floating point accessors for {@code sun.misc.Unsafe} under CheerpJ.
 *
 * <p>CheerpJ implements the integral {@code Unsafe} accessors but not {@code getDouble}, {@code
 * putDouble}, {@code getFloat} or {@code putFloat}: the on-heap forms raise {@code
 * ArrayIndexOutOfBoundsException} and the off-heap forms raise {@code UnsatisfiedLinkError:
 * Java_sun_misc_Unsafe_putDouble}. Every {@code double} and {@code float} column in an {@code
 * UnsafeRow} goes through them, so each call site in Spark's bytecode is rewritten to the matching
 * method here, which moves the raw bits through the integral accessors instead.
 */
public final class UnsafeShim {

  private UnsafeShim() {}

  public static double getDouble(Unsafe unsafe, Object object, long offset) {
    return Double.longBitsToDouble(unsafe.getLong(object, offset));
  }

  public static void putDouble(Unsafe unsafe, Object object, long offset, double value) {
    unsafe.putLong(object, offset, Double.doubleToRawLongBits(value));
  }

  public static double getDouble(Unsafe unsafe, long address) {
    return Double.longBitsToDouble(unsafe.getLong(address));
  }

  public static void putDouble(Unsafe unsafe, long address, double value) {
    unsafe.putLong(address, Double.doubleToRawLongBits(value));
  }

  public static float getFloat(Unsafe unsafe, Object object, long offset) {
    return Float.intBitsToFloat(unsafe.getInt(object, offset));
  }

  public static void putFloat(Unsafe unsafe, Object object, long offset, float value) {
    unsafe.putInt(object, offset, Float.floatToRawIntBits(value));
  }

  public static float getFloat(Unsafe unsafe, long address) {
    return Float.intBitsToFloat(unsafe.getInt(address));
  }

  public static void putFloat(Unsafe unsafe, long address, float value) {
    unsafe.putInt(address, Float.floatToRawIntBits(value));
  }
}
