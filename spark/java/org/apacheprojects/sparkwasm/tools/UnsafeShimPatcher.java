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

package org.apacheprojects.sparkwasm.tools;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.apache.xbean.asm9.ClassReader;
import org.apache.xbean.asm9.ClassWriter;
import org.apache.xbean.asm9.Opcodes;
import org.apache.xbean.asm9.tree.AbstractInsnNode;
import org.apache.xbean.asm9.tree.ClassNode;
import org.apache.xbean.asm9.tree.MethodInsnNode;
import org.apache.xbean.asm9.tree.MethodNode;

/**
 * Routes {@code sun.misc.Unsafe} floating point accessors through {@link
 * org.apacheprojects.sparkwasm.unsafe.UnsafeShim}.
 *
 * <p>CheerpJ 4.2 does not implement {@code Unsafe.getDouble}, {@code putDouble}, {@code getFloat} or
 * {@code putFloat}; the on-heap forms throw {@code ArrayIndexOutOfBoundsException} and the off-heap
 * forms throw {@code UnsatisfiedLinkError: Java_sun_misc_Unsafe_putDouble}. Spark stores every
 * {@code double} and {@code float} column of an {@code UnsafeRow} with them, so any query over a
 * floating point column fails inside generated code, where nothing can be patched afterwards.
 *
 * <p>Each such call becomes a static call to the shim, which carries the raw bits through the
 * integral accessors. The receiver stays the first argument, so the operand stack shape and the
 * stack map frames are unchanged.
 *
 * <p>Usage: {@code UnsafeShimPatcher <output.jar> <jarDirectory> [jarToScan...]}.
 */
public final class UnsafeShimPatcher {

  private static final String UNSAFE = "sun/misc/Unsafe";

  private static final String SHIM = "org/apacheprojects/sparkwasm/unsafe/UnsafeShim";

  private UnsafeShimPatcher() {}

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("usage: UnsafeShimPatcher <output.jar> <jarDirectory> [jarToScan...]");
      System.exit(2);
    }
    File jarDirectory = new File(args[1]);
    List<File> inputs = new ArrayList<>();
    if (args.length > 2) {
      for (int i = 2; i < args.length; i++) {
        inputs.add(new File(jarDirectory, args[i]));
      }
    } else {
      File[] listed = jarDirectory.listFiles();
      if (listed != null) {
        for (File file : listed) {
          if (file.getName().endsWith(".jar")) {
            inputs.add(file);
          }
        }
      }
    }
    int[] counts = new int[2];
    Set<String> written = new HashSet<>();
    try (ZipOutputStream out = new ZipOutputStream(new FileOutputStream(new File(args[0])))) {
      for (File input : inputs) {
        try (ZipFile zip = new ZipFile(input)) {
          Enumeration<? extends ZipEntry> entries = zip.entries();
          while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();
            if (entry.isDirectory() || !entry.getName().endsWith(".class")) {
              continue;
            }
            byte[] rewritten = patchClass(read(zip, entry), counts);
            if (rewritten != null && written.add(entry.getName())) {
              out.putNextEntry(new ZipEntry(entry.getName()));
              out.write(rewritten);
              out.closeEntry();
            }
          }
        }
      }
    }
    System.out.println(
        "unsafe float call sites rewritten: " + counts[0] + " in " + counts[1] + " classes");
  }

  private static byte[] read(ZipFile zip, ZipEntry entry) throws Exception {
    byte[] buffer = new byte[(int) entry.getSize()];
    try (InputStream in = zip.getInputStream(entry)) {
      int off = 0;
      while (off < buffer.length) {
        int n = in.read(buffer, off, buffer.length - off);
        if (n < 0) {
          break;
        }
        off += n;
      }
    }
    return buffer;
  }

  /** Returns rewritten class bytes, or null when the class touches no such accessor. */
  private static byte[] patchClass(byte[] bytes, int[] counts) {
    ClassNode node = new ClassNode();
    new ClassReader(bytes).accept(node, 0);
    int rewritten = 0;
    for (MethodNode method : node.methods) {
      for (AbstractInsnNode insn : method.instructions.toArray()) {
        if (!(insn instanceof MethodInsnNode)) {
          continue;
        }
        MethodInsnNode call = (MethodInsnNode) insn;
        if (call.getOpcode() != Opcodes.INVOKEVIRTUAL
            || !UNSAFE.equals(call.owner)
            || !isFloatAccessor(call.name)) {
          continue;
        }
        method.instructions.set(
            call,
            new MethodInsnNode(
                Opcodes.INVOKESTATIC, SHIM, call.name, "(L" + UNSAFE + ";" + drop(call.desc), false));
        rewritten++;
      }
    }
    if (rewritten == 0) {
      return null;
    }
    counts[0] += rewritten;
    counts[1]++;
    ClassWriter writer = new ClassWriter(0);
    node.accept(writer);
    return writer.toByteArray();
  }

  private static boolean isFloatAccessor(String name) {
    return "getDouble".equals(name)
        || "putDouble".equals(name)
        || "getFloat".equals(name)
        || "putFloat".equals(name);
  }

  /** Turns {@code (args)ret} into {@code args)ret}, so the receiver can be prepended. */
  private static String drop(String desc) {
    return desc.substring(1);
  }
}
