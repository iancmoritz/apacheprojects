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

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import org.apache.xbean.asm9.ClassReader;
import org.apache.xbean.asm9.ClassWriter;
import org.apache.xbean.asm9.Opcodes;
import org.apache.xbean.asm9.tree.AbstractInsnNode;
import org.apache.xbean.asm9.tree.ClassNode;
import org.apache.xbean.asm9.tree.MethodInsnNode;
import org.apache.xbean.asm9.tree.MethodNode;
import org.apache.xbean.asm9.tree.TypeInsnNode;

/**
 * Makes Spark's Java serialization stream write lambdas in a form CheerpJ can read back.
 *
 * <p>Rewrites the {@code new java.io.ObjectOutputStream(...)} inside {@code
 * org.apache.spark.serializer.JavaSerializationStream} to construct {@code
 * ClosureObjectOutputStream} instead, which replaces every lambda with a {@code SerializedClosure}
 * record. Reading is unchanged: that record's {@code readResolve} rebuilds the function through
 * {@code LambdaAdapterFactory}, so the JDK's broken {@code SerializedLambda.readResolve} is never
 * reached.
 *
 * <p>Usage: {@code ClosureStreamPatcher <output.jar> <spark-core.jar>}
 */
public final class ClosureStreamPatcher {

  private static final String TARGET = "org/apache/spark/serializer/JavaSerializationStream.class";

  private static final String ORIGINAL = "java/io/ObjectOutputStream";

  private static final String REPLACEMENT =
      "org/apacheprojects/sparkwasm/lambda/ClosureObjectOutputStream";

  private ClosureStreamPatcher() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("usage: ClosureStreamPatcher <output.jar> <spark-core.jar>");
      System.exit(2);
    }
    JarFile jar = new JarFile(args[1]);
    byte[] original = null;
    Enumeration<JarEntry> entries = jar.entries();
    while (entries.hasMoreElements()) {
      JarEntry entry = entries.nextElement();
      if (entry.getName().equals(TARGET)) {
        original = read(jar.getInputStream(entry));
      }
    }
    jar.close();
    if (original == null) {
      throw new IllegalStateException("no " + TARGET + " in " + args[1]);
    }
    JarOutputStream out = new JarOutputStream(new FileOutputStream(args[0]));
    out.putNextEntry(new ZipEntry(TARGET));
    out.write(patch(original));
    out.closeEntry();
    out.close();
    System.out.println("patched " + TARGET + " -> " + args[0]);
  }

  private static byte[] read(InputStream in) throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    byte[] chunk = new byte[8192];
    int count;
    while ((count = in.read(chunk)) > 0) {
      buffer.write(chunk, 0, count);
    }
    in.close();
    return buffer.toByteArray();
  }

  private static byte[] patch(byte[] bytes) {
    ClassNode node = new ClassNode();
    new ClassReader(bytes).accept(node, 0);
    int rewritten = 0;
    for (MethodNode method : node.methods) {
      for (AbstractInsnNode insn : method.instructions.toArray()) {
        if (insn instanceof TypeInsnNode
            && insn.getOpcode() == Opcodes.NEW
            && ((TypeInsnNode) insn).desc.equals(ORIGINAL)) {
          ((TypeInsnNode) insn).desc = REPLACEMENT;
          rewritten++;
        } else if (insn instanceof MethodInsnNode
            && insn.getOpcode() == Opcodes.INVOKESPECIAL
            && ((MethodInsnNode) insn).owner.equals(ORIGINAL)
            && ((MethodInsnNode) insn).name.equals("<init>")) {
          ((MethodInsnNode) insn).owner = REPLACEMENT;
          rewritten++;
        }
      }
    }
    if (rewritten < 2) {
      throw new IllegalStateException("no ObjectOutputStream construction found in " + TARGET);
    }
    ClassWriter writer = new ClassWriter(0);
    node.accept(writer);
    return writer.toByteArray();
  }
}
