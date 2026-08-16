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

/**
 * Converts Spark closures to named classes as soon as Spark cleans them.
 *
 * <p>Appends a call to {@code Closures.convert} to every return of {@code
 * org.apache.spark.SparkContext.clean}, which every RDD and Dataset operator funnels its function
 * through. Under CheerpJ a lambda class can no longer be looked up by {@code ObjectStreamClass} by
 * the time the scheduler serializes a task ({@code NoClassDefFoundError: ...$$Lambda$NNNN}), so the
 * lambda is rebuilt as a generated named class while it is still fresh.
 *
 * <p>Usage: {@code CleanClosurePatcher <output.jar> <spark-core.jar>}
 */
public final class CleanClosurePatcher {

  private static final String TARGET = "org/apache/spark/SparkContext.class";

  private static final String METHOD = "clean";

  private static final String CONVERTER = "org/apacheprojects/sparkwasm/lambda/Closures";

  private CleanClosurePatcher() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("usage: CleanClosurePatcher <output.jar> <spark-core.jar>");
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
      if (!method.name.equals(METHOD) || !method.desc.endsWith(")Ljava/lang/Object;")) {
        continue;
      }
      for (AbstractInsnNode insn : method.instructions.toArray()) {
        if (insn.getOpcode() == Opcodes.ARETURN) {
          method.instructions.insertBefore(
              insn,
              new MethodInsnNode(
                  Opcodes.INVOKESTATIC,
                  CONVERTER,
                  "convert",
                  "(Ljava/lang/Object;)Ljava/lang/Object;",
                  false));
          rewritten++;
        }
      }
    }
    if (rewritten == 0) {
      throw new IllegalStateException("no " + METHOD + " return found in " + TARGET);
    }
    ClassWriter writer = new ClassWriter(0);
    node.accept(writer);
    return writer.toByteArray();
  }
}
