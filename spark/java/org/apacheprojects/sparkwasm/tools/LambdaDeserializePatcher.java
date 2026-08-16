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
import java.io.File;
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
import org.apache.xbean.asm9.tree.ClassNode;
import org.apache.xbean.asm9.tree.InsnList;
import org.apache.xbean.asm9.tree.InsnNode;
import org.apache.xbean.asm9.tree.MethodInsnNode;
import org.apache.xbean.asm9.tree.MethodNode;
import org.apache.xbean.asm9.tree.VarInsnNode;

/**
 * Rewrites {@code scala.runtime.LambdaDeserialize#deserializeLambda} to call {@code
 * LambdaAdapterFactory}.
 *
 * <p>Scala's implementation rebuilds a serialized lambda with {@code
 * LambdaMetafactory.altMetafactory}, invoked reflectively at run time. Under CheerpJ 4.2 that call
 * returns an instance on which every operation -- including {@code getClass()} -- throws {@link
 * ArrayIndexOutOfBoundsException}, so no Spark task closure can be rebuilt. The replacement builds an
 * equivalent adapter that calls the same target method.
 *
 * <p>Usage: {@code LambdaDeserializePatcher <output.jar> <scala-library.jar>}
 */
public final class LambdaDeserializePatcher {

  private static final String TARGET = "scala/runtime/LambdaDeserialize.class";

  private LambdaDeserializePatcher() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("usage: LambdaDeserializePatcher <output.jar> <scala-library.jar>");
      System.exit(2);
    }
    JarFile jar = new JarFile(args[1]);
    byte[] original = null;
    Enumeration<JarEntry> entries = jar.entries();
    while (entries.hasMoreElements()) {
      JarEntry entry = entries.nextElement();
      if (!entry.getName().equals(TARGET)) {
        continue;
      }
      InputStream in = jar.getInputStream(entry);
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      byte[] chunk = new byte[8192];
      int read;
      while ((read = in.read(chunk)) > 0) {
        buffer.write(chunk, 0, read);
      }
      in.close();
      original = buffer.toByteArray();
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

  private static byte[] patch(byte[] bytes) {
    ClassNode node = new ClassNode();
    new ClassReader(bytes).accept(node, 0);
    boolean patched = false;
    for (MethodNode method : node.methods) {
      if (!method.name.equals("deserializeLambda")) {
        continue;
      }
      InsnList body = new InsnList();
      body.add(new VarInsnNode(Opcodes.ALOAD, 1));
      body.add(
          new MethodInsnNode(
              Opcodes.INVOKESTATIC,
              "org/apacheprojects/sparkwasm/lambda/LambdaAdapterFactory",
              "create",
              "(Ljava/lang/invoke/SerializedLambda;)Ljava/lang/Object;",
              false));
      body.add(new InsnNode(Opcodes.ARETURN));
      method.instructions.clear();
      method.tryCatchBlocks.clear();
      if (method.localVariables != null) {
        method.localVariables.clear();
      }
      method.instructions.add(body);
      method.maxStack = 2;
      method.maxLocals = 2;
      patched = true;
    }
    if (!patched) {
      throw new IllegalStateException("deserializeLambda not found");
    }
    ClassWriter writer = new ClassWriter(0);
    node.accept(writer);
    return writer.toByteArray();
  }
}
