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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.xbean.asm9.ClassReader;
import org.apache.xbean.asm9.ClassWriter;
import org.apache.xbean.asm9.Opcodes;
import org.apache.xbean.asm9.tree.AbstractInsnNode;
import org.apache.xbean.asm9.tree.ClassNode;
import org.apache.xbean.asm9.tree.MethodInsnNode;
import org.apache.xbean.asm9.tree.MethodNode;

/**
 * Restores the interface flag on the method references Retrolambda flattens.
 *
 * <p>Retrolambda rewrites calls to interface static and default methods as plain {@code Methodref}
 * entries, which is right for its Java 7 target but not for the Java 8 classes we keep: the verifier
 * rejects them with {@code IncompatibleClassChangeError: ... must be InterfaceMethodref constant}.
 * Scala 2.12 compiles every trait body to such a call, so this pass walks the desugared classes and
 * puts the flag back wherever the owner really is an interface.
 *
 * <p>Usage: {@code InterfaceRefPatcher <classes-dir> <jar-dir>}
 */
public final class InterfaceRefPatcher {

  private InterfaceRefPatcher() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("usage: InterfaceRefPatcher <classes-dir> <jar-dir>");
      System.exit(2);
    }
    final Path classes = new File(args[0]).toPath();
    final Set<String> interfaces = interfaces(new File(args[1]), classes);
    final int[] counts = new int[2];
    Files.walkFileTree(
        classes,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            if (!file.toString().endsWith(".class")) {
              return FileVisitResult.CONTINUE;
            }
            byte[] patched = patch(Files.readAllBytes(file), interfaces, counts);
            if (patched != null) {
              FileOutputStream out = new FileOutputStream(file.toFile());
              out.write(patched);
              out.close();
              counts[1]++;
            }
            return FileVisitResult.CONTINUE;
          }
        });
    System.out.println(
        "interface refs restored: " + counts[0] + " in " + counts[1] + " classes");
  }

  private static byte[] patch(byte[] bytes, Set<String> interfaces, int[] counts) {
    ClassNode node = new ClassNode();
    new ClassReader(bytes).accept(node, 0);
    int fixed = 0;
    for (MethodNode method : node.methods) {
      for (AbstractInsnNode insn : method.instructions.toArray()) {
        if (!(insn instanceof MethodInsnNode)) {
          continue;
        }
        MethodInsnNode call = (MethodInsnNode) insn;
        if (call.itf || !interfaces.contains(call.owner)) {
          continue;
        }
        if (call.getOpcode() == Opcodes.INVOKESTATIC
            || call.getOpcode() == Opcodes.INVOKESPECIAL) {
          call.itf = true;
          fixed++;
        } else if (call.getOpcode() == Opcodes.INVOKEVIRTUAL) {
          method.instructions.set(
              call,
              new MethodInsnNode(
                  Opcodes.INVOKEINTERFACE, call.owner, call.name, call.desc, true));
          fixed++;
        }
      }
    }
    if (fixed == 0) {
      return null;
    }
    counts[0] += fixed;
    ClassWriter writer = new ClassWriter(0);
    node.accept(writer);
    return writer.toByteArray();
  }

  /** Every interface name on the classpath: the jars, plus the classes being patched. */
  private static Set<String> interfaces(File jarDirectory, Path classes) throws Exception {
    final Set<String> names = new HashSet<String>();
    File[] jars = jarDirectory.listFiles();
    if (jars != null) {
      for (File file : jars) {
        if (!file.getName().endsWith(".jar")) {
          continue;
        }
        JarFile jar = new JarFile(file);
        Enumeration<JarEntry> entries = jar.entries();
        while (entries.hasMoreElements()) {
          JarEntry entry = entries.nextElement();
          if (!entry.getName().endsWith(".class")) {
            continue;
          }
          InputStream in = jar.getInputStream(entry);
          collect(in, names);
          in.close();
        }
        jar.close();
      }
    }
    Files.walkFileTree(
        classes,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            if (file.toString().endsWith(".class")) {
              collect(Files.newInputStream(file), names);
            }
            return FileVisitResult.CONTINUE;
          }
        });
    return names;
  }

  private static void collect(InputStream in, Set<String> names) throws IOException {
    ClassReader reader = new ClassReader(in);
    if ((reader.getAccess() & Opcodes.ACC_INTERFACE) != 0) {
      names.add(reader.getClassName());
    }
  }
}
