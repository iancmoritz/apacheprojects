/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apacheprojects.sparkwasm.tools;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.apache.xbean.asm9.ClassReader;
import org.apache.xbean.asm9.ClassWriter;
import org.apache.xbean.asm9.Handle;
import org.apache.xbean.asm9.Opcodes;
import org.apache.xbean.asm9.Type;
import org.apache.xbean.asm9.tree.AbstractInsnNode;
import org.apache.xbean.asm9.tree.ClassNode;
import org.apache.xbean.asm9.tree.InsnList;
import org.apache.xbean.asm9.tree.InsnNode;
import org.apache.xbean.asm9.tree.InvokeDynamicInsnNode;
import org.apache.xbean.asm9.tree.LdcInsnNode;
import org.apache.xbean.asm9.tree.MethodInsnNode;
import org.apache.xbean.asm9.tree.MethodNode;
import org.apache.xbean.asm9.tree.TypeInsnNode;
import org.apache.xbean.asm9.tree.VarInsnNode;

/**
 * Replaces {@code invokedynamic} lambda call sites with an explicit named-class construction.
 *
 * <p>A lambda class created by the JVM at runtime cannot be serialized under CheerpJ 4.2: looking one
 * up through {@code java.io.ObjectStreamClass} fails with a stackless {@code NoClassDefFoundError}
 * naming the {@code Foo$$Lambda$123} class, which aborts Spark task serialization as soon as an RDD
 * holds a function Spark itself created (for instance the {@code (context, pid, iterator)} closure
 * {@code RDD#map} stores in a {@code MapPartitionsRDD}). Closures that arrive through {@code
 * SparkContext.clean} are converted at that point, but Spark's internal ones never pass through it.
 *
 * <p>This pass rewrites the affected call sites ahead of time. Each {@code invokedynamic} bootstrapped
 * by {@code LambdaMetafactory} becomes a call to {@link
 * org.apacheprojects.sparkwasm.lambda.LambdaAdapterFactory#create(String, String, String, int,
 * String, String, String, Object[])} with the very arguments the metafactory would have received, so
 * the instance is an ordinary named class that serializes as a {@code SerializedClosure}. Captured
 * arguments are moved off the operand stack into fresh local variable slots, boxed, and passed as an
 * object array; existing stack map frames stay valid because no branch is introduced and the new
 * slots live above the original {@code maxLocals}.
 *
 * <p>Usage: {@code IndyPatcher <output.jar> <jarDirectory> <internalNamePrefix,...> [jarToScan...]}.
 */
public final class IndyPatcher {

  private static final String FACTORY = "org/apacheprojects/sparkwasm/lambda/LambdaAdapterFactory";

  private static final String CREATE_DESC =
      "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;ILjava/lang/String;"
          + "Ljava/lang/String;Ljava/lang/String;[Ljava/lang/Object;)Ljava/lang/Object;";

  private IndyPatcher() {}

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println(
          "usage: IndyPatcher <output.jar> <jarDirectory> <internalNamePrefix,...> [jarToScan...]");
      System.exit(2);
    }
    File jarDirectory = new File(args[1]);
    String[] prefixes = args[2].split(",");
    List<File> inputs = new ArrayList<>();
    if (args.length > 3) {
      for (int i = 3; i < args.length; i++) {
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
    try (ZipOutputStream out = new ZipOutputStream(new FileOutputStream(new File(args[0])))) {
      for (File input : inputs) {
        try (ZipFile zip = new ZipFile(input)) {
          Enumeration<? extends ZipEntry> entries = zip.entries();
          while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();
            String name = entry.getName();
            if (entry.isDirectory() || !name.endsWith(".class") || !matches(name, prefixes)) {
              continue;
            }
            byte[] rewritten = patchClass(read(zip, entry), counts);
            if (rewritten != null) {
              out.putNextEntry(new ZipEntry(name));
              out.write(rewritten);
              out.closeEntry();
            }
          }
        }
      }
    }
    System.out.println("lambda call sites rewritten: " + counts[0] + " in " + counts[1] + " classes");
  }

  private static boolean matches(String entryName, String[] prefixes) {
    for (String prefix : prefixes) {
      if (entryName.startsWith(prefix)) {
        return true;
      }
    }
    return false;
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

  /** Returns rewritten class bytes, or null when the class has no rewritable call site. */
  private static byte[] patchClass(byte[] bytes, int[] counts) {
    ClassNode node = new ClassNode();
    new ClassReader(bytes).accept(node, 0);
    int rewritten = 0;
    for (MethodNode method : node.methods) {
      int scratch = method.maxLocals;
      int widest = 0;
      for (AbstractInsnNode insn : method.instructions.toArray()) {
        if (!(insn instanceof InvokeDynamicInsnNode)) {
          continue;
        }
        InvokeDynamicInsnNode indy = (InvokeDynamicInsnNode) insn;
        if (!isLambdaMetafactory(indy) || !(indy.bsmArgs[1] instanceof Handle)) {
          continue;
        }
        Handle impl = (Handle) indy.bsmArgs[1];
        if (impl.getName().startsWith("<")) {
          // Constructor and array references are rebuilt reflectively, which the adapter factory
          // cannot express; those call sites keep their invokedynamic.
          continue;
        }
        widest = Math.max(widest, Type.getArgumentTypes(indy.desc).length);
        method.instructions.insertBefore(insn, expand(indy, impl, scratch));
        method.instructions.remove(insn);
        rewritten++;
      }
      // One shared block of scratch slots per method: allocating them per call site pushes large
      // Scala methods past the 255 slot limit, which CheerpJ mis-decodes.
      method.maxLocals = scratch + widest;
    }
    if (rewritten == 0) {
      return null;
    }
    counts[0] += rewritten;
    counts[1]++;
    ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    node.accept(writer);
    return writer.toByteArray();
  }

  private static boolean isLambdaMetafactory(InvokeDynamicInsnNode indy) {
    return "java/lang/invoke/LambdaMetafactory".equals(indy.bsm.getOwner())
        && indy.bsmArgs.length >= 3;
  }

  /**
   * Builds the replacement for one call site: captured arguments off the stack into locals, the
   * metafactory arguments as constants, and the adapter cast to the functional interface.
   */
  private static InsnList expand(InvokeDynamicInsnNode indy, Handle impl, int scratch) {
    Type[] captured = Type.getArgumentTypes(indy.desc);
    Type iface = Type.getReturnType(indy.desc);
    Type samType = (Type) indy.bsmArgs[0];

    int[] slots = new int[captured.length];
    for (int i = 0; i < captured.length; i++) {
      slots[i] = scratch + i;
    }

    InsnList out = new InsnList();
    for (int i = captured.length - 1; i >= 0; i--) {
      box(out, captured[i]);
      out.add(new VarInsnNode(Opcodes.ASTORE, slots[i]));
    }
    out.add(new LdcInsnNode(iface.getInternalName()));
    out.add(new LdcInsnNode(indy.name));
    out.add(new LdcInsnNode(samType.getDescriptor()));
    out.add(push(kind(impl.getTag())));
    out.add(new LdcInsnNode(impl.getOwner()));
    out.add(new LdcInsnNode(impl.getName()));
    out.add(new LdcInsnNode(impl.getDesc()));
    out.add(push(captured.length));
    out.add(new TypeInsnNode(Opcodes.ANEWARRAY, "java/lang/Object"));
    for (int i = 0; i < captured.length; i++) {
      out.add(new InsnNode(Opcodes.DUP));
      out.add(push(i));
      out.add(new VarInsnNode(Opcodes.ALOAD, slots[i]));
      out.add(new InsnNode(Opcodes.AASTORE));
    }
    out.add(new MethodInsnNode(Opcodes.INVOKESTATIC, FACTORY, "create", CREATE_DESC, false));
    out.add(new TypeInsnNode(Opcodes.CHECKCAST, iface.getInternalName()));
    return out;
  }

  /** Method handle tags and {@code SerializedLambda} implementation kinds share their values. */
  private static int kind(int tag) {
    return tag;
  }

  private static AbstractInsnNode push(int value) {
    if (value >= -1 && value <= 5) {
      return new InsnNode(Opcodes.ICONST_0 + value);
    }
    return new org.apache.xbean.asm9.tree.IntInsnNode(
        value <= Byte.MAX_VALUE ? Opcodes.BIPUSH : Opcodes.SIPUSH, value);
  }

  private static void box(InsnList out, Type type) {
    switch (type.getSort()) {
      case Type.BOOLEAN:
        out.add(
            new MethodInsnNode(
                Opcodes.INVOKESTATIC,
                "java/lang/Boolean",
                "valueOf",
                "(Z)Ljava/lang/Boolean;",
                false));
        return;
      case Type.BYTE:
        out.add(
            new MethodInsnNode(
                Opcodes.INVOKESTATIC, "java/lang/Byte", "valueOf", "(B)Ljava/lang/Byte;", false));
        return;
      case Type.CHAR:
        out.add(
            new MethodInsnNode(
                Opcodes.INVOKESTATIC,
                "java/lang/Character",
                "valueOf",
                "(C)Ljava/lang/Character;",
                false));
        return;
      case Type.SHORT:
        out.add(
            new MethodInsnNode(
                Opcodes.INVOKESTATIC, "java/lang/Short", "valueOf", "(S)Ljava/lang/Short;", false));
        return;
      case Type.INT:
        out.add(
            new MethodInsnNode(
                Opcodes.INVOKESTATIC,
                "java/lang/Integer",
                "valueOf",
                "(I)Ljava/lang/Integer;",
                false));
        return;
      case Type.LONG:
        out.add(
            new MethodInsnNode(
                Opcodes.INVOKESTATIC, "java/lang/Long", "valueOf", "(J)Ljava/lang/Long;", false));
        return;
      case Type.FLOAT:
        out.add(
            new MethodInsnNode(
                Opcodes.INVOKESTATIC, "java/lang/Float", "valueOf", "(F)Ljava/lang/Float;", false));
        return;
      case Type.DOUBLE:
        out.add(
            new MethodInsnNode(
                Opcodes.INVOKESTATIC,
                "java/lang/Double",
                "valueOf",
                "(D)Ljava/lang/Double;",
                false));
        return;
      default:
        return;
    }
  }
}
