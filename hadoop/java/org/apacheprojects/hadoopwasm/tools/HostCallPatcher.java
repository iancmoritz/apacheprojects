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

package org.apacheprojects.hadoopwasm.tools;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
 * Sends the things Hadoop asks the operating system for that a browser cannot do to shims.
 *
 * <p>Positional {@link java.nio.channels.FileChannel} reads and writes go to {@link
 * org.apacheprojects.hadoopwasm.nio.ChannelShim}: CheerpJ 4.2 has no {@code pwrite64}, so {@code
 * channel.write(buffer, position)} throws {@code IllegalArgumentException}, and the NameNode's edit
 * log preallocates through it on every segment it opens.
 *
 * <p>The small shell commands Hadoop runs about a file go to {@link
 * org.apacheprojects.hadoopwasm.util.ShellShim}: a tab has no processes, and CheerpJ's stub {@code
 * exec} succeeds while printing nothing, which makes {@code RawLocalFileSystem}'s permission parser
 * throw {@code NoSuchElementException} while the DataNode is checking its storage directories.
 *
 * <p>{@link Thread#interrupt()} goes to {@link org.apacheprojects.hadoopwasm.util.ThreadShim}, which
 * adds the {@code LockSupport.unpark} CheerpJ does not do: without it an interrupt never reaches a
 * thread waiting in an untimed {@code Condition.await}, and {@code MapTask}'s flush waits forever on
 * the spill thread it has just interrupted.
 *
 * <p>Both live in one pass because two passes writing two jars would each overwrite the other's copy
 * of a class both had patched. Every rewrite turns a call into a static call taking the original
 * receiver and arguments, so the operand stack shape and the stack map frames are unchanged.
 *
 * <p>Usage: {@code HostCallPatcher <output.jar> <jarDirectory> [jarToScan...]}.
 */
public final class HostCallPatcher {

  private static final String CHANNEL = "java/nio/channels/FileChannel";

  private static final String SHIM = "org/apacheprojects/hadoopwasm/nio/ChannelShim";

  private static final String FILE_UTIL = "org/apache/hadoop/fs/FileUtil";

  private static final String SHELL = "org/apache/hadoop/util/Shell";

  private static final String SHELL_SHIM = "org/apacheprojects/hadoopwasm/util/ShellShim";

  private static final String THREAD = "java/lang/Thread";

  private static final String THREAD_SHIM = "org/apacheprojects/hadoopwasm/util/ThreadShim";

  /** Every scanned class to its superclass, which is how a {@link Thread} subclass is recognised. */
  private static final Map<String, String> SUPERS = new HashMap<>();

  private HostCallPatcher() {}

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("usage: HostCallPatcher <output.jar> <jarDirectory> [jarToScan...]");
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
    // Hadoop interrupts its own Thread subclasses -- Daemon, SpillThread -- so the call site's owner
    // is that subclass rather than java/lang/Thread, and rewriting it needs the hierarchy first.
    for (File input : inputs) {
      try (ZipFile zip = new ZipFile(input)) {
        Enumeration<? extends ZipEntry> entries = zip.entries();
        while (entries.hasMoreElements()) {
          ZipEntry entry = entries.nextElement();
          if (entry.isDirectory() || !entry.getName().endsWith(".class")) {
            continue;
          }
          ClassReader reader = new ClassReader(read(zip, entry));
          if (reader.getSuperName() != null && !SUPERS.containsKey(reader.getClassName())) {
            SUPERS.put(reader.getClassName(), reader.getSuperName());
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
        "host call sites rewritten: " + counts[0] + " in " + counts[1] + " classes");
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
        MethodInsnNode replacement = rewrite(call);
        if (replacement == null) {
          continue;
        }
        method.instructions.set(call, replacement);
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

  /** The call to make instead, or null to leave this one alone. */
  private static MethodInsnNode rewrite(MethodInsnNode call) {
    if (call.getOpcode() == Opcodes.INVOKEVIRTUAL
        && CHANNEL.equals(call.owner)
        && isPositional(call.name, call.desc)) {
      return new MethodInsnNode(
          Opcodes.INVOKESTATIC, SHIM, call.name, "(L" + CHANNEL + ";" + drop(call.desc), false);
    }
    if (call.getOpcode() == Opcodes.INVOKESTATIC && isShellCommand(call.owner, call.name, call.desc)) {
      return new MethodInsnNode(Opcodes.INVOKESTATIC, SHELL_SHIM, call.name, call.desc, false);
    }
    if (call.getOpcode() == Opcodes.INVOKEVIRTUAL
        && "interrupt".equals(call.name)
        && "()V".equals(call.desc)
        && isThread(call.owner)) {
      return new MethodInsnNode(
          Opcodes.INVOKESTATIC, THREAD_SHIM, "interrupt", "(L" + THREAD + ";)V", false);
    }
    return null;
  }

  /** Whether this class is {@link Thread} or descends from it, as far as the scanned jars say. */
  private static boolean isThread(String owner) {
    String name = owner;
    while (name != null) {
      if (THREAD.equals(name)) {
        return true;
      }
      name = SUPERS.get(name);
    }
    return false;
  }

  /** The two-argument {@code read} and {@code write}, the ones that take an absolute position. */
  private static boolean isPositional(String name, String desc) {
    return ("read".equals(name) || "write".equals(name))
        && "(Ljava/nio/ByteBuffer;J)I".equals(desc);
  }

  /** {@code FileUtil.execCommand(File, String...)} and {@code Shell.execCommand(String...)}. */
  private static boolean isShellCommand(String owner, String name, String desc) {
    if (!"execCommand".equals(name)) {
      return false;
    }
    return (FILE_UTIL.equals(owner)
            && "(Ljava/io/File;[Ljava/lang/String;)Ljava/lang/String;".equals(desc))
        || (SHELL.equals(owner) && "([Ljava/lang/String;)Ljava/lang/String;".equals(desc));
  }

  /** Turns {@code (args)ret} into {@code args)ret}, so the receiver can be prepended. */
  private static String drop(String desc) {
    return desc.substring(1);
  }
}
