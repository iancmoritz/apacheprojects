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

package org.apacheprojects.hadoopwasm.util;

import java.io.File;
import java.io.IOException;

/**
 * The output Hadoop expects from the little shell commands it runs, produced without a shell.
 *
 * <p>A tab has no processes. CheerpJ's {@code exec} is a stub that reports success and prints
 * nothing, which is worse than failing: {@code RawLocalFileSystem}'s non-native {@code getPermission}
 * runs {@code ls -ld} and tokenizes the output, so it dies with {@code NoSuchElementException} instead
 * of an {@code IOException} it could report. The DataNode calls it for every storage directory
 * ({@code DiskChecker.mkdirsWithExistsAndPermissionCheck}), so no DataNode can start without this.
 *
 * <p>The call sites are rewritten to come here at build time (see {@code tools/SubprocessPatcher}).
 * There is nothing to consult about modes and ownership: CheerpJ's filesystem has no POSIX metadata,
 * and there is no OS user. So the answer for a permission query is a fixed {@code rwxrwxrwx} owned by
 * the JVM's user, and anything else (a {@code chmod}, a {@code chown}) is a no-op, exactly as it
 * already was when CheerpJ swallowed it. Local file modes shown by the page are therefore synthetic;
 * HDFS's own permissions, which live in the NameNode's namespace rather than on disk, are real.
 */
public final class ShellShim {

  private ShellShim() {}

  private static final String USER = System.getProperty("user.name", "browser");

  /** Hadoop's {@code FileUtil.execCommand(File, String...)}: a command about one file. */
  public static String execCommand(File file, String... command) throws IOException {
    return answer(file, command);
  }

  /** Hadoop's {@code Shell.execCommand(String...)}: the file, if any, is the last argument. */
  public static String execCommand(String... command) throws IOException {
    File file = command.length > 0 ? new File(command[command.length - 1]) : null;
    return answer(file, command);
  }

  private static String answer(File file, String... command) {
    return isPermissionQuery(command) ? listing(file) : "";
  }

  /** {@code Shell.getGetPermissionCommand()} is {@code ls -ld}, and {@code stat} on some platforms. */
  private static boolean isPermissionQuery(String... command) {
    for (String argument : command) {
      if ("-ld".equals(argument) || argument.startsWith("%A")) {
        return true;
      }
    }
    return false;
  }

  /** One line in {@code ls -ld} order: mode, link count, owner, group, size, date, name. */
  private static String listing(File file) {
    boolean directory = file != null && file.isDirectory();
    long size = file == null ? 0L : file.length();
    String name = file == null ? "." : file.getPath();
    return (directory ? "d" : "-")
        + "rwxrwxrwx 1 "
        + USER
        + " supergroup "
        + size
        + " 1970-01-01 00:00 "
        + name
        + "\n";
  }
}
