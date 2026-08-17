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

package org.apacheprojects.flinkwasm.rest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * An HTTP client for the JobManager's own REST endpoint, spoken over the virtual loopback.
 *
 * <p>The MiniCluster starts the same {@code DispatcherRestEndpoint} a real JobManager does, and it is
 * the thing Flink's web dashboard talks to. In a tab it listens on a socket that only exists inside
 * this JVM, so nothing outside can connect to it -- but code inside can, and that is enough: the
 * page's service worker hands each request the dashboard makes to this class, which speaks HTTP/1.1
 * to netty over a {@link Socket} that {@code VirtualSocketImpl} routes in process, and hands the
 * response back.
 *
 * <p>Written by hand rather than with {@code HttpURLConnection}, which resolves and connects through
 * its own plumbing under CheerpJ; a socket to a virtual port is the one connection that works.
 */
public final class Dashboard {

  private static final int LIMIT = 32 * 1024 * 1024;

  private Dashboard() {}

  /** A response: the status, the headers worth forwarding and the body. */
  public static final class Response {
    public final int status;
    public final String contentType;
    public final byte[] body;

    Response(int status, String contentType, byte[] body) {
      this.status = status;
      this.contentType = contentType;
      this.body = body;
    }

    /** The body as base64, which is how it crosses into JavaScript. */
    public String base64() {
      return Base64.getEncoder().encodeToString(body);
    }
  }

  /**
   * Sends one request to {@code base} and reads the whole response.
   *
   * @param base the REST endpoint's address, as {@code MiniCluster#getRestAddress} reports it
   * @param method the HTTP method
   * @param target the request target, path and query
   * @param body the request body, or null
   */
  public static Response request(URI base, String method, String target, byte[] body)
      throws IOException {
    Socket socket = new Socket();
    try {
      socket.connect(new InetSocketAddress(base.getHost(), base.getPort()), 30_000);
      socket.setSoTimeout(60_000);
      write(socket.getOutputStream(), base, method, target, body);
      return read(socket.getInputStream());
    } finally {
      socket.close();
    }
  }

  private static void write(OutputStream out, URI base, String method, String target, byte[] body)
      throws IOException {
    StringBuilder head = new StringBuilder();
    head.append(method).append(' ').append(target).append(" HTTP/1.1\r\n");
    head.append("Host: ").append(base.getHost()).append(':').append(base.getPort()).append("\r\n");
    head.append("Accept-Encoding: identity\r\n");
    head.append("Connection: close\r\n");
    head.append("Content-Type: application/json; charset=UTF-8\r\n");
    head.append("Content-Length: ").append(body == null ? 0 : body.length).append("\r\n\r\n");
    out.write(head.toString().getBytes("ISO-8859-1"));
    if (body != null) {
      out.write(body);
    }
    out.flush();
  }

  private static Response read(InputStream in) throws IOException {
    byte[] all = drain(in);
    int split = indexOf(all, 0);
    if (split < 0) {
      throw new IOException("no header terminator in " + all.length + " bytes");
    }
    String[] lines = new String(all, 0, split, "ISO-8859-1").split("\r\n");
    int status = status(lines[0]);
    String contentType = "application/octet-stream";
    boolean chunked = false;
    for (int i = 1; i < lines.length; i++) {
      int colon = lines[i].indexOf(':');
      if (colon < 0) {
        continue;
      }
      String name = lines[i].substring(0, colon).trim().toLowerCase();
      String value = lines[i].substring(colon + 1).trim();
      if ("content-type".equals(name)) {
        contentType = value;
      } else if ("transfer-encoding".equals(name) && value.toLowerCase().contains("chunked")) {
        chunked = true;
      }
    }
    byte[] payload = new byte[all.length - split - 4];
    System.arraycopy(all, split + 4, payload, 0, payload.length);
    return new Response(status, contentType, chunked ? dechunk(payload) : payload);
  }

  private static int status(String line) throws IOException {
    String[] parts = line.split(" ");
    if (parts.length < 2) {
      throw new IOException("not a status line: " + line);
    }
    try {
      return Integer.parseInt(parts[1]);
    } catch (NumberFormatException e) {
      throw new IOException("not a status line: " + line);
    }
  }

  /** Everything until the peer closes; the request asked for {@code Connection: close}. */
  private static byte[] drain(InputStream in) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream(64 * 1024);
    byte[] chunk = new byte[64 * 1024];
    int read;
    while ((read = in.read(chunk)) >= 0) {
      buffer.write(chunk, 0, read);
      if (buffer.size() > LIMIT) {
        throw new IOException("response over " + LIMIT + " bytes");
      }
    }
    return buffer.toByteArray();
  }

  private static byte[] dechunk(byte[] payload) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream(payload.length);
    int at = 0;
    while (at < payload.length) {
      int eol = indexOfCrLf(payload, at);
      if (eol < 0) {
        break;
      }
      int size;
      try {
        String header = new String(payload, at, eol - at, "ISO-8859-1").trim();
        int extension = header.indexOf(';');
        size = Integer.parseInt(extension < 0 ? header : header.substring(0, extension), 16);
      } catch (NumberFormatException e) {
        throw new IOException("bad chunk header at " + at);
      }
      at = eol + 2;
      if (size == 0) {
        break;
      }
      out.write(payload, at, Math.min(size, payload.length - at));
      at += size + 2;
    }
    return out.toByteArray();
  }

  /** Where the blank line between headers and body starts, or -1. */
  private static int indexOf(byte[] bytes, int from) {
    for (int i = from; i + 3 < bytes.length; i++) {
      if (bytes[i] == '\r' && bytes[i + 1] == '\n' && bytes[i + 2] == '\r' && bytes[i + 3] == '\n') {
        return i;
      }
    }
    return -1;
  }

  private static int indexOfCrLf(byte[] bytes, int from) {
    for (int i = from; i + 1 < bytes.length; i++) {
      if (bytes[i] == '\r' && bytes[i + 1] == '\n') {
        return i;
      }
    }
    return -1;
  }

  /** The paths the dashboard is allowed to reach, for the page to show what it proxied. */
  public static List<String> seen() {
    return new ArrayList<String>(SEEN);
  }

  private static final List<String> SEEN = new ArrayList<String>();

  /** Records a proxied path, keeping the most recent few. */
  public static synchronized void note(String path) {
    SEEN.add(path);
    while (SEEN.size() > 50) {
      SEEN.remove(0);
    }
  }
}
