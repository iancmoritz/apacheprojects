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

package org.apacheprojects.hadoopwasm.fs;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.fs.CachingGetSpaceUsed;
import org.apache.hadoop.fs.GetSpaceUsed;

/**
 * How much space a directory holds, by walking it.
 *
 * <p>Hadoop's default is {@code org.apache.hadoop.fs.DU}, which forks {@code du -sk}. CheerpJ's stub
 * {@code exec} exits successfully and prints nothing, so DU quietly reports zero and the DataNode
 * tells the NameNode it is using no space at all. Selected through Hadoop's own
 * {@code fs.getspaceused.classname} seam rather than by shadowing DU.
 */
public class WalkedSpaceUsed extends CachingGetSpaceUsed {

  public WalkedSpaceUsed(GetSpaceUsed.Builder builder) throws IOException {
    super(builder);
  }

  @Override
  protected void refresh() {
    setUsed(Space.size(new File(getDirPath())));
  }
}
