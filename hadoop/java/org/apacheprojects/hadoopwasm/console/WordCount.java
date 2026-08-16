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

package org.apacheprojects.hadoopwasm.console;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * The canonical WordCount, submitted to Hadoop's own {@code LocalJobRunner}.
 *
 * <p>The mapper and reducer are the ones from Hadoop's MapReduce tutorial, unchanged; what is
 * interesting is that everything around them is Hadoop's: {@code TextInputFormat} computes the splits
 * from the {@link FileSystem} it is given, {@code LocalJobRunner} runs the map and reduce tasks with
 * their real spill, sort and merge, the combiner runs, and the output is written back through the same
 * filesystem.
 *
 * <p>Which filesystem that is comes from the page's mode: Hadoop's {@code LocalFileSystem} over
 * CheerpJ's writable filesystem, which is the mode the job runs to completion in, or the
 * MiniDFSCluster, where submitting it freezes the JVM (see {@code HadoopConsole#startLocal} and the
 * README). Nothing here differs between the two.
 *
 * <p>The job is not given a jar ({@code setJarByClass} is deliberately absent): the tasks run in this
 * JVM, so the classes are already on the classpath, and {@code LocalJobRunner} would otherwise copy
 * the jar out of CheerpJ's read-only {@code /app} mount into its staging directory for nothing.
 */
public final class WordCount {

  private WordCount() {}

  /** The tutorial's mapper: one line in, one {@code (word, 1)} pair per token out. */
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);

    private final Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer tokens = new StringTokenizer(value.toString(), " \t\n\r\f,.;:()[]{}\"'*/<>=|&!?#");
      while (tokens.hasMoreTokens()) {
        word.set(tokens.nextToken().toLowerCase());
        context.write(word, one);
      }
    }
  }

  /** The tutorial's reducer, used as the combiner too. */
  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  /**
   * Builds the job, asks Hadoop for its splits, and submits it.
   *
   * <p>{@code getSplits} is called here rather than reported afterwards because it is the one part of
   * the job the page can show before anything has run, and it is the NameNode's block map that
   * answers it.
   */
  static JobRun submit(FileSystem fs, String input, String output) throws Exception {
    Configuration conf = new Configuration(fs.getConf());
    conf.set("fs.defaultFS", fs.getUri().toString());
    // One reduce task: LocalJobRunner runs at most one anyway, and saying so keeps the output to a
    // single part file the page can read back.
    conf.setInt("mapreduce.job.reduces", 1);
    conf.setInt("mapreduce.task.io.sort.mb", 10);
    conf.setBoolean("mapreduce.map.output.compress", false);
    // Native checksums and the native codec loader are not available; the pure-Java paths are.
    conf.setBoolean("io.native.lib.available", false);

    Job job = Job.getInstance(conf, "wordcount");
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(input));
    Path out = new Path(output);
    if (fs.exists(out)) {
      fs.delete(out, true);
    }
    FileOutputFormat.setOutputPath(job, out);

    java.util.List<InputSplit> splits = new TextInputFormat().getSplits(job);
    job.submit();
    return new JobRun(job, splits, out);
  }
}
