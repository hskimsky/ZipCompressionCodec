package com.tistory.hskimsky.mr;

import com.tistory.hskimsky.zipcodec.SplittableZipInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZipCodec test driver.
 *
 * @author HaNeul, Kim
 * @since 0.1
 */
public class ZipCodecDriver extends Configured implements Tool {

  private static final Logger logger = LoggerFactory.getLogger(ZipCodecDriver.class);

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      printUsage();
      System.exit(1);
    }

    int exitCode = ToolRunner.run(new ZipCodecDriver(), args);
    System.exit(exitCode);
  }

  private static void printUsage() {
    System.err.println("yarn jar <thisJar> " + ZipCodecDriver.class.getName() + " <inputPath> <outputPath> [<force>]");
  }

  @Override
  public int run(String[] args) throws Exception {
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    boolean force = args.length > 2 && Boolean.parseBoolean(args[2]);

    Configuration conf = this.newConf();
    FileSystem fs = outputPath.getFileSystem(conf);
    if (force && fs.exists(outputPath)) {
      fs.delete(outputPath, true);
      System.out.println("deleted " + outputPath);
    }

    Job job = Job.getInstance(conf, ZipCodecDriver.class.getSimpleName() + "_test");
    job.setJarByClass(ZipCodecDriver.class);

    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setInputFormatClass(SplittableZipInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    // mapper
    job.setMapperClass(ZipCodecMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    // partitioner
    job.setPartitionerClass(ZipCodecPartitioner.class);

    // reducer
    job.setReducerClass(ZipCodecReducer.class);
    job.setNumReduceTasks(3);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    boolean completion = job.waitForCompletion(true);

    System.out.println("output path is " + outputPath);

    return completion ? 0 : 1;
  }

  private Configuration newConf() {
    Configuration conf = new Configuration();

    conf.set("fs.defaultFS", "hdfs://nn");
    conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());

    conf.setDouble(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 0.9);
    conf.set(MRJobConfig.QUEUE_NAME, "default");
    conf.setInt(MRJobConfig.MAP_MEMORY_MB, 1024);// default 1024
    conf.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx820m");
    conf.setInt(MRJobConfig.REDUCE_MEMORY_MB, 1024);// default 1024
    conf.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx820m");
    conf.setInt(MRJobConfig.IO_SORT_MB, 512);

    conf.set(TextOutputFormat.SEPARATOR, "^");

    return conf;
  }
}
