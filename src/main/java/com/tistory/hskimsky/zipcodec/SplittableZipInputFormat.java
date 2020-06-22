package com.tistory.hskimsky.zipcodec;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SplittableZipInputFormat extends FileInputFormat<Text, Text> {

  @Override
  public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    SplittableZipEntryReader zipEntryReader = new SplittableZipEntryReader();
    zipEntryReader.initialize(split, context);
    return zipEntryReader;
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    // final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    // System.out.println("codec.getDefaultExtension() = " + codec.getDefaultExtension());
    // System.out.println("codec.getClass().getName() = " + codec.getClass().getName());
    // System.out.println("file.getName() = " + file.getName());
    // return codec instanceof ZipCompressionCodec && file.getName().endsWith(codec.getDefaultExtension());
    return true;
  }

  /**
   * ZipEntry to InputSplit
   *
   * @param job MapReduce JobContext
   * @return ZipEntries to splits
   * @throws IOException read exception
   * @see FileInputFormat#getSplits(JobContext)
   */
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = new ArrayList<>();
    List<FileStatus> files = listStatus(job);
    Configuration conf = job.getConfiguration();
    boolean ignoreDirs = !getInputDirRecursive(job) && conf.getBoolean(INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS, false);
    for (FileStatus file : files) {
      if (ignoreDirs && file.isDirectory()) {
        continue;
      }
      Path path = file.getPath();
      long length = file.getLen();
      if (length != 0) {
        BlockLocation[] blkLocations;
        FileSystem fs = path.getFileSystem(conf);
        if (file instanceof LocatedFileStatus) {
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
        Set<String> hosts = new HashSet<>();
        for (BlockLocation blkLocation : blkLocations) {
          String[] hostArray = blkLocation.getHosts();
          if (hostArray != null) {
            Collections.addAll(hosts, hostArray);
          }
        }

        FSDataInputStream fSDataInputStream = fs.open(path);
        ZipArchiveInputStream zis = new ZipArchiveInputStream(fSDataInputStream);
        ZipArchiveEntry zipEntry;
        int index = 0;
        while ((zipEntry = zis.getNextZipEntry()) != null) {
          if (!zipEntry.isDirectory()) {
            // ZipEntrySplit zipEntrySplit = new ZipEntrySplit(path, index, index + 1, hosts.toArray(new String[0]));
            // zipEntrySplit.setZis(zis);
            // zipEntrySplit.setZipEntry(zipEntry);
            FileSplit fileSplit = new FileSplit(path, index, index + 1, hosts.toArray(new String[0]));
            splits.add(fileSplit);
            index++;
          }
        }
      }
    }
    job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
    return splits;
  }
}
