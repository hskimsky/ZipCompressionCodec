package com.tistory.hskimsky.zipcodec;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ZipEntryReader extends RecordReader<Text, Text> {

  public static final String BUFFER_SIZE = "mapreduce.input.zipentryreader.buffer.size";
  public static final int DEFAULT_BUFFER_SIZE = 4096;

  private Path path;
  private Configuration conf;

  private long start;
  private long position;
  private long end;
  private boolean isFinish;

  private int bufferSize;
  private ZipArchiveInputStream zis;
  private Text key;
  private Text value;

  public ZipEntryReader() {
  }

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    // ZipEntrySplit split = (ZipEntrySplit) genericSplit;
    FileSplit split = (FileSplit) genericSplit;
    path = split.getPath();
    conf = context.getConfiguration();
    FileSystem fs = path.getFileSystem(conf);
    FileStatus fileStatus = fs.listStatus(path)[0];

    start = split.getStart();
    position = 0;
    end = split.getLength();
    // end = fileStatus.getLen();
    isFinish = false;

    bufferSize = conf.getInt(BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
    // path = ((ZipEntrySplit) genericSplit).getPath();
    zis = new ZipArchiveInputStream(fs.open(path));
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (key == null) {
      key = new Text();
    }
    if (value == null) {
      value = new Text();
    }
    byte[] buffer = new byte[bufferSize];
    int read = 0;
    ZipArchiveEntry zipEntry = null;
    while ((zipEntry = zis.getNextZipEntry()) != null) {
      position = zis.getBytesRead();
      if (position == start) {
        conf.set(MRJobConfig.MAP_INPUT_FILE, path + ":" + zipEntry.getName());
        key.set(zipEntry.getName());
        while ((read = zis.read(buffer)) != -1) {
          value.append(buffer, 0, read);
        }
        return true;
      }
    }
    isFinish = true;
    return false;
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return isFinish ? 1 : ((float) position / end);
  }

  @Override
  public void close() throws IOException {
    if (zis != null) {
      zis.close();
    }
  }
}
