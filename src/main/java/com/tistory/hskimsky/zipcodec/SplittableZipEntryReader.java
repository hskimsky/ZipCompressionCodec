package com.tistory.hskimsky.zipcodec;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class SplittableZipEntryReader extends RecordReader<Text, Text> {

  public static final String BUFFER_SIZE = "mapreduce.input.zipentryreader.buffer.size";

  private Configuration conf;
  private Path path;
  private long start;
  private long position;
  private long end;
  private int bufferSize;
  private ZipArchiveInputStream zis;
  private Text key;
  private Text value;

  public SplittableZipEntryReader() {
  }

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    // ZipEntrySplit split = (ZipEntrySplit) genericSplit;
    FileSplit split = (FileSplit) genericSplit;
    start = split.getStart();
    position = 0;
    end = split.getLength();
    conf = context.getConfiguration();
    bufferSize = conf.getInt(BUFFER_SIZE, 4096);
    // path = ((ZipEntrySplit) genericSplit).getPath();
    path = split.getPath();
    FileSystem fs = path.getFileSystem(conf);
    zis = new ZipArchiveInputStream(fs.open(path));
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (position > start) {
      return false;
    }
    if (key == null) {
      key = new Text();
    }
    if (value == null) {
      value = new Text();
    }
    byte[] buffer = new byte[bufferSize];
    int read = 0;
    long sum = 0;
    ZipArchiveEntry zipEntry = null;
    while ((zipEntry = zis.getNextZipEntry()) != null) {
      if (position == start) {
        conf.set(MRJobConfig.MAP_INPUT_FILE, path + ":" + zipEntry.getName());
        key.set(zipEntry.getName());
        while ((read = zis.read(buffer)) != -1) {
          value.append(buffer, 0, read);
          sum += read;
        }
        position++;
        return true;
      }
      position++;
    }
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
    return position > start ? 1 : 0;
  }

  @Override
  public void close() throws IOException {
    if (zis != null) {
      zis.close();
    }
  }
}
