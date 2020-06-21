package com.tistory.hskimsky.zipcodec;

import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;

import java.io.IOException;
import java.io.InputStream;

public class ZipSplitCompressionInputStream extends SplitCompressionInputStream {

  public ZipSplitCompressionInputStream(InputStream in) throws IOException {
    this(new ZipArchiveInputStream(in), 0, Long.MAX_VALUE);
  }

  public ZipSplitCompressionInputStream(InputStream in, long start, long end) throws IOException {
    super(new ZipArchiveInputStream(in), start, end);
    // ((ZipArchiveInputStream) this.in).getNextEntry();// must call
  }

  @Override
  public int read() throws IOException {
    return this.in.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return this.in.read(b, off, len);
  }

  @Override
  public void resetState() throws IOException {
    this.in.close();
  }
}
