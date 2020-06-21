package com.tistory.hskimsky.zipcodec;

import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

public class ZipCompressionOutputStream extends CompressionOutputStream {

  protected ZipCompressionOutputStream(OutputStream out) {
    super(new ZipArchiveOutputStream(out));
  }

  @Override
  public void write(int b) throws IOException {
    this.out.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    this.out.write(b, off, len);
  }

  @Override
  public void finish() throws IOException {
    ((ZipArchiveOutputStream) this.out).finish();
  }

  @Override
  public void resetState() throws IOException {
    ((ZipArchiveOutputStream) this.out).closeArchiveEntry();
  }
}
