package com.tistory.hskimsky.zipcodec;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ZipEntrySplit extends FileSplit {

  private ZipArchiveInputStream zis;
  private ZipArchiveEntry zipEntry;

  // must exist
  public ZipEntrySplit() {
  }

  public ZipEntrySplit(Path file, long start, long length, String[] hosts) throws IOException {
    super(file, start, length, hosts);
  }

  // public ZipEntrySplit(Path file, ZipArchiveInputStream zis, ZipArchiveEntry zipEntry, String[] hosts) {
  //   super(file, 0, zipEntry.getSize(), hosts);
  //   // super(file, zipEntry.getDataOffset(), zipEntry.getCompressedSize(), hosts);
  //   this.zis = zis;
  //   this.zipEntry = zipEntry;
  // }

  public ZipArchiveInputStream getZis() {
    return zis;
  }

  public void setZis(ZipArchiveInputStream zis) {
    this.zis = zis;
  }

  public ZipArchiveEntry getZipEntry() {
    return zipEntry;
  }

  public void setZipEntry(ZipArchiveEntry zipEntry) {
    this.zipEntry = zipEntry;
  }

  @Override
  public String toString() {
    return getPath() + "!" + (zipEntry == null ? "" : zipEntry.getName()) + ":" + getStart() + "+" + getLength();
  }
}
