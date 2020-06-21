package com.tistory.hskimsky.zipcodec;

import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * description.
 *
 * @author Haneul, Kim
 * @since 2.1.0
 */
public class ZipCompressionCodec implements SplittableCompressionCodec {

  @Override
  public SplitCompressionInputStream createInputStream(InputStream seekableIn, Decompressor decompressor, long start, long end, READ_MODE readMode) throws IOException {
    return new ZipSplitCompressionInputStream(seekableIn, start, end);
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) {
    return new ZipCompressionOutputStream(out);
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) {
    return new ZipCompressionOutputStream(out);
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return null;
  }

  @Override
  public Compressor createCompressor() {
    return null;
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in) throws IOException {
    return new ZipSplitCompressionInputStream(in);
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor) throws IOException {
    return new ZipSplitCompressionInputStream(in);
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return null;
  }

  @Override
  public Decompressor createDecompressor() {
    return null;
  }

  @Override
  public String getDefaultExtension() {
    return ".zip";
  }
}
