package com.tistory.hskimsky.zipcodec;

import org.apache.hadoop.io.compress.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * description.
 *
 * @author Haneul, Kim
 * @since 2.1.0
 */
public abstract class ZipCompressionCodec implements CompressionCodec {

    private class ZipCompressionOutputStream extends CompressionOutputStream {

        ZipCompressionOutputStream(OutputStream out) {
            super(new ZipOutputStream(out));
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
            ((ZipOutputStream) this.out).finish();
        }

        @Override
        public void resetState() throws IOException {
            ((ZipOutputStream) this.out).finish();
        }
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
        return new ZipCompressionOutputStream(out);
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) throws IOException {
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

    private class ZipCompressionInputStream extends CompressionInputStream {

        ZipCompressionInputStream(InputStream in) throws IOException {
            super(new ZipInputStream(in));
            ((ZipInputStream) this.in).getNextEntry();// must call
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
            ((ZipInputStream) this.in).closeEntry();
        }
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in) throws IOException {
        return new ZipCompressionInputStream(in);
    }

    @Override
    public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor) throws IOException {
        return new ZipCompressionInputStream(in);
    }

    @Override
    public Class<? extends Decompressor> getDecompressorType() {
        return null;
    }

    @Override
    public Decompressor createDecompressor() {
        return null;
    }
}
