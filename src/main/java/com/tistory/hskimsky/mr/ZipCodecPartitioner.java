package com.tistory.hskimsky.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Arrays;

/**
 * description.
 *
 * @author HaNeul, Kim
 * @since 0.1
 */
public class ZipCodecPartitioner extends Partitioner<Text, Text> {
  @Override
  public int getPartition(Text key, Text value, int numPartitions) {
    byte[] bytes = key.getBytes();
    return (Arrays.hashCode(bytes) & Integer.MAX_VALUE) % numPartitions;
  }
}
