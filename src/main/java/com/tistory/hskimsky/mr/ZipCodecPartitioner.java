package com.tistory.hskimsky.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Arrays;

/**
 * description.
 *
 * @author Haneul, Kim
 * @since 2.1.0
 */
public class ZipCodecPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        byte[] bytes = key.getBytes();
        return (Arrays.hashCode(bytes) & Integer.MAX_VALUE) % numPartitions;
    }
}
