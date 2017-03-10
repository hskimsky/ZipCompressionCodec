package com.tistory.hskimsky.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * description.
 *
 * @author Haneul, Kim
 * @since 2.1.0
 */
public class ZipCodecMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final String COUNT_GROUP = "Custom";

    private String inputSeparator;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.inputSeparator = "^";
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.getCounter(COUNT_GROUP, "map read files").increment(1);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(COUNT_GROUP, "map read records").increment(1);

        String[] tokens = value.toString().split("\\" + this.inputSeparator, Integer.MAX_VALUE);
        String fileNumber = tokens[0];
        String lineNumber = tokens[1];

        context.write(new Text(lineNumber), new Text(fileNumber));
        context.getCounter(COUNT_GROUP, "map write records").increment(1);
    }
}
