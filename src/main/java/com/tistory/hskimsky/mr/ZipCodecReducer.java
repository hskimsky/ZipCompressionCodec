package com.tistory.hskimsky.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * description.
 *
 * @author Haneul, Kim
 * @since 2.1.0
 */
public class ZipCodecReducer extends Reducer<Text, Text, Text, Text> {

    private static final String COUNT_GROUP = "Custom";

    private String outputArraySeparator;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.outputArraySeparator = "|";
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.getCounter(COUNT_GROUP, "reduce read files").increment(1);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.getCounter(COUNT_GROUP, "reduce read groups").increment(1);
        // context.write(new Text(lineNumber), new Text(fileNumber));

        final List<String> fileNumbers = new ArrayList<>();
        values.forEach(value -> fileNumbers.add(value.toString()));

        context.write(key, new Text(StringUtils.join(fileNumbers, this.outputArraySeparator)));
        context.getCounter(COUNT_GROUP, "reduce write records").increment(1);
    }
}
