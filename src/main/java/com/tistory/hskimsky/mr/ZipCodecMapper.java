package com.tistory.hskimsky.mr;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * description.
 *
 * @author HaNeul, Kim
 * @since 0.1
 */
public class ZipCodecMapper extends Mapper<Text, Text, Text, Text> {

  private static final String COUNT_GROUP = "Custom";

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    context.getCounter(COUNT_GROUP, "map read files").increment(1);
  }

  @Override
  protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    context.getCounter(COUNT_GROUP, "map read records").increment(1);

    String entryName = key.toString();
    String fileContents = value.toString();
    String outputValue = StringUtils.join(fileContents.split("\n"), "|");

    context.write(new Text(entryName), new Text(outputValue));
    context.getCounter(COUNT_GROUP, "map write records").increment(1);
  }
}
