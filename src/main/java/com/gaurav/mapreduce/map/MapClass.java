package com.gaurav.mapreduce.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapClass extends Mapper<Object, Text, Text, IntWritable> {

  Text text;
  IntWritable intValue = new IntWritable(1);

  @Override
  protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    if (value.toString().contains("female")) {
      text = new Text("female");
      context.write(text, intValue);
    } else if (value.toString().contains("male")) {
      text = new Text("male");
      context.write(text, intValue);
    }
  }
}
