package com.gaurav.mapreduce.map;

import com.mobileum.wcmodel.IRecord;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapCombineInputFormat extends Mapper<NullWritable, IRecord, Text, IntWritable> {

  Text text;
  IntWritable intValue = new IntWritable(1);

  @Override
  protected void map(NullWritable key, IRecord value, Context context) throws IOException, InterruptedException {

    System.out.println("MapClass : GG");
    StackTraceElement[] stackTraceArray = Thread.currentThread().getStackTrace();
    for (StackTraceElement stackTraceElement : stackTraceArray) {
      System.out.println("MapClass : GG : " + stackTraceElement);
    }
    System.out.println("GG : MapClass " + value.toString());
    context.write(new Text(value.toString()), new IntWritable(1));
  }
}
