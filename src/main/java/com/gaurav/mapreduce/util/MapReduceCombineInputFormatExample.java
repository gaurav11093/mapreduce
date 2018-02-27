package com.gaurav.mapreduce.util;

import com.gaurav.mapreduce.map.MapCombineInputFormat;
import com.mobileum.roamingmodel.input.LsltrBasedOutRoamerMessageInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceCombineInputFormatExample extends Configured implements org.apache.hadoop.util.Tool {

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new MapReduceCombineInputFormatExample(), args);
  }

  public int run(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "PriorityInputFormat");
    job.setJarByClass(MapReduceCombineInputFormatExample.class);
    job.setMapperClass(MapCombineInputFormat.class);
    job.setInputFormatClass(LsltrBasedOutRoamerMessageInputFormat.class);
    // job.getConfiguration().set("spark.lastProcessedTime", "1481763600");
    job.getConfiguration().set("spark.roamingmodel.datasource.priority", "[NRTRDE_IN,TAP_IN]");
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    // job.setNumReduceTasks(3);
    FileInputFormat.addInputPath(job, new Path("output/roaming_model/outroamer/message/calltype=international/bintime=-1/part*"));
    job.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", "gg/priority_output");
    // FileOutputFormat.setOutputPath(new JobConf(conf), new Path("gg/mapreduce/output"));
    return job.waitForCompletion(true) ? 0 : 1;

  }

}
