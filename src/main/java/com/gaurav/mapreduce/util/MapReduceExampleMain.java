package com.gaurav.mapreduce.util;

import com.gaurav.mapreduce.map.MapClass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceExampleMain extends Configured implements org.apache.hadoop.util.Tool {

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new MapReduceExampleMain(), args);
  }

  public int run(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(MapReduceExampleMain.class);
    job.setMapperClass(MapClass.class);
    // job.setCombinerClass(IntSumReducer.class);
    // job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(3);
    FileInputFormat.addInputPath(job, new Path("gg/mapreduce/input"));
    job.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", "gg/mapreduce/output_with_3reducer");
    // FileOutputFormat.setOutputPath(new JobConf(conf), new Path("gg/mapreduce/output"));
    return job.waitForCompletion(true) ? 0 : 1;

    // JobConf job = new JobConf();
    //
    // job.setJarByClass(MapReduceExampleMain.class);
    // job.setJobName("MapReduceExample");
    // job.confi
    //
    // FileInputFormat.setInputPaths(job, comaSeparatedInputFilePathsString);
    // FileOutputFormat.setOutputPath(job, new Path("tmp/merge"));
    //
    // job.setInputFormatClass(MergeActionOrcCombineInputFormat.class);
    // job.setOutputFormatClass(MergeActionOrcOutputFormat.class);
    // job.setSpeculativeExecution(false);
    // job.setMapperClass(MergeActionOrcMapper.class);
    // job.getConfiguration().set(Constants.FINAL_OUTPUT_PATH, jobOutputPath);
    // job.getConfiguration().set(Constants.JOB_TIME_PROPERTY_NAME, System.currentTimeMillis() + "");
    // job.getConfiguration().set(Constants.MERGE_ACTION_SEQUENCE_KEY_COLUMNS, comaSeparatedSequenceKeyColumns);
    // job.setMapOutputKeyClass(Text.class);
    // job.setMapOutputValueClass(TextArrayWritable.class);
    // job.setOutputKeyClass(Text.class);
    // job.setOutputValueClass(Text.class);
    // for (FileStatus fs : HdfsUtil.get().getAllPathsRecursively(new Path[] { new Path("/lib/WS") })) {
    // job.addFileToClassPath(fs.getPath());
    // }
    // System.out.println("GG : mapreduce.map.java.opts = " + System.getProperty("mapreduce.map.java.opts"));
    // return job.waitForCompletion(true) ? 0 : 1;
  }

}
