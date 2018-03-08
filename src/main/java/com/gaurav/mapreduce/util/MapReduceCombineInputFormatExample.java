package com.gaurav.mapreduce.util;

import com.gaurav.mapreduce.map.MapCombineInputFormat;
import com.mobileum.common.hdfs.HdfsUtil;
import com.mobileum.roamingmodel.input.LsltrBasedOutRoamerMessageInputFormat;
import com.mobileum.roamingmodel.input.OutRoamerMessageInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
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
    for (FileStatus fs : HdfsUtil.get().getAllPathsRecursively(new Path[] { new Path("/lib/WS") })) {
      job.addFileToClassPath(fs.getPath());
    }
    // job.addArchiveToClassPath(new Path("/lib/WS/*"));
    // job.addArchiveToClassPath(new Path("/lib/WS/datamodelcommon-1.0-SNAPSHOT.jar"));
    if (args[4].equals("lsltr")) {
      job.setInputFormatClass(LsltrBasedOutRoamerMessageInputFormat.class);
    } else {
      job.setInputFormatClass(OutRoamerMessageInputFormat.class);
    }
    job.getConfiguration().set("roamingmodel.lastProcessedTime", args[0]); // "1481763600"
    job.getConfiguration().set("roamingmodel.datasource.priority", args[1]); // "[NRTRDE_IN,TAP_IN]"
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    // job.setNumReduceTasks(3);
    FileInputFormat.addInputPath(job, new Path(args[2])); // "output/roaming_model/outroamer/message/calltype=international/bintime=1/part*"
    job.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", args[3]); // "gg/priority_output"
    // FileOutputFormat.setOutputPath(new JobConf(conf), new Path("gg/mapreduce/output"));
    return job.waitForCompletion(true) ? 0 : 1;

  }

}
