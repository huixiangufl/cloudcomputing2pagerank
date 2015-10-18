package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class ConvertToOneFile {
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Text Page = new Text();
    private Text RankValue = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String [] val = line.split("\t");
      String page = val[0];
      String rankNeighbors = val[1];

      double pagerank = 0.0;
      int split = rankNeighbors.indexOf(" ");
      if(split >= 0) {
        pagerank = new Double (rankNeighbors.substring(0, split));
      } else {
        pagerank = new Double (rankNeighbors);
      }

      Page.set(page);
      RankValue.set(String.valueOf(pagerank));
      if(line.length() > 3)
       output.collect(Page, RankValue);
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    private Text RankValues = new Text();
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String rankValues = "";
      while(values.hasNext()) {
        String rankValue = values.next().toString();
        rankValues = rankValues + rankValue + " ";
      }
      RankValues.set(rankValues);
      output.collect(key, RankValues);
    }
  }


  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(ConvertToOneFile.class);
    conf.setJobName("converttoonefile");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }


}
