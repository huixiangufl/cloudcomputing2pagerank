package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class CalculateSquare {
  public static double globalSquare = 0.0;

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Text Square = new Text();
    private Text RankValue = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String [] val = line.split("\t");
      String rankNeighbors = val[1];
      String [] rankValues = rankNeighbors.split(" ");

      double rankValue1 = new Double(rankValues[0]);
      double rankValue2 = new Double(rankValues[1]);
      double currentSquare = (rankValue1-rankValue2) * (rankValue1-rankValue2);

      Square.set("square value:");
      RankValue.set(String.valueOf(currentSquare));

      output.collect(Square, RankValue);
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    private Text GlobalSquareValue = new Text();
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      double globalSquare = 0.0;
      while(values.hasNext()) {
        globalSquare += new Double(values.next().toString());
      }

      GlobalSquareValue.set(String.valueOf(globalSquare));

      output.collect(key, GlobalSquareValue);
    }
  }


  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(CalculateSquare.class);
    conf.setJobName("calculatesquare");

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
