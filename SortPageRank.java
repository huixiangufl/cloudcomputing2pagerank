package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SortPageRank {
  public static final int SORTMAX = 1000000000;
  public static final double SORTMULT = 10000.0f;

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Text RankValue = new Text();
    private Text Page = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String [] val = line.split("\t");
      String page = val[0];
      String rankNeighbors = val[1];

      double pagerank = 0.0;
      int split = rankNeighbors.indexOf(" ");
      if(split >= 0) {
        pagerank = new Double(rankNeighbors.substring(0, split));
      } else {//for nodes with no output degrees
        pagerank = new Double(rankNeighbors);
      }

      RankValue.set(String.valueOf( SORTMAX - (int) (pagerank * SORTMULT) ));
      Page.set(page);
      output.collect(RankValue, Page);
    }
  }


  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {    	
    private Text RankValue = new Text();
    private int count = 0;

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

      while(values.hasNext() && count < 10) {
        double pagerank = (SORTMAX - new Integer(key.toString())) / SORTMULT;
        String pagerankStr = String.format("%12.10f", pagerank);
        RankValue.set(pagerankStr);
        output.collect(RankValue, values.next());
        count++;
      }
    }

  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(SortPageRank.class);
    conf.setJobName("sortpagerank");

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
