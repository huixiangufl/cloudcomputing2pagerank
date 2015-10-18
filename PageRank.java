package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class PageRank {
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private Text Page = new Text();
    private Text ValueCategory1 = new Text();
    private Text Neighbor = new Text();
    private Text ValueCategory2 = new Text();	//this is the pagerank value that each neighbors get

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String line = value.toString();
      //split the value to key and value: key: page, value: rankvalue<space>numOfNeighbors<space>links
      String [] val = line.split("\t");
      String page = val[0];
      String rankNeighbors = val[1]; //rankvalue<space>numOfNeighbors<space>links

      int split = rankNeighbors.indexOf(" ");
      if(split >= 0) { //if split<0, there is no out links
        double pagerank = new Double(rankNeighbors.substring(0, split));
        rankNeighbors = rankNeighbors.substring(split+1);
        //first outputs: <page, numOfNeighbors<space>links>
        Page.set(page);
        ValueCategory1.set(rankNeighbors);
        output.collect(Page, ValueCategory1);

        //then calculate the pagerank for each neighbor node, and outputs: <neighborPage, pagerankvalue>
        split = rankNeighbors.indexOf(" ");
        double numOfNeighbors = new Double(rankNeighbors.substring(0, split));
        rankNeighbors = rankNeighbors.substring(split+1);

        String[] neighbors = rankNeighbors.split(" ");
        for(String n : neighbors) {
          Neighbor.set(n);
          ValueCategory2.set(String.valueOf(pagerank/numOfNeighbors));
          output.collect(Neighbor, ValueCategory2);
        }
      } else { // if there is no output degree for that page, output <page, "missingpagerankvalue">
        Page.set(page);
        ValueCategory1.set("missingpagerankvalue");
        output.collect(Page, ValueCategory1);
      }
    }

  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    private Text OutputText = new Text();

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      double currentRank = 0.15;
      String rankNeighbors = "";
      Boolean zeroOutputFlag = false; //indicate that this node has no output degree
      while(values.hasNext()) {
        String acceptedValues = values.next().toString();
        if(acceptedValues.equals("missingpagerankvalue")) {
          System.out.println("Huixiang Chen True.\n");
          zeroOutputFlag = true;
          continue;
        }
        int split = acceptedValues.indexOf(" ");
        if(split < 0) { // this is a category2 <key, value> pair
          currentRank += 0.85 * new Double(acceptedValues);
        } else { // this is a category1 <key, value> pair, each node only has one or none
          rankNeighbors = acceptedValues;
        }
      }

      if(!zeroOutputFlag) {
        OutputText.set(String.valueOf(currentRank) + " " + rankNeighbors);
      }else {
        OutputText.set(String.valueOf(currentRank));
      }

      output.collect(key, OutputText);
    }
  }


  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(PageRank.class);
    conf.setJobName("pagerank");

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
