package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class LinkGraph {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String line = value.toString();
      int split = line.indexOf(" ");
      String page = "";
      String rankNeighbors = "";
      if(split >= 0) {
        page = line.substring(0, split);

        int numOfNeighbors = 1;
        for (char c: line.substring(split+1).toCharArray() ) {
          if(c == ' ') numOfNeighbors++;
        }

        rankNeighbors = "1.0 "+ numOfNeighbors + " " + line.substring(split+1);
      } else {
        page = line;
        rankNeighbors = "1.0";
      }

      if(page.length() >= 3) {
        Text Page = new Text();	Page.set(page);
        Text RankNeighbors = new Text();	RankNeighbors.set(rankNeighbors);
        output.collect(Page, RankNeighbors);
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> links, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      if(links.hasNext()) {
        output.collect(key, links.next());
      }else {
        throw new IOException("No coded links for page" + key.toString());
      }
    }


  }


}
