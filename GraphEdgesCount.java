package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class GraphEdgesCount {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private Text Node= new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);

      Boolean flag = false;
      String node = "";
      if(tokenizer.hasMoreTokens()) {
        node = tokenizer.nextToken();
        flag = true;
      }

      int neighbors = 0;
      while(tokenizer.hasMoreTokens()) {
        tokenizer.nextToken(); neighbors++;
      }

      if(flag) {
        IntWritable numOfNeighbors = new IntWritable(neighbors);
        Node.set("NumOfEdges");
        output.collect(Node, numOfNeighbors);
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      int totalNumofNodes = 0;
      int totalNumofEdges = 0;
      int maxOutDegree = -1;
      int minOutDegree = 2147483647;
      while (values.hasNext()) {
        int curEdges = values.next().get();
        if(curEdges > maxOutDegree) {
          maxOutDegree = curEdges;
        }
        if(curEdges < minOutDegree) {
          minOutDegree = curEdges;
        }
        totalNumofEdges += curEdges;
        totalNumofNodes += 1;
      }
      double averageOutDegree = totalNumofEdges / (1.0 * totalNumofNodes);
      Text ReduceKey = new Text(); ReduceKey.set("the graph summary: ");
      Text graphSummary = new Text();
      String graphSummaryStr = "";
      graphSummaryStr += "\n";
      graphSummaryStr += "total number of nodes: " + totalNumofNodes + "\n";
      graphSummaryStr += "total number of edges: " + totalNumofEdges + "\n";
      graphSummaryStr += "max out degree: " + maxOutDegree + "\n";
      graphSummaryStr += "min out degree: " + minOutDegree + "\n";
      graphSummaryStr += "average out degree: " + averageOutDegree + "\n";
      graphSummary.set(graphSummaryStr);

      output.collect(ReduceKey, graphSummary);
    }
  }


  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(GraphEdgesCount.class);
    conf.setJobName("GraphEdgesCount");

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(IntWritable.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
//    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }

}
