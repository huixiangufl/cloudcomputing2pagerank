package org.myorg;

import java.io.IOException;
import java.util.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.myorg.GraphEdgesCount;
import org.myorg.LinkGraph;
import org.myorg.PageRank;
import org.myorg.SortPageRank;
import org.myorg.ConvertToOneFile;
import org.myorg.CalculateSquare;


public class ExecutePageRank {

//	public static double globalSquare = 0.0;
    public static Boolean exitFlag = false;
    public static double squareThreshold = 1.0;

  public static void main(String [] args) throws Exception {
    if(args.length < 4) {
      System.out.println("Usage: ExecutePageRank <inputGraphFile> <intermediateFilePath> <graphSummaryOutput> <top10PageRankoutput>");
      System.exit(-1);
    }

    Path inputPath = new Path(args[0]);
    Path intermediateFilePath = new Path(args[1]);
    String intermediateFilePathStr = args[1];
    Path graphSummaryPath = new Path(args[2]);
    Path top10PageRankPath = new Path(args[3]);

    JobConf conf = new JobConf(LinkGraph.class);
    conf.setJobName("linkgraph");	
    FileSystem fs = FileSystem.get(conf);

    //delete intemediate path if exists
    if(fs.exists(intermediateFilePath)) fs.delete(intermediateFilePath, true);
    if(fs.exists(graphSummaryPath)) fs.delete(graphSummaryPath, true);
    if(fs.exists(top10PageRankPath)) fs.delete(top10PageRankPath, true);

    //Build the initial link for the input graph, by running job LinkGraph
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(LinkGraph.Map.class);
    conf.setReducerClass(LinkGraph.Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, new Path(intermediateFilePathStr + "/iteration0"));
    JobClient.runJob(conf);

    int iteration = 1;

    do {
      // now use the PageRank class
      conf = new JobConf(PageRank.class);
      conf.setJobName("pagerank");
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);
      conf.setMapperClass(PageRank.Map.class);
      conf.setReducerClass(PageRank.Reduce.class);
      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
      FileInputFormat.setInputPaths(conf, intermediateFilePathStr + "/iteration" + Integer.toString(iteration-1));
      FileOutputFormat.setOutputPath(conf, new Path(intermediateFilePathStr + "/iteration" + Integer.toString(iteration)));	
      JobClient.runJob(conf);

      // now use the ConvertToOneFile class
      conf = new JobConf(ConvertToOneFile.class);
      conf.setJobName("converttoonefile");
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);
      conf.setMapperClass(ConvertToOneFile.Map.class);
      conf.setReducerClass(ConvertToOneFile.Reduce.class);    
      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      String cInput = intermediateFilePathStr + "/iteration" + Integer.toString(iteration-1);
      cInput += "," + intermediateFilePathStr + "/iteration" + Integer.toString(iteration);
      String cOutput = intermediateFilePathStr + "/convertoutput" + Integer.toString(iteration);
      FileInputFormat.setInputPaths(conf, cInput);
      FileOutputFormat.setOutputPath(conf, new Path(cOutput));
      JobClient.runJob(conf);

      //now use the CalculateSquare class to get the standard deviation of pagerank values of this round and last round
      
      conf = new JobConf(CalculateSquare.class);
      conf.setJobName("calculatesquare");
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);
      conf.setMapperClass(CalculateSquare.Map.class);
      conf.setReducerClass(CalculateSquare.Reduce.class);
      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      String sInput = cOutput;
      String sOutput = intermediateFilePathStr + "/squarevalue" + Integer.toString(iteration);
      FileInputFormat.setInputPaths(conf, sInput);
      FileOutputFormat.setOutputPath(conf, new Path(sOutput));
      RunningJob curJob = JobClient.runJob(conf);

      curJob.waitForCompletion();

      //read square from hdfs to test if it's below a threshold value, if below, then exit
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(sOutput + "/part-00000"))));
      String line = bufferedReader.readLine();
      if(line != null) {
        String [] val = line.split("\t");
        double pagerank = new Double(val[1]);
        System.out.println("Huixiang Chen Success. value: " + pagerank);
        if(pagerank < squareThreshold) {
          exitFlag = true;
        }
      }


      iteration++;
    } while (!exitFlag);
    
    // next execute class GraphEdgesCount to get the graph property
    conf = new JobConf(GraphEdgesCount.class);
    conf.setJobName("graphedgescount");
    
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(IntWritable.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(GraphEdgesCount.Map.class);
    conf.setReducerClass(GraphEdgesCount.Reduce.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    
    FileInputFormat.setInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, graphSummaryPath);
    JobClient.runJob(conf);
    
    // next execute class SortPageRank to get the top ten <node, pagerank> pair, which is stored in the hdfs
    conf = new JobConf(SortPageRank.class);
    conf.setJobName("sortpagerank");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(SortPageRank.Map.class);
    conf.setReducerClass(SortPageRank.Reduce.class);
    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, intermediateFilePathStr + "/iteration" + Integer.toString(iteration-1));
    FileOutputFormat.setOutputPath(conf, top10PageRankPath);
    JobClient.runJob(conf);
    
    iteration -= 1;
    System.out.println("The total number of iterations to let it converge is: " + iteration);

  }

};
