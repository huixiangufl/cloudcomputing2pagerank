# cloudcomputing2pagerank
The procedure of the main loop in ExecutionPageRank.java is written as follows in Pseudo code:

Main:

call LinkGraph to convert the input graph format to our defined format

NumOfIteartions=0

for each loop:

\t  call Pagerank (ConvertToOneFile)

\t  call ConvertToOneFile (GraphFileThisRound, GraphFileLastRound)

\t  call CalculateSquare(ConvertOutputFile)

\t  NumOfIterations=NumOfIterations+1

\t  if square <= ThresHold

\t\t  exit loop

\t  else    

\t\t continue

call GraphEdgesCount to get the number of nodes, the number of edges, average/max/min out-degree of nodes.

call SortPageRank to get the top 10 <page, value> pairs

System.out.printLn(NumOfIterations)



#input graph format: 
page neighbor_set

#our defined graph format:
page\t page_rank_value num_of_neighbors neighbor_set


#LinkGraph:
Input: input graph format
Output: defined graph format with pagerank value initialized as 1.0

#Pagerank: 
Input: defined graph format
Output: defined graph format

#ConvertToOneFile:
Input: graph_this_iteration and graph_last_iteration
Output: In each row: <page> <page_rank_value_this_iteration> <page_rank_value_last_iteration>

#CalculateSquare:
Input: Output of ConvertToOneFile
Output: Standard deviation of pagerank values of this iteration with last iteration

#GraphEdgesCount:
Input: input graph format
Output: number of nodes, edges, average/min/max out-degree

#SortPageRank: 
Input: graph of the last iteration
Output: sorted <page, value> pairs of top 10 in descending order


#How to run the program?
Four input parameters:

para1: inputGraphFilePath

para2: intermediateFilePath (generated by PageRank/ConvertToOneFile/CalculateSquare)

para3: graphSummaryOutputFile

para4: top10<k,v>outputFile

#compile and run
mkdir pagerank_classes

javac -classpath $HADOOP_HOME/hadoop-1.2.1/hadoop-core-1.2.1.jar -d pagerank_classes/ *.java

jar -cvf ExecutePageRank.jar -C pagerank_classes .

hadoop jar ExecutePageRank.jar org.myorg.ExecutePageRank para1 para2 para3 para4
