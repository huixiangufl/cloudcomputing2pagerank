# cloudcomputing2pagerank
The procedure of the main loop in ExecutionPageRank.java is written as follows in Pseudo code:

Main:
call LinkGraph to convert the input graph format to our defined format
NumOfIteartions=0
for each loop:
  call Pagerank (ConvertToOneFile)
  call ConvertToOneFile (GraphFileThisRound, GraphFileLastRound)
  call CalculateSquare(ConvertOutputFile)
  NumOfIterations=NumOfIterations+1
  if square <= ThresHold
  exit loop
  else    
  continue

call GraphEdgesCount to get the number of nodes, the number of edges, average/max/min out-degree of nodes.
call SortPageRank to get the top 10 <page, value> pairs
System.out.printLn(NumOfIterations)



#input graph format: 
page neighbor_set

#our defined graph format:
page\t page_rank_value num_of_neighbors neighbor_set


LinkGraph:
Input: input graph format
Output: defined graph format with pagerank value initialized as 1.0

Pagerank: 
Input: defined graph format
Output: defined graph format

ConvertToOneFile:
Input: graph_this_iteration and graph_last_iteration
Output: In each row: <page> <page_rank_value_this_iteration> <page_rank_value_last_iteration>

CalculateSquare:
Input: Output of ConvertToOneFile
Output: Standard deviation of pagerank values of this iteration with last iteration

GraphEdgesCount:
Input: input graph format
Output: number of nodes, edges, average/min/max out-degree

Sort: 
Input: graph of the last iteration
Output: sorted <page, value> pairs of top 10 in descending order
