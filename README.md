# MapReduceCS6240
Consists of assignments for the course CS6240 - Parallel Data Processing Using Map Reduce

####  1) Performance Analysis of Multi-Threading methods : Coarse lock , Fine lock , No-shared locks, No-locks
####  2) Performance Analysis of Map Reduce Design Patterns : Combiner, In-Mapper , Secondary Sort 
####  3) Computing Page Rank Using Map Reduce On Wikipedia Dataset
####  4) Computing Page Rank Using Spark
####  5) Computing Page Rank Using Matrix Multiplication
---
# Performance Analysis of multi-threading methods

   This assignment evaluates the performance of various multi-threading approaches against the sequential approach. 
   In this assignment we are using the data from 

   ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/

   This data consists of weather readings from multiple weather stations . the format and description of each fields is given    in the following link

   ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/readme.txt

   In this assignment , we are evaluating various multithreading methods by calculating average TMAX value from file 1912.csv
   located at ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/ .
   
   In order to see the effects of multi-threading on large dataset, We will be adding a function fib(17) which calculates 17th    fibonacci number. This function is present in each of the approaches 

   Following is the description of the contents of folder

   a) no-lock-multithreading-example : Consits of code where I have implemented the multi-threaded without having any locking
   mechanisms on data structure. though this produces fastest result, the results are not consistent and problems that we        would generally expect to find with concurrency can be seen in this code.The main class is in nl.java. This class calls        nlSum.java.
   In order to see the effects of this multi-threading approach on large datasets, I have provided an option to execute 
   fibonacci(17) that simulates the effect of large data set by introducing delays between updates on data set
   
      
   b) Coarse lock multi-threading example : Consits of code where I have implemented the multi-threaded with coarse lock
   as locking mechanisms on data structure.In this code , we lock the whole data structure when a thread is trying to            update a value in data structure.The main class is in cl.java. This class calls clSum.java
   In order to see the effects of this multi-threading approach on large datasets, I have provided an option to execute 
   fibonacci(17) that simulates the effect of large data set by introducing delays between updates on data set.
      
   
   c) Fine Lock multi-threading example: Consits of code where I have implemented the multi-threaded with fine lock
   as locking mechanisms on data structure.In this code , we lock only the variable beign updated by the thread instead of 
   the whole data structure.The main class is in fl.java. This class calls flSum.java
   In order to see the effects of this multi-threading approach on large datasets, I have provided an option to execute 
   fibonacci(17) that simulates the effect of large data set by introducing delays between updates on data set.

   
   d) No Sharing multi-threading example: Consits of code where I have implemented the multi-threaded with no sharing            mechanisms.The threads have their own data structures . Once all threads are executed successfully, the individual data        structures are merged sequentially to form a single data structure .The main class is in noSharing.java. This class            calls nsSum.java
   In order to see the effects of this multi-threading approach on large datasets, I have provided an option to execute 
   fibonacci(17) that simulates the effect of large data set by introducing delays between updates on data set
   
   e)Sequential Code : Sequential Code consists of sequential execution of calculating average TMAX values from the file per      station-id. 
     
                     
   f) Word Count : Consists of details of execution of sample word count program available on 
      https://wiki.apache.org/hadoop/WordCount on local and cloud(AWS)
      
   g) Report : Contains details of findings of executing different locking mechanisms for 10 runs and executing word count in 
   local and cloud(AWS)
   
# Performance Analysis of map-reduce design patterns for analysing weather data
   
   In this assignment, we are analysing performance of the following design patterns on weather dataset
   a) No-Combiner Approach/Simple Map-Reduce 
   b) Combiner Design pattern
   c) In-Mapper Combining Design Pattern
   d) Secondary Sort / Value-to-key conversion design pattern

# Page Rank Using Map-Reduce

   In this assignment, I have computed page rank for data from wikipedia dumps. The data is processed to obtain a graph having the structure PageName : Adjacency List. The details of implementation are present in the report
   
# Page Rank Using Spark
   In this assignment, we are computing page ranks for data from wikipedia dumps using Spark. The details of implementation are present in the report
                                       



