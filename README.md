# MapReduceCS6240
Consists of assignments for the course CS6240 - Parallel Data Processing Using Map Reduce

#multi threading performance evaluation
-----------------------------------------

This assignment evaluates the performance of various multi-threading approaches against the sequential approach. 
In this assignment we are using the data from 

ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/

This data consists of weather readings from multiple weather stations . the format and description of each fields is given in 
the following link

ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/readme.txt

In this assignment , we are evaluating various multithreading methods by calculating average TMAX value from file 1912.csv
located at ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/ 

Following is the description of the contents of folder

a) no-lock-multithreading-example : Consits of code where I have implemented the multi-threaded without having any locking
   mechanisms on data structure. though this produces fastest result, the results are not consistent and problems that we would
   generally expect to find with concurrency can be seen in this code.The main class is in nl.java. This class calls nlSum.java.
   In order to see the effects of this multi-threading approach on large datasets, I have provided an option to execute 
   fibonacci(17) that simulates the effect of large data set by introducing delays between updates on data set
   
   The main function takes three input parameters :
   
   filePath : absolute file path of 1912.csv.
   fibonacciFlag : if set to true , fibonacci(17) is executed to simulate effect of large data set . If set to false ,
                   fibonacci(17) is not executed.
   number of cores : Number of cores we want our code to run on. If the number of cores provided is more than the available 
                     cores , then the system overrides the value provided and take the maximum available cores. If cores 
                     available in our machine is more than the cores specified as input, the input provided by users is taken
                     into consideration
   
b) Coarse lock multi-threading example : Consits of code where I have implemented the multi-threaded with coarse lock
   as locking mechanisms on data structure.In this code , we lock the whole data structure when a thread is trying to update a
   value in data structure.The main class is in cl.java. This class calls clSum.java
   
   In order to see the effects of this multi-threading approach on large datasets, I have provided an option to execute 
   fibonacci(17) that simulates the effect of large data set by introducing delays between updates on data set
   
   The main function takes three input parameters :
   
   filePath : absolute file path of 1912.csv.
   fibonacciFlag : if set to true , fibonacci(17) is executed to simulate effect of large data set . If set to false ,
                   fibonacci(17) is not executed.
   number of cores : Number of cores we want our code to run on. If the number of cores provided is more than the available 
                     cores , then the system overrides the value provided and take the maximum available cores. If cores 
                     available in our machine is more than the cores specified as input, the input provided by users is taken
                     into consideration
   
c) Fine Lock multi-threading example: Consits of code where I have implemented the multi-threaded with fine lock
   as locking mechanisms on data structure.In this code , we lock only the variable beign updated by the thread instead of 
   the whole data structure.The main class is in fl.java. This class calls flSum.java
   
   In order to see the effects of this multi-threading approach on large datasets, I have provided an option to execute 
   fibonacci(17) that simulates the effect of large data set by introducing delays between updates on data set
   
   The main function takes three input parameters :
   
   filePath : absolute file path of 1912.csv.
   fibonacciFlag : if set to true , fibonacci(17) is executed to simulate effect of large data set . If set to false ,
                   fibonacci(17) is not executed.
   number of cores : Number of cores we want our code to run on. If the number of cores provided is more than the available 
                     cores , then the system overrides the value provided and take the maximum available cores. If cores 
                     available in our machine is more than the cores specified as input, the input provided by users is taken
                     into consideration
   
d) No Sharing multi-threading example: Consits of code where I have implemented the multi-threaded with no sharing mechanisms
   .The threads have their own data structures . Once all threads are executed successfully, the individual data structures
   are merged sequentially to form a single data structure .The main class is in noSharing.java. This class calls nsSum.java
   
   In order to see the effects of this multi-threading approach on large datasets, I have provided an option to execute 
   fibonacci(17) that simulates the effect of large data set by introducing delays between updates on data set
   
   The main function takes three input parameters :
   
   filePath : absolute file path of 1912.csv.
   fibonacciFlag : if set to true , fibonacci(17) is executed to simulate effect of large data set . If set to false ,
                   fibonacci(17) is not executed.
   number of cores : Number of cores we want our code to run on. If the number of cores provided is more than the available 
                     cores , then the system overrides the value provided and take the maximum available cores. If cores 
                     available in our machine is more than the cores specified as input, the input provided by users is taken
                     into consideration
                     
  e)Sequential Code : Sequential Code consists of sequential execution of calculating average TMAX values from the file per 
                      station-id. 
   
   The main function takes three input parameters :
   
   filePath : absolute file path of 1912.csv.
   fibonacciFlag : if set to true , fibonacci(17) is executed to simulate effect of large data set . If set to false ,
                   fibonacci(17) is not executed.
   number of cores : Number of cores we want our code to run on. If the number of cores provided is more than the available 
                     cores , then the system overrides the value provided and take the maximum available cores. If cores 
                     available in our machine is more than the cores specified as input, the input provided by users is taken
                     into consideration
                     
   f) Word Count : Consists of details of execution of sample word count program available on 
      https://wiki.apache.org/hadoop/WordCount on local and cloud(AWS)
      
   g) Report : Contains details of findings of executing different locking mechanisms for 10 runs and executing word count in 
               local and cloud(AWS)
                                       



