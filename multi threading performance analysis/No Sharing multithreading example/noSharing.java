/***
 * This program is used to demonstrate use of multi-threading program using coarse lock. This program is
 * part of a assignment used to measure performance of different approaches to calculating average temperature
 * from a file
 * 
 * The program takes the following inputs
 * 
 *  @Inputs : The inputs are to be passed through command line in the following format
 *            filePath fibonacciFlag cores
 *            
 *            filePath : path where we keep 1912.csv file
 *            fibonacciFlag : true if we want to introduce delay of fibonacci(17). else false
 *            cores : number of cores on which we want to do computation. Optional parameter
 *            
 *             Output : The min,max and average run times for 10 computations
 *             
 */


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class noSharing  {
	
	//hashmap to store final averages of TMAX values per station id . key:value :: stationId : averageValue
	public static HashMap<String,Double> station_average = new HashMap<String,Double>();
	
	//hashmap to store running sum of TMAX values per station id . key:value :: stationId : runningSum
	public static HashMap<String,Double> station_temp = new HashMap<String,Double>();
	
	//hashmap to store total TMAX records processed . key:value :: stationId : count
	public static HashMap<String,Integer> station_count = new HashMap<String,Integer>();
	
	//hashmap to store data from file
	public static List<String> weatherData = new ArrayList<String>();
	
	//flag to indicate if delay needs to be introduced
	public static Boolean fibonacciFlag=false;

	public static void main(String[] args){
		
		
		/*
		 * 
		 * Reading command line parameters . 
		 * 
		 * args[0] contains filepath where 1912.csv is present
		 * 
		 * args[1] contains fibonacci flag. If flag is set to true, then fibonacci(17) delay is 
		 * introduced . if the value of args[1] is false , there is no delay introduced.
		 * 
		 * args[2] represents the fibonacci flag value which indicates whether to enforce delay or not
		 * 
		 * args[3] contains number of cores we want to utilize .It is an optional parameter
		 * 
		 */

		String fileLocation="";
		try{
		 fileLocation = args[0];
		 fibonacciFlag= Boolean.parseBoolean(args[1]);
		}
		catch(Exception e){
			System.out.println("One or more of the command line arguments is missing. Please refer"
					+ " readme.txt  file for expected input parameters");
		}
	
		
		// finding number of cores available  
		int cores = Runtime.getRuntime().availableProcessors();
		
		int numberOfThreads = 0;
		
		if(args.length<3){
			numberOfThreads = cores;
		}else{
			if(Integer.parseInt(args[2])>cores){
				numberOfThreads=cores;
			}else{
				numberOfThreads=Integer.parseInt(args[2]);
			}
		}
		
		//Logic to read from file and store in data structure.
		String line = "";
		BufferedReader weatherFile;
		 try{
			weatherFile = new BufferedReader(new FileReader(fileLocation));
			while((line=weatherFile.readLine()) != null){
				weatherData.add(line);
				
			}
			weatherFile.close();
			//file reading completed
			
			
			double maxExecutionTime =0; // variable to store max execution time
			double minExecutionTime =0; // variable to store min execution time
		    double totalExecutionTime=0; //variable to store total Execution time
		    double numberOfExecutions=10; //variable to store number of executions
		    double averageExecutionTime=0; // variable to store average execution time
		    double executionTime =0; //variable which stores execution time of current execution
		    
		    
		    for(int executionCounter=0; executionCounter<10; executionCounter++){
		    	
		    	//Clearing all data structures
		    	station_average.clear();
		    	station_temp.clear();
		    	station_count.clear();
		    	
				//starting timer to calculate execution time
		    	long startTime =  System.currentTimeMillis() ;
				executionTime=0;
				ArrayList<Thread> arrayOfThreads = new ArrayList<Thread>();
				ArrayList<HashMap<String,Double>> station_temp = new ArrayList<HashMap<String,Double>>(); 
				ArrayList<HashMap<String,Integer>> station_count = new ArrayList<HashMap<String,Integer>>(); 
				int incrementCount=weatherData.size()/numberOfThreads;
				int start = 0;
				
				for(int i=0 ; i<numberOfThreads; i++){
					station_temp.add(new HashMap<String,Double>());
					station_count.add(new HashMap<String,Integer>());
					arrayOfThreads.add(new Thread(new nsSum(start,start+incrementCount,(ArrayList<String>)weatherData,station_temp.get(i), station_count.get(i), fibonacciFlag)));
					start+=incrementCount;
				}
				//Starting all threads
				for(int i=0; i<arrayOfThreads.size(); i++){
					arrayOfThreads.get(i).start();
				}
				//Waiting for threads to complete
				for(int i=0; i<arrayOfThreads.size(); i++){
					arrayOfThreads.get(i).join();
				}
				
				
				//Hash Map that is used to merge the intermediate hash maps generated by each thread. The structure is
				//similar to the existing hash maps
				HashMap<String,Double> aggregateHashMapTemp = new HashMap<String,Double>();		
				HashMap<String,Integer> aggregateHashMapCount = new HashMap<String,Integer>();
				
				//Logic to merge intermediate hash maps 
				for(int i =0 ; i<station_temp.size();i++){
					HashMap<String,Double> local_storage_temp = (HashMap<String,Double>)station_temp.get(i);
					HashMap<String,Integer> local_storage_count=station_count.get(i);
					
					Iterator<String> aggregator = local_storage_temp.keySet().iterator();
					
					while(aggregator.hasNext()){
						String stationId = aggregator.next().toString();
						Double totalTemperature = local_storage_temp.get(stationId);
						int countOfRecords = local_storage_count.get(stationId);
						
						if(aggregateHashMapTemp.containsKey(stationId)){
							aggregateHashMapTemp.put(stationId, Double.parseDouble(aggregateHashMapTemp.get(stationId).toString())+totalTemperature);
						}else{
							aggregateHashMapTemp.put(stationId,totalTemperature);
						}
						
						if(aggregateHashMapCount.containsKey(stationId)){
							aggregateHashMapCount.put(stationId, Integer.parseInt(aggregateHashMapCount.get(stationId).toString())+countOfRecords);
						}else{
							aggregateHashMapCount.put(stationId, countOfRecords);
						}
		
					}
				}
				
				//logic to compute average of TMAX values per station Id
				Iterator<String> i = aggregateHashMapTemp.keySet().iterator();
				
				while(i.hasNext()){
					
					String stationId = i.next().toString();
					double temperatureSum = Double.parseDouble(aggregateHashMapTemp.get(stationId).toString());
					int totalCount = Integer.parseInt(aggregateHashMapCount.get(stationId).toString());				
					double average = temperatureSum/totalCount;				
					station_average.put(stationId, average);
				}
				
				//stopping timer that calculates execution time
				long endTime = System.currentTimeMillis();
				
				//calculating performance meterics
				
				executionTime = (double)(endTime-startTime)/1000;	
				totalExecutionTime+=executionTime;
				
				if(executionTime>maxExecutionTime){
					maxExecutionTime=executionTime;
				}
				
				if(executionCounter==1){
					minExecutionTime = executionTime;
				}
				
				if(executionTime < minExecutionTime){
					minExecutionTime=executionTime;
				}
			
		    }
		    
		  //timestamp is used as filename of output
			Timestamp filename = new Timestamp(System.currentTimeMillis());
			
			//Printing data to file
			PrintWriter pw= new PrintWriter(new FileWriter(filename.toString()+".txt"));
			
			pw.println("The output of no-sharing lock program containing average TMAX value recorded per stationId");
		    pw.println("STATION_ID : AVERAGE_TMAX_VALUE");
		    pw.println("*****************************************************");
		    
			Iterator<String> it = station_average.keySet().iterator();
			while(it.hasNext()){
				String stationId = it.next().toString();
				pw.println(stationId + ":" + station_average.get(stationId));
				
			}
			
			pw.println("******************************************************");
			averageExecutionTime=totalExecutionTime/numberOfExecutions;
			System.out.println("Maximum execution time for 10 rounds of program execution is "
					+ ""+maxExecutionTime+ " seconds");
			pw.println("Maximum execution time for 10 rounds of program execution is "
					+ ""+maxExecutionTime+ " seconds");
			System.out.println("Minimum execution time for 10 rounds of program execution is "+
					minExecutionTime+" seconds");
			pw.println("Minimum execution time for 10 rounds of program execution is "+
					minExecutionTime+" seconds");
			System.out.println("Average execution time for 10 rounds of program execution is "+
					averageExecutionTime+" seconds");
			pw.println("Average execution time for 10 rounds of program execution is "+
					averageExecutionTime+" seconds");
			pw.close();

		}
		catch(Exception e){
			System.out.println("error in file processing "+e);
	
		}
		
	} 

}
