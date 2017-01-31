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

public class cl  {
	
	public static HashMap<String,Double> station_average = new HashMap<String,Double>(); // variable to store average of TMAX values per station
	public static HashMap<String,Double> station_temp = new HashMap<String,Double>();//HashMap that stores sum of TMAX values per station id . key:value :: station_id : sumOfTmaxValues
	public static HashMap<String,Integer> station_count = new HashMap<String,Integer>();//HashMap that stores number of TMAX values per station id . key:value :: station_id:totalCount
	public static List<String> weatherData = new ArrayList<String>(); // array to hold data from file
	
	public static boolean fibonacciFlag =false;

	public static void main(String[] args){
		
		/***
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
		 fibonacciFlag = Boolean.parseBoolean(args[1]);
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
		
		
		String line = "";
		BufferedReader weatherFile;
		List<String> weatherData = new ArrayList<String>();
        
		//Reading from file starts
		try{
			weatherFile = new BufferedReader(new FileReader(fileLocation));
			while((line=weatherFile.readLine()) != null){
				weatherData.add(line);
			}
			weatherFile.close();
	   		
			//file reading ends
			
			double maxExecutionTime =0; // variable to store max execution time
			double minExecutionTime =0; // variable to store min execution time
		    double totalExecutionTime=0; //variable to store total Execution time
		    double numberOfExecutions=10; //variable to store number of executions
		    double averageExecutionTime=0; // variable to store average execution time
		    
		    
			for(int executionCounter=0; executionCounter<numberOfExecutions; executionCounter++){
				
				//clearing all hashmaps
				station_average.clear();
		    	station_temp.clear();
		    	station_count.clear();
				
				long startTime =  System.currentTimeMillis(); //setting timer before we begin computation
				double executionTime=0;
				ArrayList<Thread> arrayOfThreads = new ArrayList<Thread>();
				
				//incremental count is used to split output based on number of threads available
				int incrementCount=weatherData.size()/numberOfThreads;
				
				int start = 0;
				
				for(int i=0 ; i<numberOfThreads; i++){
					arrayOfThreads.add(new Thread(new clSum(start,start+incrementCount,(ArrayList<String>)weatherData,station_temp, station_count,fibonacciFlag)));
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
				
				//Logic to compute average temperature
				   // station_temp is a hash map that has stationId -> sum of temperature values
				   // Station_count is a hash map that has stationId -> total count of records for a station Id
					
					Iterator i = station_temp.keySet().iterator();
				
					while(i.hasNext()){
						String stationId = i.next().toString();
						double temperatureSum = Double.parseDouble(station_temp.get(stationId).toString());
						int totalCount = Integer.parseInt(station_count.get(stationId).toString());
						double average = temperatureSum/totalCount;
						//System.out.println("stationiD is"+stationId);
						//System.out.println("Average is"+average);
						station_average.put(stationId,average);
					}
					
					//stopping the timer
					double endTime = System.currentTimeMillis();
					
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
			
			pw.println("The output of coarse lock program containing average TMAX value recorded per stationId");
		    pw.println("STATION_ID : AVERAGE_TMAX_VALUE");
		    pw.println("*****************************************************");
		    
			Iterator it = station_average.keySet().iterator();
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
