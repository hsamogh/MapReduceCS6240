/***
 *  This program is used to calculate average TMAX temperature recorded from various weather stations.
 *  The program reads a csv file from the location specified as command line argument and calculates the
 *  average of the TMAX readings per station-id. This program runs sequentially
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
 *  @Output : The min,max and average run times for 10 computations
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
import java.util.Set;

public class sequence {
	
	 /**
	  * A program to calculate fibonacci number . In this program , this function is used to introduce
	  * delays
	  * 
	  * @param n : an integer which indicates the position of fibonacci number
	  * @return : nth fibonacci number
	  */
	 
	public static  long  fibonacci(int n) {
		if(n<0)
			return (long)-1;
		else if (n==0)
			return(long)0;
		else if (n==1) 
			return(long) 1;
		else
			return (long)(fibonacci(n-1) + fibonacci(n-2));
		}
	
	
	//Entry point of program
	public static void main(String[] args){
		
		//initializing all variables
		String line = "";
		int count=0;
		BufferedReader weatherFile;
		List<String> weatherData = new ArrayList<String>(); //array to store data from file
		HashMap station_temp = new HashMap(); //hash map that stores sum of temperature of each station Id . Key is stationId
		HashMap station_count = new HashMap();//hash map that stores total number of records for each station Id . key is stationId
		HashMap station_average = new HashMap(); // hash map that stores average TMAX value for each station Id. key is stationId
		Boolean fibonacciFlag=false;
		
		//reading command line arguments
		String fileLocation="";
		try{
		 fileLocation = args[0];
		 fibonacciFlag = Boolean.parseBoolean(args[1]);
		}
		catch(Exception e){
			System.out.println("One or more of command line parameters not specified. please refer documentation");
		}
		
		try{
			
			//reading from file to store in local data structure
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
		    
		    for(int executionCounter = 0 ; executionCounter < numberOfExecutions ; executionCounter++){
		    	//clearing all hashmaps
		    	station_average.clear();
		    	station_temp.clear();
		    	station_count.clear();
		    	
		    	double executionTime=0;

		    	//Starting the timer 
		    	long startTime =  System.currentTimeMillis() ;
		    	//Reading each line and creating accumulative data strutures
		    	for(String l : weatherData){
		    		//we read each line from csv file and split it with ',' as delimiter to read individual records.
		    		String[] splitWeatherFields = l.split(",");
		    		//stationId is first field in dataset
		    		String stationId = splitWeatherFields[0].trim();
		    		//recordType is third field in dataset
		    		String recordType = splitWeatherFields[2].trim();
		    		//logic to ignore all records that are not TMAX
		    		if(recordType.equalsIgnoreCase("TMAX")){
		    			double maxTemp = Double.parseDouble(splitWeatherFields[3]);
					
		    			//upadting temperature and count to existing running sum and count respectively
		    			if(station_temp.containsKey(stationId)){
						
		    				if(fibonacciFlag){
		    					long c = fibonacci(17);
		    				}
		    				station_temp.put(stationId,(Double.parseDouble(station_temp.get(stationId).toString())+maxTemp));
		    				station_count.put(stationId,(Integer.parseInt(station_count.get(stationId).toString())+1));
		    			}
		    			else{
		    				//adding default value if station id is not present in hash tables
		    				station_temp.put(stationId, maxTemp);
		    				station_count.put(stationId, 1);
		    			}
		    		}
		    	}
			
		    	//Logic to compute average TMAX value 
		    	Iterator i = station_temp.keySet().iterator();
		    	while(i.hasNext()){

		    		String stationId = i.next().toString();
		    		double temperatureSum = Double.parseDouble(station_temp.get(stationId).toString());

		    		int totalCount = Integer.parseInt(station_count.get(stationId).toString());

		    		double average = temperatureSum/totalCount;

		    		station_average.put(stationId, average);
		    	}
		    	//stopping the timer after processing is done
		    	long endTime = System.currentTimeMillis();
		    	
		    	//Calculating performance meterics
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
		    
		    //timestamp is used as filename of output file
		    Timestamp fileName = new Timestamp(System.currentTimeMillis());
		    PrintWriter pw = new PrintWriter(new FileWriter(fileName.toString()+".txt"));
		    
		    pw.println("The output of sequential program containing average TMAX value recorded per stationId");
		    pw.println("STATION_ID : AVERAGE_TMAX_VALUE");
		    pw.println("*****************************************************");
		    
		    Iterator it = station_average.keySet().iterator();
		    //writing output to file
		    while(it.hasNext()){
		    	String stationId = it.next().toString();
		    	pw.println(stationId+" : "+station_average.get(stationId));
		    }
		    pw.println("*****************************************************");
		    pw.println("Performance Meterics");
		    
		    averageExecutionTime=totalExecutionTime/numberOfExecutions;
			System.out.println("Maximum execution time for "+numberOfExecutions+ " rounds of program execution is "
					+ ""+maxExecutionTime+ " seconds");
			pw.println("Maximum execution time for "+numberOfExecutions+ " rounds of program execution is "
					+ ""+maxExecutionTime+ " seconds");
			System.out.println("Minimum execution time for "+numberOfExecutions+" rounds of program execution is "+
					minExecutionTime+" seconds");
			pw.println("Minimum execution time for "+numberOfExecutions+" rounds of program execution is "+
					minExecutionTime+" seconds");
			System.out.println("Average execution time for "+numberOfExecutions+ " rounds of program execution is "+
					averageExecutionTime+" seconds");
			pw.println("Average execution time for "+numberOfExecutions+ " rounds of program execution is "+
					averageExecutionTime+" seconds");
			pw.close();
		   }
		catch(Exception e){
			System.out.println("error in file processing "+e);
	
		}
		
	}

}
