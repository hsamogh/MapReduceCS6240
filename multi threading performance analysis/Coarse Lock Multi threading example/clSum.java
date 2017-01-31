/***
 * 
 * This class calculates the sum of all TMAX records and stores it in a hash map
 * Called from main class of cl.java
 * 
 */


import java.util.ArrayList;
import java.util.HashMap;

public class clSum implements Runnable {

	
	 int start ; //indicates start of array 
	 int end;  //indicates end of array
	 boolean fibonacciFlag; //flag to indicate if fib(17) delay is to be introduced
	 ArrayList<String> weatherData = new ArrayList<String>(); //array containing weather data from different stations
	 HashMap<String,Double> station_temp = new HashMap<String,Double>(); //hashmap containing running sum of all station Id's .key : stationId , value : running sum
	 HashMap<String,Integer> station_count = new HashMap<String,Integer>();// hashmap containing total number of TMAX records for each station Id. key : stationId , value : total
	
	 
	 /***
	  * Constructor to initialize object 
	  * 
	  * @param start : Indicates start index of the array from where records are to be read
	  * @param end : Indicates end index of the array where the reading needs to stop
	  * @param weatherData : An array that contains weather recordings for different station Id's
	  * @param temperatureRecords : hashMap containing stationId as key and sum of temperatures 
	  *                             recorded as value
	  * @param stationRecords: hashMap containing stationId as key and count of recorded values
	  *                        for that stationId
	  * @param fibonacciFlag: Indicates if the fibonacci(17) delay needs to be introduced . delay is 
	  *                       introduced if flag is true. 
	  */
	 
	 
	 public clSum(int start , int end , ArrayList<String> weatherData, HashMap<String,Double> temperatureRecords, HashMap<String,Integer> stationRecords , boolean fibonacciFlag){
		 this.start=start;
		 this.end=end;
		 this.weatherData = weatherData;
		 station_temp = temperatureRecords;
		 station_count = stationRecords;
		 this.fibonacciFlag=fibonacciFlag;
		 
	 }
	
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
	 
	@Override
	public void run() {
		
		//reading input from array from start position to end. 
		String line = "";
		for(int i = start; i<=end; i++){
			line = weatherData.get(i);
			//splitting line in a csv file with ',' as delimiter
			String[] split_line = line.split(",");
			//the first field in the csv file is a station Id.
			String stationId = split_line[0].trim();
			//the second third field in csv file is a record Type 
			String recordType = split_line[2].trim();
			//logic to ignore all records other than TMAX
			if(recordType.equalsIgnoreCase("TMAX")){
				double maxTemp = Double.parseDouble(split_line[3].trim());
				//locking station_temp and station_count data structures
				synchronized(station_temp){
					synchronized(station_count){
						//updating the data structures if stationId is already present
						if(station_temp.containsKey(stationId)){
							  if(fibonacciFlag){
								  long c = fibonacci(17);
							  }
							   station_temp.put(stationId,(Double.parseDouble(station_temp.get(stationId).toString())+maxTemp));
							   if(station_count.containsKey(stationId)){
								   if(fibonacciFlag){
									   long c = fibonacci(17);
								   } 
							   station_count.put(stationId,(Integer.parseInt(station_count.get(stationId).toString())+1));}
							   else{
								   station_count.put(stationId, 1);
							   }
							}
							else{
								//updating data structures if stationId is not present
								station_temp.put(stationId, maxTemp);
								if(!(station_count.containsKey(stationId))){
									station_count.put(stationId, 1);
									}
							}

					}
					
				}
				
			}
		}
	
	
	}

}
