import java.util.ArrayList;
import java.util.HashMap;

public class nsSum implements Runnable {

	
	 public int start ;
	 public int end;
	 public ArrayList<String> weatherData = new ArrayList<String>();
	 public HashMap<String,Double> station_temp = new HashMap<String,Double>();
	 public HashMap<String,Integer> station_count = new HashMap<String,Integer>();
	 public Boolean fibonacciFlag=false;
	
	 
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
	  * @param fibonacciFlag: flag that indicates if the delay of fibonacci(17) needs to be introduced
	  *               
	  */
	 
	 public nsSum(int start , int end , ArrayList<String> weatherData, HashMap<String,Double> temperatureRecords, HashMap<String,Integer> stationRecords , Boolean fibonacciFlag){
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
		station_temp.clear();
		station_count.clear();
		//logic to process lines from the subset of the array indicated by start and end parameters
		String line = "";
		for(int i = start; i<=end; i++){
			line = weatherData.get(i);
			//splitting the line by ',' to extract fields since the fields in line are delimited by ','
			String[] split_line = line.split(",");
			//stationId is the first field in the line
			String stationId = split_line[0].trim();
			//record Type is the third field in the line
			String recordType = split_line[2].trim();
			//Logic to ignore the records that are not TMAX
			if(recordType.equalsIgnoreCase("TMAX")){
				//the fourth field indicates indicates the TMAX value
				double maxTemp = Double.parseDouble(split_line[3].trim());
				
				//logic to update hash maps if the stationId is already present in it
				if(station_temp.containsKey(stationId)){
					   if(fibonacciFlag){
						  @SuppressWarnings("unused")
						  long c = fibonacci(17);
					   }
					   station_temp.put(stationId,(Double.parseDouble(station_temp.get(stationId).toString())+maxTemp));
					   if(station_count.containsKey(stationId)){
					   station_count.put(stationId,(Integer.parseInt(station_count.get(stationId).toString())+1));}
					   else{
						   station_count.put(stationId, 1);
					   }
					}
					else{
						//logic to update data structures if stationId is not present in hashmaps
						station_temp.put(stationId, maxTemp);
						if(!(station_count.containsKey(stationId))){
							station_count.put(stationId, 1);
							}
					}
			}
		}
	
	
	}

}
