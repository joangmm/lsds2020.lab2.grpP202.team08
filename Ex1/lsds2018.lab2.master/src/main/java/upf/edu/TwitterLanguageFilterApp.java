package upf.edu;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import upf.edu.parser.SimplifiedTweet;

public class TwitterLanguageFilterApp {
    public static void main( String[] args ) throws IOException, Exception { 
    	long start = System.currentTimeMillis();	//time init
    	
    	List<String> arguments= Arrays.asList(args);
    		//We read the inputs from the command line
    	//String input = arguments.get(0);
    	String lang = arguments.get(0);
    	String output = arguments.get(1);
    	
    	//We create the spark as defined in the slides
    	SparkConf conf = new SparkConf().setAppName("Your App");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	for(String input: arguments.subList(2, arguments.size())) {       
	    	JavaRDD<String> lines = sc.textFile(input);
	    	//We read the lines and store them into optional simplified Tweets as in Lab 1
	    	JavaRDD<Optional<SimplifiedTweet>> optionals = lines.map(A -> SimplifiedTweet.fromJson(A));
	    	//We then erase the optional
	    	JavaRDD<SimplifiedTweet>tweets = optionals.filter(B -> B.isPresent()).map(C->C.get());
	    	JavaRDD<SimplifiedTweet> result = tweets.filter(ST -> lang.equals(ST.getLanguage()));
	    	
                
	    	result.saveAsTextFile(output + "/" + input);
    	}
    	
    	long finish = System.currentTimeMillis();	//time end
     	long timeElapsed = finish - start;
    	System.out.println("Time Elapsed: " + timeElapsed + "ms");
    }
}
