package upf.edu;

import upf.edu.filter.FileLanguageFilter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class TwitterFilter {
    public static void main( String[] args ) throws IOException, Exception {    // throws??
        
    	long start = System.currentTimeMillis();	//time init
   	    	
    	List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        String outputFile = argsList.get(1);
        String bucket = argsList.get(2);
        System.out.println("Language: " + language + ". Output file: " + outputFile + ". Destination bucket: " + bucket);
        
        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Twitter Filter");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
           
        
        for(String inputFile: argsList.subList(3, argsList.size())) {                 
            System.out.println("Processing: " + inputFile);
            
            final FileLanguageFilter filter = new FileLanguageFilter(inputFile, outputFile, sparkContext);
            filter.filterLanguage(language);
        }

        
        long finish = System.currentTimeMillis();	//time end
    	long timeElapsed = finish - start;
    	System.out.println("Time Elapsed: " + timeElapsed + "ms");
    }
}

 
