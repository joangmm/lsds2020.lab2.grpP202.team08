package upf.edu;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import upf.edu.parser.ExtendedSimplifiedTweet;

public class MostRetweeted {
    public static void main( String[] args ) throws IOException, Exception { 
    	List<String> arguments= Arrays.asList(args);
        //We read the inputs from the command line
    	String input = arguments.get(0);
    	String lang = arguments.get(1);
    	
    	//We create the spark as defined in the slides
    	SparkConf conf = new SparkConf().setAppName("MostRetweeted");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	JavaRDD<String> lines = sc.textFile(input);
        
    	//We read the lines and store them into optional simplified Tweets as in Lab 1
    	JavaRDD<Optional<ExtendedSimplifiedTweet>> optionals = lines.map(A -> ExtendedSimplifiedTweet.fromJson(A));
    	
        //We then erase the optional
    	JavaRDD<ExtendedSimplifiedTweet>tweets = optionals.filter(B -> B.isPresent()).map(C->C.get());
               
    	//Count num of retweets x tweet
    	JavaRDD<ExtendedSimplifiedTweet> retweets = tweets.filter(D-> D.isNotOriginal());
         
        //Count most retweeted users
        JavaRDD<Long> masterUserId = retweets.map(F-> F.retweetedUserId());                
        JavaPairRDD<Long, Integer> rtwUsrcounts = masterUserId
              .mapToPair(Id -> new Tuple2<>(Id, 1))
              .reduceByKey((a, b) -> a + b);        
        JavaPairRDD<Integer, Long> rtwUsrcountsSwaped = rtwUsrcounts.mapToPair(J -> new Tuple2(J._2, J._1));
        JavaPairRDD<Integer, Long> tweetUsrCountOrdered = rtwUsrcountsSwaped.sortByKey(false);
        JavaPairRDD<Integer, Long> topTenUsers= tweetUsrCountOrdered.take(10);
        
        //Count most retweeted twits  
        JavaRDD<Long> masterTweetId = retweets.map(F-> F.getRetweetedTweetId()); 
        
        JavaRDD<ExtendedSimplifiedTweet> = tweets.filter( T -> T.getTweetId().equals(retweets.map(F-> F.getRetweetedTweetId())) );
        
        JavaPairRDD<ExtendedSimplifiedTweet, Integer> rtwIdcounts = MasterTweetId
              .mapToPair(Id -> new Tuple2<>(Id, 1))
              .reduceByKey((a, b) -> a + b);        
        JavaPairRDD<Integer, ExtendedSimplifiedTweet> rtwIdcountsSwaped = rtwIdcounts.mapToPair(J -> new Tuple2(J._2, J._1));
        JavaPairRDD<Integer, ExtendedSimplifiedTweet> tweetIdCountOrdered = rtwIdcountsSwaped.sortByKey(false);
       
        
      
        
        
        List<Tuple2<Integer,Long>> TopTen = tweetIdCountOrdered.take(10);
    	int i;
    	for(i=0;i<10;i++) {
            System.out.println(TopTen.get(i));   		
    	}
    }
}
