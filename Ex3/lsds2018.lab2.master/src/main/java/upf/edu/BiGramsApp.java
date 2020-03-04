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

public class BiGramsApp {
    public static void main( String[] args ) throws IOException, Exception { 
    	List<String> arguments= Arrays.asList(args);
        //We read the inputs from the command line
    	String input = arguments.get(0);
    	String lang = arguments.get(1);
    	
    	//We create the spark as defined in the slides
    	SparkConf conf = new SparkConf().setAppName("BiGramsApp");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
    	JavaRDD<String> lines = sc.textFile(input);
        
    	//We read the lines and store them into optional simplified Tweets as in Lab 1
    	JavaRDD<Optional<ExtendedSimplifiedTweet>> optionals = lines.map(A -> ExtendedSimplifiedTweet.fromJson(A));
    	
        //We then erase the optional
    	JavaRDD<ExtendedSimplifiedTweet>tweets = optionals.filter(B -> B.isPresent()).map(C->C.get());
               
    	JavaRDD<ExtendedSimplifiedTweet> result = tweets.filter(ST -> lang.equals(ST.getLanguage()));
    	JavaRDD<ExtendedSimplifiedTweet> originals = result.filter(D-> !D.isNotOriginal());
    	JavaRDD<String> textFromTweets = originals.map(F-> F.getText());
    	JavaRDD<String> NormalisedtextFromTweets = textFromTweets.map(G-> G.trim().toLowerCase().replace("\n"," "));
        
    	JavaRDD<ArrayList<String>> Bigrams = NormalisedtextFromTweets.map(H->{
            ArrayList<String> bigrams = new ArrayList<String>();
            StringTokenizer itr = new StringTokenizer(H);
            if(itr.countTokens() > 1)
            {
                String s1 = "";
                String s2 = "";
                String s3 = "";
                while (itr.hasMoreTokens())
                {
                    if(s1.isEmpty())
                        s1 = itr.nextToken();
                    s2 = itr.nextToken();
                    s3 = s1 + " " + s2;
                    bigrams.add(s3);
                    s1 = s2;
                    s2 = "";
                }

            }
            return bigrams;  
    	});
    	
        JavaRDD<String> FinalBigrams = Bigrams.flatMap(I -> I.iterator());
      
        JavaPairRDD<String, Integer> counts = FinalBigrams
              .mapToPair(word -> new Tuple2<>(word, 1))
              .reduceByKey((a, b) -> a + b);

    
    	JavaPairRDD<Integer,String> BigramsSwitch = counts.mapToPair(J -> new Tuple2(J._2, J._1));
    	JavaPairRDD<Integer,String> BigramsOrdered = BigramsSwitch.sortByKey(false);
    	
        List<Tuple2<Integer,String>> TopTen = BigramsOrdered.take(10);
    	int i;
    	for(i=0;i<10;i++) {
            System.out.println(TopTen.get(i));   		
    	}
    }
}
