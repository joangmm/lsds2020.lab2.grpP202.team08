package upf.edu.parser;


import java.util.Optional;

import com.google.gson.Gson;

import com.google.gson.JsonParser;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;



public class ExtendedSimplifiedTweet {

    private static JsonParser parser = new JsonParser();

    private final long tweetId;			  // the id of the tweet ('id')
    private final String text;  		      // the content of the tweet ('text')
    private final long userId;			  // the user id ('user->id')
    private final String userName;		  // the user name ('user'->'name')
    private final String language;          // the language of a tweet ('lang')
    private final long timestampMs;		  // seconds from epoch ('timestamp_ms')


    private final long followersCount; // the number of followers (’user’->’followers_count’)
    private final boolean isRetweeted; // is it a retweet? (the object ’retweeted_status’ exists?)
    private final Long retweetedUserId; // [if retweeted] (’retweeted_status’->’user’->’id’)
    private final Long retweetedTweetId; // [if retweeted] (’retweeted_status’->’id’)


    public ExtendedSimplifiedTweet(long tweetId, String text, long userId, String userName,
                                   String language, long timestampMs, long followersCount,
                                   boolean isRetweeted, long retweetedUserId, long retweetedTweetId) {

        // PLACE YOUR CODE HERE!
        this.tweetId = tweetId;
        this.text = text;
        this.userId = userId;
        this.timestampMs = timestampMs;
        this.userName = userName;
        this.language = language;

        this.followersCount = followersCount;   // the number of followers (’user’->’followers_count’)
        this.isRetweeted= isRetweeted;          // is it a retweet? (the object ’retweeted_status’ exists?)
        this.retweetedUserId = retweetedUserId; // [if retweeted] (’retweeted_status’->’user’->’id’)
        this.retweetedTweetId = retweetedTweetId;

    }
  
    public String getLanguage(){
        return this.language;
    }
    public String getText(){
        return this.text;
    }
    public Long getRetweetedTweetId(){
        return this.retweetedTweetId;
    }
    public Long getRetweetedUserId(){
        return this.retweetedUserId;
    }
    public Long getTweetId(){
        return this.tweetId;
    }
    public Boolean isNotOriginal(){
        return this.isRetweeted;
    }

    @Override
    public String toString() {
            String OutputTweet =   new Gson().toJson(this);
            return OutputTweet;

    }

  /**
   * Returns a {@link ExtendedSimplifiedTweet} from a JSON String.
   * If parsing fails, for any reason, return an {@link Optional#empty()}
   *
   * @param jsonStr
   * @return an {@link Optional} of a {@link ExtendedSimplifiedTweet}
   */
    public static Optional<ExtendedSimplifiedTweet> fromJson(String jsonStr) {
    // PLACE YOUR CODE HERE!
    
	Object je;
	JsonObject jo;
	JsonObject user;
	JsonObject Retweet;
	JsonObject RT_User;
	Boolean isRetweeted = true;
	Object temp; 
        long tweetId, userId, timestampMs,followersCount ,retweetedUserId = -1, retweetedTweetId = -1;
        String text, userName, language;
    	
	try {
            je = parser.parse(jsonStr);
            jo = (JsonObject) je;
            user = (JsonObject) jo.get("user");
	}
	catch(Exception e) {
            return Optional.empty();
	}
                
          
        if(jo.has("id") && jo.has("text") && jo.has("timestamp_ms") && jo.has("lang")
			&& user.has("id") && user.has("name") && user.has("followers_count")){
            tweetId = jo.get("id").getAsLong();
	    text = jo.get("text").getAsString();
	    userId = user.get("id").getAsLong();
	    timestampMs = jo.get("timestamp_ms").getAsLong();
	    userName = user.get("name").getAsString();
	    language = jo.get("lang").getAsString();   
            followersCount = user.get("followers_count").getAsLong();
            
            if(jo.has("retweeted_status")){
                Retweet = (JsonObject) jo.get("retweeted_status");
                if(Retweet.has("user")){
                    RT_User = (JsonObject) Retweet.get("user");
                    if( RT_User.has("id") && Retweet.has("id")){
                        retweetedUserId = (Long) RT_User.get("id").getAsLong();
                        retweetedTweetId = (Long) Retweet.get("id").getAsLong();
                    }else
                        isRetweeted = false;
                }else
                    isRetweeted = false;
            }                       
            else
                isRetweeted = false;
            
            ExtendedSimplifiedTweet Tweet = new ExtendedSimplifiedTweet(tweetId, text, userId, userName, language, timestampMs, followersCount, isRetweeted,retweetedUserId, retweetedTweetId);
            Optional <ExtendedSimplifiedTweet> OptionalTweet = Optional.ofNullable(Tweet);
            return(OptionalTweet);
        }
        
        	
	
            
    
    
    return(Optional.empty());
    
  }
}