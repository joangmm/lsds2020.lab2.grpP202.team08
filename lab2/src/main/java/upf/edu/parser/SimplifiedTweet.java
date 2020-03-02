package upf.edu.parser;

import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;


import java.util.Optional;

public class SimplifiedTweet {

  private static JsonParser parser = new JsonParser();

  private final long tweetId;			  // the id of the tweet ('id')
  private final String text;  		      // the content of the tweet ('text')
  private final long userId;			  // the user id ('user->id')
  private final String userName;		  // the user name ('user'->'name')
  private final String language;          // the language of a tweet ('lang')
  private final long timestampMs;		  // seconds from epoch ('timestamp_ms')

  public SimplifiedTweet(long tweetId, String text, long userId, String userName,
                         String language, long timestampMs) {

    // PLACE YOUR CODE HERE!
    this.tweetId = tweetId;
    this.text = text;
    this.userId = userId;
    this.timestampMs = timestampMs;
    this.userName = userName;
    this.language = language;

  }
  
  public String getLanguage(){
      return this.language;
  }

  /**
   * Returns a {@link SimplifiedTweet} from a JSON String.
   * If parsing fails, for any reason, return an {@link Optional#empty()}
   *
   * @param jsonStr
   * @return an {@link Optional} of a {@link SimplifiedTweet}
   */
  @SuppressWarnings("deprecation")
public static Optional<SimplifiedTweet> fromJson(String jsonStr) {
    // PLACE YOUR CODE HERE!
   
	
	JsonElement je;
	JsonObject jo;
	JsonObject user;
	
	try {
		je = parser.parse(jsonStr);
		jo = je.getAsJsonObject();		
		user = (jo.get("user")).getAsJsonObject();
	
	}
	catch(Exception e) {
		return Optional.empty();
	}
		
		
	long tweetId, userId, timestampMs;			
	String text, userName, language;    
	Optional <SimplifiedTweet> tw;
	if(jo.has("id") && jo.has("text") && jo.has("timestamp_ms") && jo.has("lang")
			&& user.has("id") && user.has("name")){
		tweetId = jo.get("id").getAsLong();
	    text = jo.get("text").getAsString();
	    userId = user.get("id").getAsLong();
	    timestampMs = jo.get("timestamp_ms").getAsLong();
	    userName = user.get("name").getAsString();
	    language = jo.get("lang").getAsString();   
	    SimplifiedTweet Tweet = new SimplifiedTweet(tweetId, text, userId, userName, language, timestampMs);
	    tw = Optional.ofNullable(Tweet);
	    return(tw);
	} 
   	
   		
	return(Optional.empty());

  }
}