import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

public class Producer {

public static void main(String[] args) {

ConfigurationBuilder cb = new ConfigurationBuilder();
cb.setDebugEnabled(true)
  .setOAuthConsumerKey("")
  .setOAuthConsumerSecret("")
  .setOAuthAccessToken("")
  .setOAuthAccessTokenSecret("");
Configuration config = cb.build();

TwitterStream twitterStream = new TwitterStreamFactory(config).getInstance();

StatusListener listener = new StatusListener() {

public void onStatus(Status status) {
  System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
}

public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
  System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
}

public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
  System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
}

public void onScrubGeo(long userId, long upToStatusId) {
  System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
}

public void onStallWarning(StallWarning warning) {
  System.out.println("Got stall warning:" + warning);
}

public void onException(Exception ex) {
  ex.printStackTrace();
}
};

twitterStream.addListener(listener);

Map<String, String[]> map = new HashMap<String, String[]>();


map.put("#music", new String[]{"rock", "pop", "jazz"});
map.put("#games", new String[]{"fps", "rpg", "adventure"});

for (String hashtag : map.keySet()) {
  String[] labels = map.get(hashtag);
  twitterStream.filter(new FilterQuery(0, null, labels));
}
}
}