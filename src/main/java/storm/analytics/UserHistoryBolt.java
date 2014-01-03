package storm.analytics;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * User: domenicosolazzo
 */
public class UserHistoryBolt extends BaseRichBolt {
    public static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private String host;
    private int port;
    private Jedis jedis;

    private HashMap<String, Set<String>> userNavigatedItems = new HashMap<String, Set<String>>();


    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.host = conf.get("redis-host").toString();
        this.port = Integer.valueOf(conf.get("redis-port").toString());
        reconnect();
    }

    private void reconnect(){
        this.jedis = new Jedis(host, port);
    }

    @Override
    public void execute(Tuple input) {
        String user = (String)input.getValue(0);
        String product = (String)input.getValue(1);
        String category = (String)input.getValue(2);

        String prodKey = product+":"+ category;

        Set<String> productNavigated = getUserNavigatedHistory(user);

        // If user previously navigated this item, ignore it
        if(!productNavigated.contains(product)){
            // Update product items
            for(String other: productNavigated){
                String[] ot = other.split(":");
                String prod = ot[0];
                String cat = ot[1];
                collector.emit(new Values(product, cat));
                collector.emit(new Values(prod, category));

            }
            AddProductToHistory(user, prodKey);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("product", "categ"));
    }

    private void AddProductToHistory(String user, String product){
        Set<String> userHistory = getUserNavigatedHistory(user);
        userHistory.add(product);
        jedis.sadd(buildKey(user), product);
    }

    private Set<String> getUserNavigatedHistory(String user){
        Set<String> userHistory = userNavigatedItems.get(user);
        if(userHistory == null){
            userHistory = jedis.smembers(buildKey(user));
            if(userHistory == null){
                userHistory = new HashSet<String>();
            }
            userNavigatedItems.put(user, userHistory);
        }
        return userHistory;
    }

    private String buildKey(String user){
        return "history:" + user;
    }
}
