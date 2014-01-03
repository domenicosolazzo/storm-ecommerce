package storm.analytics;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import redis.clients.jedis.Jedis;
import storm.analytics.utilities.NavigationEntry;

import java.util.HashMap;
import java.util.Map;

/**
 * User: domenicosolazzo
 */
public class UsersNavigationSpout extends BaseRichSpout {

    public static final long serialVersionUID = 1L;

    private Jedis jedis;
    private String host;
    private int port;
    private SpoutOutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user", "otherdata"));

    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        this.host = conf.get("redis-host").toString();
        this.port = Integer.valueOf(conf.get("redis-port").toString());

        // Reconnect to Redis
        reconnect();
    }

    private void reconnect(){
        this.jedis = new Jedis(this.host, this.port);
    }

    @Override
    public void nextTuple() {
        String content = jedis.rpop("navigation");
        if(content == null){
            try{
                Thread.sleep(3000);
            }catch(InterruptedException ex ){

            }
        }else{
            JSONObject obj = (JSONObject) JSONValue.parse(content);
            String user = obj.get("user").toString();
            String product = obj.get("product").toString();
            String type = obj.get("type").toString();
            HashMap<String, String> map = new HashMap<String, String>();
            map.put("product", product);
            NavigationEntry entry = new NavigationEntry(user, type, map);
            collector.emit(new Values(user, entry));
        }
    }
}
