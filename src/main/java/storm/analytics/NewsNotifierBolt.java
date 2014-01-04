package storm.analytics;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.mortbay.jetty.HttpContent;

import java.util.Map;

/**
 * User: domenicosolazzo
 */
public class NewsNotifierBolt extends BaseRichBolt {
    public static final long serialVersionUID = 1L;
    String webserver;
    HttpClient client;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.webserver = (String)conf.get("webserver");
        reconnect();
    }

    private void reconnect(){
        this.client = new DefaultHttpClient();
    }
    @Override
    public void execute(Tuple input) {
        String product = input.getString(0);
        String categ = input.getString(1);
        Integer visits = input.getInteger(2);

        String content = "{\"product\":" + product + "," +
                "\"categ\":" + categ + "," +
                "\"visits\":" + visits +
                "}";
        HttpPost post = new HttpPost(webserver);
        try{
            post.setEntity(new StringEntity(content));
            HttpResponse response = client.execute(post);
            EntityUtils.consume(response.getEntity());
        }catch(Exception e){
            e.printStackTrace();
            reconnect();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
