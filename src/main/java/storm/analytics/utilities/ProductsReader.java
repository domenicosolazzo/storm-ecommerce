package storm.analytics.utilities;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import redis.clients.jedis.Jedis;

import java.util.logging.Logger;

/**
 * User: domenicosolazzo
 */
public class ProductsReader {
    private Logger log;

    private String redisHost;
    private int redisPort;
    private Jedis jedis;

    public ProductsReader(String redisHost, int redisPort){
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        reconnect();
    }

    private void reconnect(){
        this.jedis = new Jedis(this.redisHost, this.redisPort);
    }

    public Product readItem(String id) throws Exception{
        String content = this.jedis.get(id);
        if(content == null){
            return null;
        }
        Object obj = JSONValue.parse(content);
        JSONObject product = (JSONObject)obj;
        Product i = new Product(
           (Long)product.get("id"),
           (String)product.get("title"),
           (Long)product.get("price"),
           (String)product.get("category")
        );

        return i;

    }
}
