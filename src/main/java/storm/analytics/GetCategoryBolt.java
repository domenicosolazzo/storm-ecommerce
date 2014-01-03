package storm.analytics;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.analytics.utilities.NavigationEntry;
import storm.analytics.utilities.Product;
import storm.analytics.utilities.ProductsReader;

import java.util.Map;

/**
 * User: domenicosolazzo
 */
public class GetCategoryBolt extends BaseBasicBolt {
    public static final long serialVersionUID = 1L;
    private ProductsReader reader;

    @Override
    public void prepare(Map conf, TopologyContext context) {
        String host = conf.get("redis-host").toString();
        int port = Integer.valueOf(conf.get("redis-port").toString());

        this.reader = new ProductsReader(host, port);
        super.prepare(conf, context);

    }


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        NavigationEntry entry = (NavigationEntry) input.getValue(1);
        if("PRODUCT".equals(entry.getPageType())){
            try{
                String product = (String) entry.getOtherData().get("product");
                Product item = this.reader.readItem(product);
                if( item == null){
                    return;
                }

                String category = item.getCategory();
                collector.emit(new Values(entry.getUserId(), product, category));

            }catch(Exception ex){
                System.err.println("Error processing PRODUCT tuple " + ex);
                ex.printStackTrace();
            }
        }
    }
}
