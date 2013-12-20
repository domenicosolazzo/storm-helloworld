package storm.cookbook;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * User: domenicosolazzo
 */
public class HelloWorldTopology {
    public static void main(String[] args) throws Exception{

        /// Create a topology builder
        TopologyBuilder builder = new TopologyBuilder();

        /// Set the spout for the topology
        builder.setSpout("randomHelloWorld", new HelloWorldSpout(), 10);

        /// Set the bolt for the topology. The bolt gets the data from the spout assigned previously
        builder.setBolt("HelloWorldBolt", new HelloWorldBolt(), 2)
                .shuffleGrouping("randomHelloWorld");

        /// Configuration
        Config conf = new Config();
        conf.setDebug(true);

        if(args != null &&  args.length > 0){
            /// Remote storm cluster
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }else{
            /// Local cluster for testing purpose
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());

            Utils.sleep(100);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
