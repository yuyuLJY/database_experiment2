package cluster;

import java.util.ArrayList;

import org.apache.hadoop.mapred.JobConf;



public class Main {
	public static void main(String[] args) {
		//如果还变化就不停下来
		int count = 0 ;
		ArrayList<String> clusterList = new ArrayList<String>();
		info INFO = new info(clusterList,count);
		while(true) {
			
		}
	}
	
    public static JobConf config() {
        JobConf conf = new JobConf(Main.class);
        conf.setJobName("Recommend");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }
}
