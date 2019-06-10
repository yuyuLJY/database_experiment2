package cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import countWord.countword;
import countWord.countword.WordCountMapper1;
import countWord.countword.WordCountReducer1;

public class kmean {
	
	public static class ClusterMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			int size = info.getListLength();//聚簇的点
			String[] splitResult = value.toString().split(",");//特殊字符
			if(size==0) {//初始阶段，需要选出clusterNumber个点
				String newCluster = getRecordString(splitResult);
				info.addClusterList(newCluster);
				//归属关系
			}else {//
				
			}
			
			//求出某个点属于哪个簇点key
			
			//求出value
			
			System.out.println(value.toString());
			context.write(new Text(splitResult[10]), value);//<"doctor",整条信息>
		}
	}
	
	public static class ClusterReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			ArrayList<String> infoList = new  ArrayList<String>();
			System.out.printf("key:%s\n",key.toString());
			for(Text value:values) {
				//value = transformTextToUTF8(value,"GBK");
				infoList.add(value.toString());
			}
			//进行抽样
			int personSum = infoList.size();
			int sampleSum = (int) Math.floor(personSum*0.1);//ceil不小于他的最小整数
			System.out.printf("sampleSum:%d\n",sampleSum);
			int segments = personSum/sampleSum;
			//按照系统抽样的方法，抽取指定的人数
			for(int i = 0;i<infoList.size();i = i+segments) {
				System.out.println(infoList.get(i));
				context.write(new Text(infoList.get(i)),new Text());
			}
			
			//某一职业抽样完毕，进行清空列表
			infoList.clear();
		}
	}
	
	//组成key的字符串
	public static String getRecordString(String[] splitResult) {
		String s = splitResult[1];
		for(int i = 2;i<splitResult.length;i++) {
			s = s + ","+splitResult[i];
		}
		return s;
	}
	
	
	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException{
		Job job = new Job(Main.config(),"step1");
		
        String input = path.get("Step1Input");
        String output = path.get("Step1Output");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setJarByClass(kmean.class);
		job.setMapperClass(ClusterMapper.class);  //输入  
		job.setReducerClass(ClusterReducer.class);   //输入
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
	}
}
