package cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Step3 {
	public static class SortMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitKey = value.toString().split("	");
			String[] splitResult = splitKey[1].split(",");
			context.write(new Text(splitResult[0]),new Text(splitKey[0]));//<"1001",0>
		}
	}
	
	public static class SortReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			for(Text value:values) {
				//System.out.println("count："+info.countOutput);
				//info.countOutput = info.countOutput+1;
				//info.setCount(info.countOutput);
				//if(info.countOutput<10001) {
					context.write(new Text(key),value);//产生的形式：<key,values>
				//}
			}
		}
	}
	
	
	public static void run(String input,String output) throws IOException, ClassNotFoundException, InterruptedException{
		Job job = new Job(Main.config(),"step3");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setJarByClass(Step1.class);
		job.setMapperClass(SortMapper.class);  //输入  
		job.setReducerClass(SortReducer.class);   //输入
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		//System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
		job.waitForCompletion(true);
	}
}
