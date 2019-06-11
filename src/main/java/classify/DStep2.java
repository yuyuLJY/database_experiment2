package classify;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DStep2 {
	public static class Step2Mapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split(",");
			if(splitResult[0].equals("0")) {//NO 0
				Dinfo.addAllClassifyNum("N");//否定的分类计次： +1
				for(int i=1;i<splitResult.length;i++) {//1-17比较18个属性
					if(splitResult[i].equals(Dinfo.currentTest[i])) {
						Dinfo.setAttribteCount("N",i);
					}
				}
			}else {//YES 1
				context.write(new Text("1"),value);//<1,比率> <0,比率>
			}
			//最后一个train的记录也统计完了
			if(Dinfo.YesNum+Dinfo.NoNum==Dinfo.trainNum) {//NO+YES=100
				//计算NO
				//TODO 所有的属性计数都+1，防止出现1*0*0.5
				double NoAttributeCount[] = Dinfo.NoCount; 
				for(int k=1;k<Dinfo.NoCount.length;k++) {
					double rate = NoAttributeCount[k]/Dinfo.NoNum;
					context.write(new Text("0"),new Text(String.valueOf(rate)));//<0,比率>
				}
				//计算Yes
				double YesAttributeCount[] = Dinfo.YesCount; 
				for(int k=1;k<Dinfo.YesCount.length;k++) {
					double rate = YesAttributeCount[k]/Dinfo.YesNum;
					context.write(new Text("1"),new Text(String.valueOf(rate)));//<0,比率>
				}
			}
			System.out.println(value.toString());
			
		}
	}
	
	public static class Step2Reducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			double judgeRate =1;
			for(Text value:values) {
				judgeRate = judgeRate*Double.valueOf(value.toString());
				//context.write(new Text(key),value);//产生的形式：<key,values>
			}
			if(key.toString().equals("0")) {//NO
				judgeRate = judgeRate*(Dinfo.NoNum/Dinfo.trainNum);
				Dinfo.setClassifyJudeRate("N",judgeRate);
			}else {//Yes
				judgeRate = judgeRate*(Dinfo.YesNum/Dinfo.trainNum);
				Dinfo.setClassifyJudeRate("Y",judgeRate);
			}
		}
	}
	
	public static void run(String input,String output) throws IOException, ClassNotFoundException, InterruptedException{
		Job job = new Job(DMain.config(),"step2");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setJarByClass(DStep1.class);
		job.setMapperClass(Step2Mapper.class);  //输入  
		job.setReducerClass(Step2Reducer.class);   //输入
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		//System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
		job.waitForCompletion(true);
	}
}
