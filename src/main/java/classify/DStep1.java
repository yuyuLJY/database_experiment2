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

public class DStep1 {
	public static class Step1Mapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			//String[] splitKey = value.toString().split("	");
			//String[] splitResult = splitKey[1].split(",");
			System.out.println(value.toString());
			if(Dinfo.count<100) {//train
				Dinfo.trainList.add(value.toString());
			}
			if(Dinfo.count>=100 && Dinfo.count<120) {//test
				Dinfo.testList.add(value.toString());
			}
			if(Dinfo.count>120) {
				System.out.println("train的长度："+Dinfo.trainList.size());
				System.out.println("test的长度："+Dinfo.testList.size());
				//把内容写进TXT
				writeTotxt(Dinfo.trainList,"small_classify_train");
				writeTotxt(Dinfo.testList,"small_classify_test");
				System.exit(1);
			}
			Dinfo.setCount(Dinfo.count+1);//增加条数
			context.write(new Text("0"),value);//
		}
	}
	
	public static void writeTotxt(ArrayList<String> list,String filename) {
		//写进txt
        String outurl="E:/A大三下/大数据分析/实验/实验2/classifyData/"+filename+".txt";
        File outfile=new File(outurl);
        try {
            //如果写入的文件不存在创建新文件
            if(!outfile.exists()){
                outfile.createNewFile();
            }
            //文件的输入流读取文件
            FileOutputStream out=new FileOutputStream(outfile);
            //写文件
            BufferedWriter write=new BufferedWriter(new OutputStreamWriter(out));
            
            String temp="";
            
            for(String s :list){
                //写入文件
                write.write(s+"\r\n");
                //System.out.println("写入"+s);
            }
            write.close();
            out.close();
        } catch (Exception e) {
            // TODO 自动生成的 catch 块
            e.printStackTrace();
        }
	}
	
	public static class Step1Reducer extends Reducer<Text,Text,Text,Text> {
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
	
	static void writeToTxt() {
		
	}
	
	
	public static void run(String input,String output) throws IOException, ClassNotFoundException, InterruptedException{
		Job job = new Job(DMain.config(),"step3");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setJarByClass(DStep1.class);
		job.setMapperClass(Step1Mapper.class);  //输入  
		job.setReducerClass(Step1Reducer.class);   //输入
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		//System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
		job.waitForCompletion(true);
	}
}
