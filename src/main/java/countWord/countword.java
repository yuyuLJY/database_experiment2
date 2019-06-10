package countWord;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException; 
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer;


public class countword {
	 public static void main(String[] args) throws Exception { 
		 Configuration conf = new Configuration();//实例化，从Hadoop的配置文件里读取参数
		 //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();//args命令行参数
		 //if (otherArgs.length != 2) {//判断参数的个数是否正确 
		   //	System.err.println("Usage: wordcount <in> <out>");      
	    	//System.exit(2);    
		 //}
		 Job job = new Job(conf, "wordcount");//job_name = "wordcount"    
		 job.setJarByClass(countword.class);//输入    
		 job.setMapperClass(WordCountMapper1.class);  //输入  
		 job.setReducerClass(WordCountReducer1.class);   //输入
		 job.setOutputKeyClass(Text.class);    //输出
		 job.setOutputValueClass(IntWritable.class); //输出   
		 FileInputFormat.addInputPath(job, new Path("hdfs://192.168.126.132:9000/user/nancy/word.txt"));//输入文件    
		 FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.126.132:9000/user/nancy/wordcount/try6"));//输出文件    
		 System.out.println("2");
		 System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
		 
	 }
	 
	 
	 public static class WordCountMapper1  extends Mapper<Object, Text, Text, IntWritable>{ 
			IntWritable one = new IntWritable(1);  //定义输出值始终是1
			Text word = new Text();   //定义输出形式key的形式
			public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
				System.out.println("Mapper");
				StringTokenizer itr = new StringTokenizer(value.toString());//输入值是Text，需要转换
				while (itr.hasMoreTokens()){      
					word.set(itr.nextToken()); //把输出的可以保存起来
					//System.out.printf("%d\n",one);
					context.write(word, one); //以形式<word,1>保存  
				}  
		    }
	}

	 //输入键类型，输入值类型，输出键类型， 输出值类型
	 public static class WordCountReducer1 extends Reducer<Text,IntWritable,Text,IntWritable> {
	 	  IntWritable result = new IntWritable();
	 	  
	 	  public void reduce(Text key, Iterable<IntWritable> values, Context context)
	 			  throws IOException, InterruptedException {
	 		  System.out.println("Reducer");
	 		  int sum = 0;    
	 		  for (IntWritable val : values) {      
	 			  sum += val.get();    
	 		  }    
	 		  System.out.println(key+" "+sum);
	 		  result.set(sum);    
	 		  context.write(key, result);  
	 	  } 
	 }
	 
}
