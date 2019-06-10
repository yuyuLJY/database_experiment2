package countWord;

import java.io.IOException; 
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper  extends Mapper<Object, Text, Text, IntWritable>{ 
	IntWritable one = new IntWritable(1);  //定义输出值始终是1
	Text word = new Text();   //定义输出形式key的形式
	public void map(Object key, Text value, Context context)throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());//输入值是Text，需要转换
		while (itr.hasMoreTokens()){      
			word.set(itr.nextToken()); //把输出的可以保存起来
			//System.out.printf("%d\n",one);
			context.write(word, one); //以形式<word,1>保存  
		}  
    }
}

