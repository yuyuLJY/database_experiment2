package countWord;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer;

//输入键类型，输入值类型，输出键类型， 输出值类型
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	  IntWritable result = new IntWritable();
	  public void reduce(Text key, Iterable<IntWritable> values, Context context)
			  throws IOException, InterruptedException {
		  int sum = 0;    
		  for (IntWritable val : values) {      
			  sum += val.get();    
		  }    
		  result.set(sum);    
		  context.write(key, result);  
	  } 
}

