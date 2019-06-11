package cluster;

import java.io.IOException;
import java.text.DecimalFormat;
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


public class Step2 {
	public static class ReClusterMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			int size = info.getListLength();//聚簇的点
			String[] splitKey = value.toString().split("	");
			String[] splitResult = splitKey[1].split(",");
			//System.out.println("maper："+value.toString());
			//重新找的簇点：ClusterNo
			String ClusterNo = judgeBelonging(splitResult);
			//判断跟原来是否一样：如果一样，
			if(!ClusterNo.equals(splitKey[0])) {
				info.setChangeCount(info.changeNum+1);//改变的数量
				info.isStop = false;
			}
			//System.out.println("新簇点No:"+ClusterNo+" 旧："+splitKey[0]);
			context.write(new Text(ClusterNo), new Text(splitKey[1]));//<"C1",整条信息>
		}
	}
	
	public static class ReClusterReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			//System.out.printf("key:%s\n",key.toString());
			//重新统计新的点
			DecimalFormat df = new DecimalFormat("#.00");
			double countRecord = 0;
			//创建一个double数组，来存放每个的聚类的平均值,创建多少个数组：key按照","分割的大小
			double sum[] = new double[info.clusterList.get(0).split(",").length];
			double ave[] = new double[info.clusterList.get(0).split(",").length];
			for(Text value:values) {
				//System.out.printf(key+" "+value.toString());
				countRecord++;
				String[] splitResult = value.toString().split(",");//特殊字符
				//System.out.println("长度："+splitResult.length);
				for(int i =1;i<splitResult.length;i++) {
					//测试是不是每次都按照列相加
					if(i==68) {
						//System.out.println("列相加"+Double.valueOf(splitResult[i]));
					}
					sum[i] = sum[i]+Double.valueOf(splitResult[i]);
					sum[i] = Double.valueOf(df.format(sum[i]));
				}
				context.write(new Text(key),value);//产生的形式：<key,values>
			}
			//System.out.println("countRecord:"+countRecord);
			//该类所有的点都遍历完毕
			String cluster = df.format(sum[1]/countRecord);
			for(int i =2;i<ave.length;i++) {
				cluster = cluster +","+df.format(sum[i]/countRecord);
			}
			//产生新的簇点cluster
			//替换掉原来的簇中心
			//int oldCluster = info.clusterList.indexOf(key.toString());
			cluster = "#"+key.toString()+","+cluster;
			System.out.println("新的簇点："+cluster);
			info.clusterList.set(Integer.valueOf(key.toString()), cluster);
		}
	}
	
	//组成key的字符串
	public static String getRecordString(String[] splitResult) {
		String s = "#"+String.valueOf(info.clusterList.size())+","+splitResult[1];
		for(int i = 2;i<splitResult.length;i++) {
			s = s + ","+splitResult[i];
		}
		return s;
	}
	
	public static String judgeBelonging(String[] splitResult){
		/**
		 * 填充map阶段的key的值
		 * @param 需要判定在属于哪个类的点
		 * @return 返回簇点的信息 
		 */
		String key = "";
		//跟每一个簇点都比较
		ArrayList<String> list = info.clusterList;
		double min = 1000000000;
		int position = 0;
		for(int i = 0;i<list.size();i++) {//
			double sum = 0;
			//按照","进行划分
			String cluster[] = list.get(i).split(",");
			//System.out.println("长度"+cluster.length+" "+splitResult.length);
 			for(int j=1;j<cluster.length;j++) {
 				//System.out.println(Double.valueOf(cluster[j])+" "+Double.valueOf(splitResult[j]));
				sum = sum+(Double.valueOf(cluster[j])-Double.valueOf(splitResult[j]))*
						(Double.valueOf(cluster[j])-Double.valueOf(splitResult[j]));
			}
 			//跟一个出点比较完毕
 			if(sum<min) {//跟这个更进
 				position = i;
 				min = sum;
 			}
		}
		return String.valueOf(position);
	}
	
	
	public static void run(String input,String output) throws IOException, ClassNotFoundException, InterruptedException{
		Job job = new Job(Main.config(),"step2");
		
        //String input = path.get("Step1Input");
        //String output = path.get("Step1Output");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setJarByClass(Step1.class);
		job.setMapperClass(ReClusterMapper.class);  //输入  
		job.setReducerClass(ReClusterReducer.class);   //输入
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		//System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
		job.waitForCompletion(true);
	}
	
}
