package classify;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.mapred.JobConf;

import cluster.Main;

public class DMain {
	//基于朴素贝叶斯方法的分类
	public static final String HDFS = "hdfs://192.168.126.132:9000";
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		//先尝试读，看是否能读成功
		/*
		String input = HDFS+"/BigDataAnaly/experiment2/Data/SUSY.csv";
		String output = HDFS+"/BigDataAnaly/experiment2/ClassifyResult/1";
		//Step1:5000000条。按照8:2进行分割,写进两个不同的train.txt、test.txt
		DStep1.run(input,output);
		*/
		//Step2:统计Yi的概率,对于每个test，读进内存有20个test，就有20个轮回
		readTestInfo();
		//TODO 重新规划输入输出路径
		int countRound=2;
		String input = HDFS+"/BigDataAnaly/experiment2/Data/small_classify_train.txt";
		String output ="";
		for(int i=0;i<Dinfo.everyTestList.size();i++) {
			Dinfo.setCurrentTest(Dinfo.everyTestList.get(i));//输入当前的测试的内容
			output = HDFS+"/BigDataAnaly/experiment2/ClassifyResult/"+String.valueOf(countRound);
			countRound++;
			DStep2.run(input, output);
			//比较rate，得出答案
			if(Dinfo.classifyJudeRate[0]>=Dinfo.classifyJudeRate[1]) {
				Dinfo.testAnswerList.add("0");
			}else {
				Dinfo.testAnswerList.add("1");
			}
			//把前面的存储信息清空，为下一个Round作准备
			Arrays.fill(Dinfo.currentTest, "0");
			Arrays.fill(Dinfo.NoCount, 0);
			Arrays.fill(Dinfo.YesCount, 0);
			Dinfo.NoNum=0;
			Dinfo.YesNum=0;
			Arrays.fill(Dinfo.classifyJudeRate, 0);
		}
		
		//Step3：计算准确率
		ArrayList<String> judeAnswerList = new ArrayList<String>();
		int correctNum = 0;
		for(int i=0;i<Dinfo.everyTestList.size();i++) {
			String correctAnswer = Dinfo.everyTestList.get(i)[0];
			String judeAnswer = judeAnswerList.get(i);
			if(Integer.valueOf(correctAnswer)==Integer.valueOf(judeAnswer)) {
				correctNum++;
			}
		}
		Double correctRate = (double)correctNum/Dinfo.testNum;
		System.out.println("END");
	}
	
	//读进test的属性信息
	static void readTestInfo() {
		
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
