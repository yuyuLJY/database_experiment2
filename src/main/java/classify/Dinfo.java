package classify;

import java.util.ArrayList;

public class Dinfo {
	static ArrayList<String[]> everyTestList = new ArrayList<String[]>();//记录每一个要判别的属性信息
	static ArrayList<String> testAnswerList = new ArrayList<String>();
	static ArrayList<String> trainList = new ArrayList<String>();//Step1:存放训练集
	static ArrayList<String> testList = new ArrayList<String>();//Step1:存放测试集
	static String currentTest[] = new String[19];//当前的测试的属性（需要作出分类的test语句）
	static double NoCount[] = new double [19];//No类型下的分类
	static double YesCount[] = new double [19];
	static double NoNum=0;
	static double YesNum=0;
	static int trainNum = 100;//训练数据的大小
	static int testNum = 20;//训练数据的大小
	static double classifyJudeRate[] = new double[2];//因为仅有2类
	static int count = 0;
	public static void setCount(int num) {
		count = num;
	}
	
	public static void setCurrentTest(String s[]) {
		currentTest = s;
	}
	
	public static void setAttribteCount(String s,int i) {
		if(s.equals("N")) {
			NoCount[i]=NoCount[i]+1;
		}else {
			YesCount[i]=YesCount[i]+1;
		}
	}
	
	public static void addAllClassifyNum(String s) {
		if(s.equals("N")) {
			NoNum=NoNum+1;
		}else {
			YesNum=YesNum+1;
		}
	}
	
	public static void setClassifyJudeRate(String s,double rate) {
		if(s.equals("N")) {
			classifyJudeRate[0]=rate;
		}else {
			classifyJudeRate[1]=rate;
		}
	}
}
