package cluster;

import java.util.ArrayList;

public class info {
	static ArrayList<String> clusterList = new ArrayList<String>();
	static int clusterNumber = 3;
	static int countOutput = 0;//输出10000个结果
	static int changeNum= 0;
	static boolean isStop = false;
	public info(ArrayList<String> clusterList) {
		this.clusterList = clusterList;
	}
	
	public static int getListLength() {
		return clusterList.size();
	}

	//添加簇的中心点
	public static void addClusterList(String s) {
		clusterList.add(s);
	}
	
	public static void setCount(int a) {
		countOutput = a;
	}
	
	public static void setChangeCount(int a) {
		changeNum = a;
	}

}
