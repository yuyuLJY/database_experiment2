package cluster;

import java.util.ArrayList;

public class info {
	static ArrayList<String> clusterList = new ArrayList<String>();
	static int clusterNumber = 3;
	static int count;
	public info(ArrayList<String> clusterList,int count) {
		this.clusterList = clusterList;
		this.count = count;
	}
	
	public static int getListLength() {
		return clusterList.size();
	}

	//添加簇的中心点
	public static void addClusterList(String s) {
		clusterList.add(s);
	}
	
	public static int getCount() {
		return count;
	}

}
