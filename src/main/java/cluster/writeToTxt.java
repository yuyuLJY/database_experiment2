package cluster;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class writeToTxt {
	public static void main(String[] args) throws IOException {
		//把前10000个点读出来，然后按照每行一个写进txt
		//从python端读出点，放进列表
		//读出信息源正确的数量
		ArrayList<String> list = new ArrayList<String>();
		Configuration conf=Main.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.132:9000");
		FileSystem fs = FileSystem.get(conf);
		
		Path pathCorrect = new Path("hdfs://192.168.126.132:9000/BigDataAnaly/experiment2/kmeanResult/Last/part-r-00000");
		if (fs.exists(pathCorrect)) {
			System.out.println("Exists!");
			try {
				//此为hadoop读取数据类型
				FSDataInputStream is = fs.open(pathCorrect);
				InputStreamReader inputStreamReader=new InputStreamReader(is,"utf-8");
                String line=null;
                //把数据读入到缓冲区中
                BufferedReader reader = new BufferedReader(inputStreamReader);
                //从缓冲区中读取数据
                int count=0;
                while(((line=reader.readLine())!=null )){
                	String[] split = line.split("\t");
                	if(Integer.valueOf(split[0])<20000) {
                		//写到txt文件里边
                		list.add(split[1]);
                		System.out.println(line);
                	}
                    count++;
                }
			} catch (Exception e) {
				System.out.println(e);
			}
		}
		else {
			System.out.println("不存在");
		}
		
		//写进txt
		System.out.println("长度："+list.size());
        String outurl="E:/A大三下/大数据分析/实验/实验2/kmeanData/sort.txt";
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
}
