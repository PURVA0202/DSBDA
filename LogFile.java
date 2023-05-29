package pk1;

import java.io.IOException;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;


public class LogFile {
	
	public static void main(String [] args) throws Exception{
		Configuration c = new Configuration();
		
		String [] files = new GenericOptionsParser(c, args).getRemainingArgs();
		
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		
		Job j = new Job(c, "title");
		j.setJarByClass(LogFile.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		j.setMapperClass(MyMapper.class);
		j.setReducerClass(MyReducer.class);
		
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
		
		
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
			String line = value.toString();
			String [] lines = line.split("\n");
			for(String eachLine: lines){
				String []eachWord = eachLine.split(",");
				String ip = eachWord[1];
				String startTime = eachWord[5].split(" ")[1];
				String endTime = eachWord[7].split(" ")[1];
				
				int h1 = Integer.parseInt(startTime.substring(0, 2));
				int m1 = Integer.parseInt(startTime.substring(3, 5));
				int s1 = Integer.parseInt(startTime.substring(6, 8));
				
				int h2 = Integer.parseInt(endTime.substring(0, 2));
				int m2 = Integer.parseInt(endTime.substring(3, 5));
				int s2 = Integer.parseInt(endTime.substring(6, 8));
				
				int t1 = h1 * 3600 + m1 * 60 + s1;
				int t2 = h2 * 3600 + m2 * 60 + s2;
				
				int diff = t2 - t1;
				
				con.write(new Text(ip), new Text(Integer.toString(diff)));
			}
		}
	}
		
		public static class MyReducer extends Reducer<Text, Text, Text, Text>{
			int maxi = 0;
			String ip = "";
			public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException{
				int totdurationinsec =0;
				for(Text vals: values){
					totdurationinsec += Integer.parseInt(vals.toString());
				}
				
				
				
				String ans = "";
				int hrs = totdurationinsec/3600;
				int min = (totdurationinsec % 3600)/60;
				int sec = (totdurationinsec % 3600)%60;
				ans += hrs + ":" + min + ":" + sec;
				
				if(totdurationinsec > maxi){
					maxi = totdurationinsec;
					ip = key.toString();
					
				}
				con.write(key, new Text(ans));

			}
			
			@Override
			protected void cleanup(Context con) throws IOException, InterruptedException{
				con.write(new Text("max logged in by ip : "), new Text(ip));
			}
		} // myreducer
	

}
