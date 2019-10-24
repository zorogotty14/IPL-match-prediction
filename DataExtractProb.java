import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataExtractProb {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(),",");
     int c=0;
 	 String batsman="",bowler="";
    int runs=-2;
	if(itr.countTokens()>9){
		String inn=itr.nextToken();
		String ball=itr.nextToken();
		String team=itr.nextToken();
		batsman=itr.nextToken();
		bowler=itr.nextToken();
		String opp=itr.nextToken();
		runs=-1;
	}
	else{
		String inn=itr.nextToken();
		String ball=itr.nextToken();
		String team=itr.nextToken();
		batsman=itr.nextToken();
		bowler=itr.nextToken();
		String opp=itr.nextToken();
		runs=Integer.parseInt(itr.nextToken());
	}

      context.write(new Text(batsman+","+bowler), new IntWritable(runs));
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,Text> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      double one = 0, two=0,three=0,four=0,dot=0,six=0,balls=0, wickets = 0;
      for (IntWritable val : values) {
		int temp=val.get();
		if(temp==0)//Counting the number of 0s, 1s, 2s, 3s, 4s, 6s and Wickets by a batsman against a particular bowler
			dot++;
		else if(temp==1)
			one++;
		else if(temp==2)
			two++;
		else if(temp==3)
			three++;
		else if(temp==4)
			four++;
		else if(temp==6)
			six++;
		else if(temp==-1)
			wickets++;
		balls++;
      }
	  context.write(key, new Text(Double.toString(dot/balls)+","+Double.toString(one/balls)+","+Double.toString(two/balls)+","+Double.toString(three/balls)+","+Double.toString(four/balls)+","+Double.toString(six/balls)+","+Double.toString(wickets/balls)));//Output - Player to Player Probabilities
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator",",");
    Job job = Job.getInstance(conf, "IPL Data Extraction");
    job.setJarByClass(DataExtractProb.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
