import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Grep extends Configured implements Tool {
	
	private static String searchExpression = "MapReduce";
	
	public static class Map
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String[] id = value.toString().split(":");
    	
    	Pattern p = Pattern.compile(searchExpression);
    	Matcher m = p.matcher(value.toString());
    	if (m.find() == true) {
    		word.set(id[0].trim());
    		context.write(word, null);
    	}
    }
  }

  @Override
  public int run(String[] args) throws Exception
  {
	    Configuration conf = getConf();
	    
	    Job job = Job.getInstance(conf, "grep");
	    job.setJarByClass(Grep.class);
	    job.setMapperClass(Map.class);
	    job.setNumReduceTasks(0);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
	    FileOutputFormat.setOutputPath(job, new Path(args[1]+timeStamp));

	    searchExpression = args[2].toString();
	    
	    return job.waitForCompletion(true)?0:1;
  }
	public static void main(String[] args) throws Exception 
	{
		ToolRunner.run(new Configuration(),new Grep(), args);
	}
}