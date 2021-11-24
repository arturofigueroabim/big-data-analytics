import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndex extends Configured implements Tool {

  public static class Map
       extends Mapper<Object, Text, Text, IntWritable>{

    private IntWritable docid = new IntWritable();
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
    	String[] id = value.toString().split(":");
    	
    	String input = value.toString().replace('\"', ' ')
    					  .replace('!', ' ')
    					  .replace('\'', ' ')
    					  .replace('?', ' ')
    					  .replace('(', ' ')
    					  .replace(')', ' ')
    					  .replace('-', ' ')
    					  .replace('.', ' ')
    					  .replace(':', ' ')
    					  .replace(';', ' ')
    					  .replace('=', ' ')
    					  .replace(',',' ')
    					  .replace('[',' ')
    					  .replace(']',' ')
    					  .toLowerCase();
    	
      StringTokenizer itr = new StringTokenizer(input);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        docid.set(Integer.parseInt(id[0].trim()));
        context.write(word, docid);
      }
    }
  }

  public static class Reduce
       extends Reducer<Text,IntWritable,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
    	Set<String> docs_ids = new HashSet<String>();
    	
      for (IntWritable val : values) {
        docs_ids.add(val.toString());
      }
      if (docs_ids.size() > 3) {
    	  result.set(String.join(", ",docs_ids));
          context.write(key, result);
      }
    }
  }
  @Override
  public int run(String[] args) throws Exception
  {
	    Configuration conf = getConf();
	    
	    Job job = Job.getInstance(conf, "inverted index");
	    job.setJarByClass(InvertedIndex.class);
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
	    FileOutputFormat.setOutputPath(job, new Path(args[1]+timeStamp));
	    return job.waitForCompletion(true)?0:1;
  }
    public static void main(String[] args) throws Exception 
    {
    	ToolRunner.run(new Configuration(),new InvertedIndex(), args);
    }
}