package assignment06;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

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

public class MinHashingAndLSH extends Configured implements Tool {
	//public String inputFile = "/Users/arturofigueroa/Desktop/Maestria-DataScience/Lectures/2nd-Semester/Big-Data/assigments/a06/corpus_with_line_numbers.txt";
	//public String outputFile = "/Users/arturofigueroa/inverted-index";
	public String inputFile = "C:\\Users\\Lisandro\\Desktop\\Data Science - Uni\\Big Data\\Assignment 6\\BDA\\data\\input\\corpus_with_line_numbers.txt";
	public String outputFile = "C:\\Users\\Lisandro\\Desktop\\Data Science - Uni\\Big Data\\Assignment 6\\BDA\\data\\output\\outputTask2";
	String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());

	public static class Map extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] text = value.toString().split(":", 2);
			String id = text[0].trim();

			String input = text[1].toString();

			int[] signatureVector = GenericHashFunction.minHashSignature(10, input);

			System.out.println("");

			String p2 = "";
			String p3 = "";
			String p4 = "";
			String p5 = "";

			for (int i = 0; i < signatureVector.length; i++) {
				String p1 = String.valueOf(signatureVector[i]);
				context.write(new Text(p1), new IntWritable(Integer.parseInt(id)));

				p2 = p2 + "," + p1;
				if (i > 1) {
					p3 = p3 + "," + p1;
					context.write(new Text(p3), new IntWritable(Integer.parseInt(id)));

				}
				if (i > 3) {
					p5 = p5 + "," + p2;
					context.write(new Text(p5), new IntWritable(Integer.parseInt(id)));
				}
				p5 = p4;
				p4 = p3;
				p3 = p2;
				p2 = p1;
			}

		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			String listValues = "";
			
			int count = 0;
			boolean idFound = false;
			
			for (IntWritable val : values) {
				
				count ++;
				
				if (val.get() == 1282) idFound = true;
				
				if (count == 1) listValues += (val.toString());
				else listValues += (", " + val.toString());

			}
			
			if (count > 1 && idFound) {
				String keyStr = String.format("%-15s", key.toString());
				context.write(new Text(keyStr), new Text(listValues));
			}

		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(MinHashingAndLSH.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputFile));
		FileOutputFormat.setOutputPath(job, new Path(outputFile + timeStamp));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new MinHashingAndLSH(), args);
	}
}
