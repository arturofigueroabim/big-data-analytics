import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mrdp.utils.MRDPUtils;

public class JoinsTask extends Configured implements Tool {

	public String inputFilePost10000 = "/Users/arturofigueroa/Desktop/Maestria-DataScience/Lectures/2nd-Semester/Big-Data/assigments/a05/post10000.xml";
	public String inputFileUser10000 = "/Users/arturofigueroa/Desktop/Maestria-DataScience/Lectures/2nd-Semester/Big-Data/assigments/a05/user10000.xml";
	public String outputFile = "/Users/arturofigueroa/JoinsTaks";
	String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());

	static java.util.Map<String, String> MRDPUPosts = new HashMap<String, String>();
	static java.util.Map<String, String> MRDPUUsers = new HashMap<String, String>();

	static HashMap<String, String> usersInfo = new HashMap<String, String>();

	public static class JoinMapPost10000 extends Mapper<Object, Text, Text, Text> {
		private Text userScore = new Text();
		private Text idUser = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			MRDPUPosts = MRDPUtils.transformXmlToMap(value.toString());

			if (MRDPUPosts.get("OwnerUserId") != null && MRDPUPosts.get("Score") != null) {
				String id = MRDPUPosts.get("OwnerUserId");
				String score = MRDPUPosts.get("Score");
				idUser.set(id);
				userScore.set(score);

				context.write(idUser, userScore);
			}

		}
	}

	public static class JoinMapuser10000 extends Mapper<Object, Text, Text, Text> {
		private Text user = new Text();
		private Text idUser = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			MRDPUUsers = MRDPUtils.transformXmlToMap(value.toString());

			if (MRDPUUsers.get("Id") != null && MRDPUUsers.get("DisplayName") != null) {
				String id = MRDPUUsers.get("Id");
				String name = MRDPUUsers.get("DisplayName");

				idUser.set(id);
				user.set("Name: " + name);
				context.write(idUser, user);
			}

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String name = null;
			String userInfo = null;
			int posts = 0;
			double score = 0;
			double avg = 0;

			if (20 <= Integer.parseInt(key.toString()) && Integer.parseInt(key.toString()) <= 40) {

				for (Text user : values) {

							userInfo = user.toString();
							
							if(userInfo.contains("Name:")) {
								name = userInfo;
								continue;
							}
							
							score += Integer.parseInt(userInfo);

							System.out.println("Score: " + score);
							posts++;
						
				}
				avg = (score / posts ) * 100;
				
				String userTotal = String.format("%-30s\tPosts:	%d\tAVG: %.2f" , name, posts, avg);

				context.write(new Text("id: " + key), new Text(userTotal));
			}

		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "StackOverFlow Task");

		job.setJarByClass(JoinsTask.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(Reduce.class);
		job.setOutputValueClass(Text.class);

		Path postInputPath = new Path(inputFilePost10000);
		Path userInputPath = new Path(inputFileUser10000);

		MultipleInputs.addInputPath(job, postInputPath, TextInputFormat.class, JoinMapPost10000.class);
		MultipleInputs.addInputPath(job, userInputPath, TextInputFormat.class, JoinMapuser10000.class);
		FileOutputFormat.setOutputPath(job, new Path(outputFile + timeStamp));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new JoinsTask(), args);
	}

}
