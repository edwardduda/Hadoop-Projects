package ngram;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class NgramMapReduce extends Configured implements Tool {
	public static enum Profiles {
		A1('a', 1),
		B1('b', 1),
		A2('a', 2),
		B2('b', 2);

		private final char profileChar;
		private final int ngramNum;

		private Profiles(char c, int n) {
			// Initialize the Profiles enum constructor
			this.profileChar = c;
			this.ngramNum = n;
		}

		public boolean equals(Profiles p) {
			// Implement the equals method for Profiles
			if (p == null)
				return false;
			return this.profileChar == p.profileChar && this.ngramNum == p.ngramNum;
		}
	}

	public static class TokenizerMapper extends Mapper<Object, BytesWritable, Text, VolumeWriteable> {

		// Define and initialize necessary class variables
		private VolumeWriteable volume = new VolumeWriteable();

		public void map(Object key, BytesWritable bWriteable, Context context)
				throws IOException, InterruptedException {
			Profiles profile = context.getConfiguration().getEnum("profile", Profiles.A1); // get profile

			// Generate an UUID to uniquely identify the book
			UUID uuid = UUID.randomUUID();

			// code to get a book
			String rawText = new String(bWriteable.getBytes(), "UTF-8");
			Book book = new Book(rawText, profile.ngramNum);
			StringTokenizer itr = new StringTokenizer(book.getBookBody());

			// Extract necessary information from the book (author, year)
			String author = book.getBookAuthor();
			String year = book.getBookYear();

			// Define any helper variables you need before looping through tokens
			String previousToken = null;

			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();

				// Implement the mapping logic for different profiles (A1, B1, A2, B2)
				Text keyOut;

				if (profile.ngramNum == 1) {
					// Unigrams
					if (profile.profileChar == 'a') {
						// Profile A1
						keyOut = new Text(token);
					} else {
						// Profile B1
						keyOut = new Text(author + "\t" + token);
					}

					VolumeWriteable volume = new VolumeWriteable();
					volume.set(new MapWritable(), new IntWritable(1));
					volume.insertMapValue(new Text(uuid.toString()), new IntWritable(1));
					context.write(keyOut, volume);

				} else {
					// Bigrams
					if (previousToken != null) {
						String bigram = previousToken + " " + token;

						if (profile.profileChar == 'a') {
							// Profile A2
							keyOut = new Text(bigram);
						} else {
							// Profile B2
							keyOut = new Text(author + "\t" + bigram);
						}

						VolumeWriteable volume = new VolumeWriteable();
						volume.set(new MapWritable(), new IntWritable(1));
						volume.insertMapValue(new Text(uuid.toString()), new IntWritable(1));
						context.write(keyOut, volume);
					}
					previousToken = token;
				}
			}
		}

	}

	public static class IntSumReducer extends Reducer<Text, VolumeWriteable, Text, VolumeWriteable> {

		public void reduce(Text key, Iterable<VolumeWriteable> values, Context context)
				throws IOException, InterruptedException {
			// Aggregate the counts and volume information
			int sum = 0;
			MapWritable volumeIds = new MapWritable();

			for (VolumeWriteable val : values) {
				sum += val.getCount().get();
				volumeIds.putAll(val.getVolumeIds());
			}

			VolumeWriteable result = new VolumeWriteable();
			result.set(volumeIds, new IntWritable(sum));

			context.write(key, result);
		}
	}

	public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {
		// function to run job

		Job job = Job.getInstance(conf, "ngram");

		// specify classes for Map Reduce tasks
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setJarByClass(NgramMapReduce.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VolumeWriteable.class);

		FileInputFormat.addInputPath(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new NgramMapReduce(), args);
		System.exit(res); // res will be 0 if all tasks are executed successfully and 1 otherwise
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Profiles profiles[] = { Profiles.A1, Profiles.A2, Profiles.B1, Profiles.B2 };
		for (Profiles p : profiles) {
			conf.setEnum("profile", p); // Set the correct argument in the configuration
			System.out.println("For profile: " + p.toString());
			if (runJob(conf, args[0], args[1] + p.toString()) != 0) // Call runJob with the correct arguments
				return 1; // error
		}
		return 0; // success
	}
}
