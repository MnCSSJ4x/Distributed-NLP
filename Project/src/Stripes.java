import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;

public class Stripes {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private POSModel model;
	    private POSTaggerME tagger;
		

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			model = new POSModelLoader().load(new File("/Users/monjoy/Desktop/Assignment2/opennlp-en-ud-ewt-pos-1.0-1.9.3.bin")); 
			tagger = new POSTaggerME(model);
			String line = value.toString();
			
//			String[] tokens = line.split("[^\\w']+");
//			for (String token : tokens) {
//				word.set(token);
//				context.write(word, one);
//			}
//			this.init();
			
			MapWritable map = new MapWritable();
			if (line != null) {
				SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
		    	String tokenizedLine[] = tokenizer.tokenize(line); //Tokenize line
		    	String[] tags = tagger.tag(tokenizedLine); //Instanciate tags

				//POS Tag
		    	POSSample sample = new POSSample(tokenizedLine, tags); //Identify tags
//		    	System.out.println("\n\n" + sample.toString()); //Print tagged sentence
//				for(String token : sample.getTags()){
//					System.out.println(token); //Print tags of words
//			  	}
		    	List<String> words = Arrays.asList(sample.toString().split(" "));
		    	
		
		    	for(String w : words){
		    		word.set(w.split("_")[1]);
					context.write(word, one);
//		    		
		    	}
		    	
		   
				
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}

	public static void main(String[] args) throws Exception {
		
		

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Pairs");

		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class); // enable to use 'local aggregation'
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

