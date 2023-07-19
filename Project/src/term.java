import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;


import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.stemmer.PorterStemmer;

public class term {
	public static class TokenizerMapper extends Mapper<LongWritable, DoubleWritable, Text, DoubleWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private POSModel model;
	    private Set<String> stopWords = new HashSet<>();
		
	    private Map<String, Integer> dfMap = new HashMap<>();

	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	        URI[] files = context.getCacheFiles();
	        Path dfPath = new Path(files[0].getPath());
	        try (BufferedReader reader = new BufferedReader(new FileReader(dfPath.toString()))) {
	          String line;
	          while ((line = reader.readLine()) != null) {
	            String[] parts = line.split("\t");
	            String term = parts[0];
	            int df = Integer.parseInt(parts[1]);
	            dfMap.put(term, df);
	          }
	        }
	      }

		@Override
		public void map(LongWritable key, DoubleWritable value, Context context) throws IOException, InterruptedException {
			
			String[] words = value.toString().split("\\s+");
		    String docId = words[0];
		    Map<String, Integer> tfMap = new HashMap<>();
		    for (int i = 1; i < words.length; i++) {
		      String word = words[i];
		      tfMap.put(word, tfMap.getOrDefault(word, 0) + 1);
		    }
		    for (Map.Entry<String, Integer> entry : tfMap.entrySet()) {
		      String term = entry.getKey();
		      int tf = entry.getValue();
		      int df = dfMap.getOrDefault(term, 0);
		      double score = tf * Math.log10(10000.0 / (df + 1));
		      context.write(new Text(docId + "\t" + term), new DoubleWritable(score));
		    }
		}
	}

	public static class IntSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			//Checking for unique document only 
			for (DoubleWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
		    context.write(key, result);
		
		}

	}
//	public static class OutputFormat extends TextOutputFormat<Text, Text> {
//
//		  @Override
//		  public void writeRecord(CSVPrinter printer, Entry<Text, Text> entry) throws IOException {
//		    printer.print(entry.getKey().toString());
//		    printer.print('#');
//		    printer.print(entry.getValue().toString());
//		    printer.println();
//		  }
//		}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		//Creating Configuration
	    Job job = Job.getInstance(conf, "document frequency");
	    //Add link to Stopwords.txt 
	    job.addCacheFile(new URI("/Users/monjoy/Desktop/Assignment2/stopwords.txt"));
	    
	    job.setMapperClass(TokenizerMapper.class);
	    job.setReducerClass(IntSumReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    //	    job.setOutputFormatClass(OutputFormat.class);
	
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
