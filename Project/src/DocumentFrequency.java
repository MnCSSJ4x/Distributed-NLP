import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class DocumentFrequency {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private POSModel model;
	    private Set<String> stopWords = new HashSet<>();
		
	    public void setup(Context context) throws IOException, InterruptedException {
	        // Load stopwords from file
	        URI[] cacheFiles = context.getCacheFiles();
	        if (cacheFiles != null && cacheFiles.length > 0) {
	          try (BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()))) {
	            String line;
	            while ((line = reader.readLine()) != null) {
	              stopWords.add(line.trim());
	            }
	          }
	        }
	      }

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			model = new POSModelLoader().load(new File("/Users/monjoy/Desktop/Assignment2/opennlp-en-ud-ewt-pos-1.0-1.9.3.bin")); 
			PorterStemmer stemmer = new PorterStemmer();
			String line = value.toString();
			
			
			if (line != null) {
				SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
		    	String tokenizedLine[] = tokenizer.tokenize(line); //Tokenize line
		    	
		    	//Unique token extraction 
		    	Set<String> uniqueWords = new HashSet<String>();
		    	
		    	for(String s: tokenizedLine) {
		    		 if (!stopWords.contains(s)){
		    		uniqueWords.add(s.toLowerCase());
		    		 
		    		}
		    	}
		    	
		    	//Stemming and intermediate map formation 
		    	for(String token: uniqueWords){
		    		String stemmedOutput = stemmer.stem(token);
		    		word.set(stemmedOutput);
					context.write(word, one);
					//we emit word,1 
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
			//Checking for unique document only 
			for (IntWritable val : values) {
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
