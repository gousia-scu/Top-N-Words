package TopNWords;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;



public class TopWords {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: TopN <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("TopWords");
        job.setJarByClass(TopWords.class);
        job.setMapperClass(TopNMapper.class);
        job.setCombinerClass(TopNReducer.class);
        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(80);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);
        FileSystem fs =  FileSystem.getLocal(conf);
        boolean deleteSource = false;
        Path srcPath = new Path(otherArgs[1]);
        Path dstPath = new Path("result");
        fs.mkdirs(dstPath, FsPermission.getDefault());
        String addString = null;
        final boolean result = FileUtil.copyMerge(fs, srcPath, fs, dstPath, deleteSource, conf,
                addString);
        if(result) {
        	SortedSet<Map.Entry<String,Integer>> sortedEntries = new TreeSet<Map.Entry<String,Integer>>(
        	        new Comparator<Map.Entry<String,Integer>>() {
        	            @Override
        	            public int compare(Map.Entry<String,Integer> e1, Map.Entry<String,Integer> e2) {
        	            	int res = e2.getValue().compareTo(e1.getValue());
        	                return res != 0 ? res : 1;
        	            }
        	        }
        	    );
        	try(BufferedReader br = new BufferedReader(new FileReader(dstPath.toString()+"/out"))) {
        	    for(String line; (line = br.readLine()) != null; ) {
        	    	String[] splited = line.split("\\s+");
        	    	sortedEntries.add(new AbstractMap.SimpleEntry<String,Integer>(splited[0], Integer.parseInt(splited[1])));
        	    	
        	    }
        	}
        	    Iterator<Entry<String, Integer>> entries = sortedEntries.iterator();
        	    int c = 0;
        	    while (entries.hasNext() && c<100) { 
        	    	Entry<String, Integer> entry = entries.next();
                    System.out.println(entry.getKey()+" : "+entry.getValue());
                    c++;
                }
        	    
        }
    }

    public static class TopNMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ");
            StringTokenizer itr = new StringTokenizer(cleanLine);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().trim());
                context.write(word, one);
            }
        }
    }

    /**
     * The reducer retrieves every word and puts it into a Map: if the word already exists in the
     * map, increments its value, otherwise sets it to 1.
     */
    public static class TopNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    	private Map<Text, IntWritable> countMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	// computes the number of occurrences of a single word
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
        	countMap.put(new Text(key), new IntWritable(sum));
        	
        }

        @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<Text, IntWritable> sortedMap = MiscUtils.sortByValues(countMap);
            int counter2 = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter2++ == 2) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }
    }

    public static class TopNCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // computes the number of occurrences of a single word
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}