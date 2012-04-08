package org.apache.nutch.indexer.elasticsearch;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.indexer.IndexerMapReduce;
import org.apache.nutch.indexer.NutchIndexWriterFactory;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.apache.solr.common.util.DateUtil;

public class ElasticsearchIndexer  extends Configured implements Tool {
	public static Log LOG = LogFactory.getLog(ElasticsearchIndexer.class);

	  public ElasticsearchIndexer() {
	    super(null);
	  }

	  public ElasticsearchIndexer(Configuration conf) {
	    super(conf);
	  }
	  
	  public void indexElasticsearch(String elasticsearchUrl, String elasticsearchPort, Path crawlDb, Path linkDb,
		      List<Path> segments) throws IOException {
		  
		    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		    long start = System.currentTimeMillis();
		    LOG.info("ElasticsearchIndexer: starting at " + sdf.format(start));

		    final JobConf job = new NutchJob(getConf());
		    job.setJobName("index-elasticsearch " + elasticsearchUrl);

		    IndexerMapReduce.initMRJob(crawlDb, linkDb, segments, job);

		    job.set(ElasticsearchConstants.SERVER_URL, elasticsearchUrl);
		    job.set(ElasticsearchConstants.SERVER_PORT, elasticsearchPort);

		    NutchIndexWriterFactory.addClassToConf(job, ElasticsearchWriter.class);

		    job.setReduceSpeculativeExecution(false);

		    final Path tmp = new Path("tmp_" + System.currentTimeMillis() + "-" +
		                         new Random().nextInt());

		    FileOutputFormat.setOutputPath(job, tmp);
		    try {
		      // run the job and write the records to infinite (this will be done via the rest api
		      JobClient.runJob(job);
		      long end = System.currentTimeMillis();
		      LOG.info("ElasticsearchIndexer: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
		    }
		    catch (Exception e){
		      LOG.error(e);
		    } finally {
		      FileSystem.get(job).delete(tmp, true);
		    }
		  }
	  
	  public int run(String[] args) throws Exception {
		    if (args.length < 5) {
		      System.err.println("Usage: ElasticsearchIndexer <elasticsearch url> <elasticsearch port> <crawldb> <linkdb> (<segment> ... | -dir <segments>)");
		      return -1;
		    }

		    final Path crawlDb = new Path(args[2]);
		    final Path linkDb = new Path(args[3]);

		    final List<Path> segments = new ArrayList<Path>();
		    for (int i = 4; i < args.length; i++) {
		      if (args[i].equals("-dir")) {
		        Path dir = new Path(args[++i]);
		        FileSystem fs = dir.getFileSystem(getConf());
		        FileStatus[] fstats = fs.listStatus(dir,
		                HadoopFSUtil.getPassDirectoriesFilter(fs));
		        Path[] files = HadoopFSUtil.getPaths(fstats);
		        for (Path p : files) {
		          segments.add(p);
		        }
		      } else {
		        segments.add(new Path(args[i]));
		      }
		    }

		    try {
		      indexElasticsearch(args[0], args[1], crawlDb, linkDb, segments);
		      return 0;
		    } catch (final Exception e) {
		      LOG.fatal("ElasticsearchIndexer: " + StringUtils.stringifyException(e));
		      return -1;
		    }
		  }

	      // ./bin/nutch org.apache.nutch.indexer.mongodb.ElasticSearchIndexer localhost 9000 crawldb crawldb/linkdb crawldb/segments/*
		  public static void main(String[] args) throws Exception {
		    final int res = ToolRunner.run(NutchConfiguration.create(), new ElasticsearchIndexer(), args);
		    System.exit(res);
		  }
}
