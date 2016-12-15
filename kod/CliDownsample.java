// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.opentsdb.core.Aggregator;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.Query;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.RateOptions;
import net.opentsdb.core.Tags;
import net.opentsdb.core.TSDB;
import net.opentsdb.graph.Plot;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.DownsampleConfig;
import net.opentsdb.utils.JSON;

final class CliDownsample {

  private static final Logger LOG = LoggerFactory.getLogger(CliDownsample.class);
  private static boolean isEnabled = false;
  
  /** Prints usage and exits with the given retval.  */
  private static void usage(final ArgP argp, final String errmsg,
                            final int retval) {
    System.err.println(errmsg);
    System.err.println("Usage: query"
        + " [Gnuplot opts] START-DATE [END-DATE] <query> [queries...]\n"
        + "A query has the form:\n"
        + "  FUNC [rate] [counter,max,reset] [downsample N FUNC] SERIES [TAGS]\n"
        + "For example:\n"
        + " 2010/03/11-20:57 sum my.awsum.metric host=blah"
        + " sum some.other.metric host=blah state=foo\n"
        + "Dates must follow this format: YYYY/MM/DD-HH:MM[:SS] or Unix Epoch\n"
        + " or relative time such as 1y-ago, 2d-ago, etc.\n"
        + "Supported values for FUNC: " + Aggregators.set()
        + "\nGnuplot options are of the form: +option=value");
    if (argp != null) {
      System.err.print(argp.usage());
    }
    System.exit(retval);
  }

  public static void main(String[] args) throws Exception {
    ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    CliOptions.addVerbose(argp);
    argp.addOption("--graph", "BASEPATH",
                   "Output data points to a set of files for gnuplot."
                   + "  The path of the output files will start with"
                   + " BASEPATH.");
    args = CliOptions.parse(argp, args);
    if (args == null) {
      usage(argp, "Invalid usage.", 1);
    } else if (args.length < 3) {
      usage(argp, "Not enough arguments.", 2);
    }

    // get a config object
    Config config = CliOptions.getConfig(argp);
    
    DownsampleConfig downsampleConfig = CliOptions.getDownsampleConfig(argp);
    try {
    	checkDownsampleConfig(downsampleConfig);
      } catch (Exception e) {
        LOG.error("Downsample configuration error: ",e);
        System.exit(1);
      }
    
    /*
    final ArrayList<TSDB> tsdbs = new ArrayList<TSDB>();    
    for (int i = 0;i<downsampleConfig.getInt("tsd.count");i++){
    	//Config config = getTSDConfig(downsampleConfig,i+1);
    	//tsdbs.add(i,new TSDB(config));
    }*/
    
    
    
    
    final TSDB tsdb = new TSDB(config);
    tsdb.checkNecessaryTablesExist().joinUninterruptibly();
    final String basepath = argp.get("--graph");
    argp = null;

    
    for (int i=0;i<downsampleConfig.getInt("tsd.downsapmle.metric.count");i++) {
    	String metric = downsampleConfig.getString("tsd.downsapmle.metric."+(i+1)+".name");
    	String commonTag = downsampleConfig.getString("tsd.downsapmle.metric."+(i+1)+".tag");
    	String downsampleMethod = downsampleConfig.getString("tsd.downsapmle.metric."+(i+1)+".downsample_method");
    	
    	final Aggregator agg = Aggregators.get("sum");
        final Aggregator downsample = Aggregators.get("downsampleMethod");
        DataPoints[]  dataPoints = doQuery(tsdb, getStartTime(), getEndTime(), agg, downsample, metric);
    }
    
   
    /*

    if (plot != null) {
      try {
        final int npoints = plot.dumpToFiles(basepath);
        LOG.info("Wrote " + npoints + " for Gnuplot");
      } catch (IOException e) {
        LOG.error("Failed to write the Gnuplot file under " + basepath, e);
        System.exit(1);
      }
    }*/
  }
  

private static long getEndTime() {
	// TODO Auto-generated method stub
	return 0;
}

private static long getStartTime() {
	// TODO Auto-generated method stub
	return 0;
}

/*

  private static Config getTSDConfig(DownsampleConfig downsampleConfig, int place) throws Exception {
	  int port = downsampleConfig.getInt("tsd."+place+".port");
	  String address = downsampleConfig.getString("tsd."+place+".address");
	  URL configUrl = new URL("http://"+address+":"+port+"/api/config");
	  LOG.info("Reading config for tsd server number " +place+" from URL: " + configUrl.toExternalForm());
	  
	  BufferedReader in;
	  try {
		  in = new BufferedReader(new InputStreamReader(configUrl.openStream()));
	  } catch (Exception e) {
		  LOG.error("Failed reading config for tsd server number " +place+" from URL: " + configUrl.toExternalForm());
		  throw e;
	  }

	  String tmpConfigFile = "/tmp/opentsdb-downsample-tmp" + place;
	  PrintWriter writer;
	  try{
		    writer = new PrintWriter(tmpConfigFile, "UTF-8");
		} catch (IOException e) {
			LOG.error("Erorr creating temporary config file for downsample TSD number " +place);
			throw e;
		}
      String inputLine = in.readLine();
      ObjectMapper jsonMapper = new ObjectMapper();
      JsonNode jsonTree = jsonMapper.readTree(inputLine);
      Iterator<String> values = jsonTree.fieldNames();
      while (values.hasNext()) {
    	  String key = values.next();
    	  String value = jsonTree.findValue(key).asText();
    	  writer.println(key + " = " + value);
      }
      
      writer.close();
      in.close();
      return null;
}*/

private static void checkCorrectServer(DownsampleConfig downsampleConfig, int place, boolean isDownsample) 
		throws Exception {
	try {
		  downsampleConfig.getInt("tsd.downsapmle.server."+place+".port");
	  } catch (Exception e) {
		  throw new Exception("port not found for tsd number " + place + ".");
	  }
	try {
		  downsampleConfig.getString("tsd.downsapmle.server."+place+".address");
	  } catch (Exception e) {
		  throw new Exception("address not found for tsd number " + place + ".");
	  }
	
	if (isDownsample) {
		  try {
			  downsampleConfig.getInt("tsd.downsapmle.server."+place+".downsample_period");
		  } catch (Exception e) {
			  throw new Exception("downsample_period not found for tsd number " + place + ".");
		  }
		  try {
			  downsampleConfig.getInt("tsd.downsapmle.server."+place+".downsample_interval");
		  } catch (Exception e) {
			  throw new Exception("downsample_interval not found for tsd number " + place + ".");
		  }
	}
  }
  
  private static void checkDownsampleConfig(DownsampleConfig downsampleConfig) throws Exception {
	  int tsdCount = downsampleConfig.getInt("tsd.downsapmle.server.count");
	  
	  if (tsdCount<2) throw new Exception("Invalid count of available TSDs.");

	  int origTrueCount = 0;
	  int j;
	  for (int i=0;i<tsdCount;i++) {
		  j=i+1;
		  try {
			  
			  if (downsampleConfig.getBoolean("tsd."+j+".original")){
				  origTrueCount++;
				  checkCorrectServer(downsampleConfig,j,false);
			  }	else {
				  checkCorrectServer(downsampleConfig,j,true);
			  }
		  } catch (Exception e) {
			  checkCorrectServer(downsampleConfig,j,true);
		  }	  
	  }

	  if (origTrueCount == 0) throw new Exception("No original TSD defined.");
	  if (origTrueCount > 1) throw new Exception("Multiple original TSD defined.");
  }

  private static DataPoints[] doMetricTagsQuery(TSDB tsdb, long startTime, long endTime, String metric, 
		  Map<String, String> tags, Aggregator downsample, long downsampleInterval) {
    final Query query = tsdb.newQuery();;
    final DataPoints[] datapoints;
    
    query.setStartTime(startTime);
    if (endTime > 0) {
      query.setEndTime(endTime);
    }
    query.setTimeSeries(metric, tags, Aggregators.get("sum"), false);
    query.downsample(downsampleInterval, downsample);
    datapoints = query.run();
    
    return datapoints;
  }
  
  private static DataPoints[] doDownsampleQuery(TSDB tsdb, long startTime, long endTime, String metric, 
		  Map<String, String> tags, Aggregator downsample, long downsampleInterval) {
    final Query query = tsdb.newQuery();;
    final DataPoints[] datapoints;
    
    query.setStartTime(startTime);
    if (endTime > 0) {
      query.setEndTime(endTime);
    }
    query.setTimeSeries(metric, tags, Aggregators.get("sum"), false);
    query.downsample(downsampleInterval, downsample);
    datapoints = query.run();
    
    return datapoints;
  }

  /**
   * Parses the query from the command lines.
   * @param args The command line arguments.
   * @param tsdb The TSDB to use.
   * @param queries The list in which {@link Query}s will be appended.
   * @param plotparams The list in which global plot parameters will be
   * appended.  Ignored if {@code null}.
   * @param plotoptions The list in which per-line plot options will be
   * appended.  Ignored if {@code null}.
   */
  static void parseCommandLineQuery(final String[] args,
                                    final TSDB tsdb,
                                    final ArrayList<Query> queries,
                                    final ArrayList<String> plotparams,
                                    final ArrayList<String> plotoptions) {
    long start_ts = DateTime.parseDateTimeString(args[0], null);
    if (start_ts >= 0)
      start_ts /= 1000;
    long end_ts = -1;
    if (args.length > 3){
      // see if we can detect an end time
      try{
      if (args[1].charAt(0) != '+'
           && (args[1].indexOf(':') >= 0
               || args[1].indexOf('/') >= 0
               || args[1].indexOf('-') >= 0
               || Long.parseLong(args[1]) > 0)){
          end_ts = DateTime.parseDateTimeString(args[1], null);
        }
      }catch (NumberFormatException nfe) {
        // ignore it as it means the third parameter is likely the aggregator
      }
    }
    // temp fixup to seconds from ms until the rest of TSDB supports ms
    // Note you can't append this to the DateTime.parseDateTimeString() call as
    // it clobbers -1 results
    if (end_ts >= 0)
      end_ts /= 1000;

    int i = end_ts < 0 ? 1 : 2;
    while (i < args.length && args[i].charAt(0) == '+') {
      if (plotparams != null) {
        plotparams.add(args[i]);
      }
      i++;
    }

    while (i < args.length) {
      final Aggregator agg = Aggregators.get(args[i++]);
      final boolean rate = args[i].equals("rate");
      RateOptions rate_options = new RateOptions(false, Long.MAX_VALUE,
          RateOptions.DEFAULT_RESET_VALUE);
      if (rate) {
        i++;
        
        long counterMax = Long.MAX_VALUE;
        long resetValue = RateOptions.DEFAULT_RESET_VALUE;
        if (args[i].startsWith("counter")) {
          String[] parts = Tags.splitString(args[i], ',');
          if (parts.length >= 2 && parts[1].length() > 0) {
            counterMax = Long.parseLong(parts[1]);
          }
          if (parts.length >= 3 && parts[2].length() > 0) {
            resetValue = Long.parseLong(parts[2]);
          }
          rate_options = new RateOptions(true, counterMax, resetValue);
          i++;
        }
      }
      final boolean downsample = args[i].equals("downsample");
      if (downsample) {
        i++;
      }
      final long interval = downsample ? Long.parseLong(args[i++]) : 0;
      final Aggregator sampler = downsample ? Aggregators.get(args[i++]) : null;
      final String metric = args[i++];
      final HashMap<String, String> tags = new HashMap<String, String>();
      while (i < args.length && args[i].indexOf(' ', 1) < 0
             && args[i].indexOf('=', 1) > 0) {
        Tags.parse(tags, args[i++]);
      }
      if (i < args.length && args[i].indexOf(' ', 1) > 0) {
        plotoptions.add(args[i++]);
      }
      
      final Query query = tsdb.newQuery();
      query.setStartTime(start_ts);
      if (end_ts > 0) {
        query.setEndTime(end_ts);
      }
      query.setTimeSeries(metric, tags, agg, rate, rate_options);
      if (downsample) {
        query.downsample(interval, sampler);
      }
      queries.add(query);
    }
  }
}
