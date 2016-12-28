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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gwt.dev.util.collect.HashSet;

import net.opentsdb.core.Aggregators;
import net.opentsdb.core.Query;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DownsampleConfig;

final class CliDownsample {

  private static final Logger LOG = LoggerFactory.getLogger(CliDownsample.class);
  private static int origTSDBIndex = 0;
  private static TSDB tsdb;
  private static DownsampleConfig downsampleConfig;
  private static List<DownsampleServer> downsampleServers;
  private static Properties lastDownsampleTime;
  
  /** Prints usage and exits with the given retval.  */
  private static void usage(final ArgP argp, final String errmsg,
                            final int retval) {
    System.err.println(errmsg);
    System.err.println("Usage: downsample [OPTION]"
    	+ " OPTIONS\n"	
        + " --downsample-config=FILE\n"
       );
    if (argp != null) {
      System.err.print(argp.usage());
    }
    System.exit(retval);
  }

  public static void main(String[] args) throws Exception {
	ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    CliOptions.addVerbose(argp);
    args = CliOptions.parse(argp, args);
    if (args == null) {
      usage(argp, "Invalid usage.", 1);
    }

    // get a config object
    Config config = CliOptions.getConfig(argp);
    
    downsampleConfig = CliOptions.getDownsampleConfig(argp);
    try {
    	checkDownsampleConfig();
      } catch (Exception e) {
        LOG.error("Downsample configuration error: ",e);
        System.exit(1);
      }
    
    tsdb = new TSDB(config);
    tsdb.checkNecessaryTablesExist().joinUninterruptibly();
    argp = null;
    
    sortDownsamplingServers();
    initLastDownsamplingTime();
    
    try {
    	doDownsamplingLoop1();
    } finally {
    	try {
            tsdb.shutdown().joinUninterruptibly();
          } catch (Exception e) {
            LOG.error("Unexpected exception", e);
            System.exit(1);
          }
    }
    return;
  }
    
private static void initLastDownsamplingTime() throws IOException {
	String configLocation = downsampleConfig.configLocation();
	int cutAt = configLocation.lastIndexOf("/");
	String path = configLocation.substring(0, cutAt+1);
	path = path.concat("opentdsb-downsample_times");

	FileInputStream file_stream = null;
	try {
		file_stream = new FileInputStream(path);
	} catch (FileNotFoundException e) {
		loadDefaultTimes(path);
		return;
	}
	
	try {
		lastDownsampleTime.load(file_stream);
	} catch (IOException e) {
		LOG.error("Error reading downsampling times file.",e);
		throw e;
	}
}

private static void loadDefaultTimes(String path) {
	lastDownsampleTime = new Properties();
	LOG.info("Loading deafult last downsampling times.");
	Date now = new Date();      
	Long nowTime = new Long(now.getTime()/1000);
	long last_time;
	for (int i =0; i<downsampleServers.size();i++) {
		if (i == downsampleServers.size()-1) {
			last_time = 0;
		} else {
			last_time = nowTime - downsampleServers.get(i+1).downsamplePeriod;
		}
		lastDownsampleTime.put("tsd.downsample.server."+downsampleServers.get(i).indexInConfig+".last_time", last_time);
	}
}

private static void sortDownsamplingServers() {
	HashMap<Integer,Integer> downsamplingIntervals = new HashMap<Integer,Integer>();
	boolean firstIndexFound = false;
	Integer minIndex=0;
	for (int i = 1; i<=downsampleConfig.getInt("tsd.downsample.server.count");i++) {
		if (i!=origTSDBIndex) {
			if (!firstIndexFound) {
				minIndex = i;
				firstIndexFound = true;
			}
			downsamplingIntervals.put(i,downsampleConfig.getInt("tsd.downsample.server."+i+".downsample_interval"));
		}		
	}
	
	Integer min=downsamplingIntervals.get(minIndex);
	
	downsampleServers = new ArrayList<DownsampleServer>();
	int k=0;
	while (!downsamplingIntervals.isEmpty()) {
		Iterator<Integer> it = downsamplingIntervals.keySet().iterator();
		minIndex = it.next();
		min = downsamplingIntervals.get(minIndex);
		
		for(Integer i : downsamplingIntervals.keySet()) {
			Integer val = downsamplingIntervals.get(i);
			if (val<min) {
				min = val;
				minIndex = i;
			}
		}
		DownsampleServer ds = new DownsampleServer(
				downsampleConfig.getString("tsd.downsample.server."+minIndex+".address"),
				downsampleConfig.getInt("tsd.downsample.server."+minIndex+".port"),
				downsampleConfig.getInt("tsd.downsample.server."+minIndex+".downsample_period"),
				downsampleConfig.getInt("tsd.downsample.server."+minIndex+".downsample_interval"),
				minIndex
				);
		downsampleServers.add(k, ds);
		k++;
		downsamplingIntervals.remove(minIndex);
	}
}

private static void doDownsamplingLoop() {
	for (DownsampleServer ds : downsampleServers) {
		(new DownsamplingServerThread(ds)).start();
	}
}

private static void doDownsamplingLoop1() {
	(new DownsamplingServerThread(downsampleServers.get(0))).start();
}

private static void downsampleAllMetrics(String host, int port, int downsampleInterval, long start, long end) {
	for (int i=1;i<=downsampleConfig.getInt("tsd.downsample.metric.count");i++) {
    	String metric = downsampleConfig.getString("tsd.downsample.metric."+i+".name");
    	
    	if (metric == null) {
    		LOG.warn("Unable to find metric name for metric number "+i+". Check config file, whether it is defined.");
        	continue;
    	} else {
    		LOG.info("Getting tags for metric \""+metric+"\".");
    	}
    	
    	Set<String> tags = getTagsForMetric(metric,start,end);
    	
    	if (tags == null) continue;
    	    	
       	System.out.println("Downsampling for metric " + metric);
	    
       	String downsampleMethod = downsampleConfig.getString("tsd.downsample.metric."+i+".downsample_method");
		DataPoints[] dataPoints = getDownsampledMetric(metric, downsampleInterval, downsampleMethod,tags,start,end);
       	
		writeDownsampledMetrics(dataPoints,host,port);       	
    }		
}

private static void writeDownsampledMetrics(DataPoints[] dataPoints,
		String host, int port) {
	PrintWriter tsdbPut;
	try {
		Socket echoSocket = new Socket(host, port);
		tsdbPut = new PrintWriter(echoSocket.getOutputStream(), true);
	} catch (Exception e) {
		LOG.error("Error opening socket for downsampling server " + host,e);
		return;
	}
		
	for (int i =0; i< dataPoints.length;i++) {
		Map<String, String> tags = dataPoints[i].getTags();
		int k = 1;
		int tagsSize = tags.size();
		String tagString = "";
		for(String key : tags.keySet()) {
			if (k<tagsSize) {
				tagString = tagString.concat(key + "=" + tags.get(key) + " ");
			} else {
				tagString = tagString.concat(key + "=" + tags.get(key));
			}
			k++;
		}
		String putQuery = "";
		for (int j = 0; j< dataPoints[i].size(); j++) {
			if (dataPoints[i].isInteger(j)) {
				putQuery = "put " + dataPoints[i].metricName() + " " + dataPoints[i].timestamp(j) + " " 
							+ dataPoints[i].longValue(j) + " " + tagString;
			} else {
				putQuery = "put " + dataPoints[i].metricName() + " " + dataPoints[i].timestamp(j) + " " 
						+ dataPoints[i].doubleValue(j) + " " + tagString;
			}
		}
		
		if (dataPoints[i].metricName().equals("iostat.disk.write_merged")) {
			System.out.println("writiiiiiing " + putQuery);
			tsdbPut.println(putQuery);
		}
	}	
}

private static DataPoints[] getDownsampledMetric(String metric,
		int downsampleInterval, String downsampleMethod, Set<String> tags, long start, long end) {
   	final Query query = tsdb.newQuery();
    final DataPoints[] datapoints;   

    query.setStartTime(start);
    if (end > 0) {
      query.setEndTime(end);
    }
    
    Map<String, String> tagValues = new HashMap<String, String>();
    for (String tag :tags) {
    	tagValues.put(tag, "*");
    }
    query.setTimeSeries(metric, tagValues, Aggregators.get("sum"), false);
    
    query.downsample(downsampleInterval, Aggregators.get(downsampleMethod));
    datapoints = query.run();
    
    return datapoints;
}

private static Set<String> getTagsForMetric(String metric, long start, long end) {
	DataPoints[] dataPoints = null;
    try {
		dataPoints = doMetricTagsQuery(start, end, metric, new HashMap<String, String>());
    } catch (RuntimeException re) {
    	LOG.warn("Unable to find tags for metric \""+metric+"\".",re);
    	return null;
    }
    
    Set<String> tags;
    if (dataPoints != null && dataPoints.length!=0) {
    	tags = getTagsFromDatapoints(dataPoints);
    } else {
    	LOG.warn("Unable to find tags for metric \""+metric+"\".");
    	return null;
    }
    
	return tags;
}

private static long getEndTime(DownsampleServer ds) {
	Date now = new Date();      
	Long nowTime = new Long(now.getTime()/1000);
	long endTime = nowTime - ds.downsamplePeriod;
	/*long res = endTime/3600;
	res = res*3600;*/

	return endTime;
}

private static long getStartTime(DownsampleServer ds) {
	return (long) lastDownsampleTime.get("tsd.downsample.server."+
		ds.indexInConfig+".last_time");
}

private static Set<String> getTagsFromDatapoints(DataPoints[] dataPoints) {
	Set<String> tags = new HashSet<String>();

	for (DataPoints datapoint : dataPoints) {
		Map<String, String> dpTags =  new HashMap<String, String>();
		dpTags = datapoint.getTags();
		tags.addAll(datapoint.getAggregatedTags());
		tags.addAll(dpTags.keySet());		
	}
	
	return tags;
}


private static void checkCorrectServer(int place, boolean isDownsample) 
		throws Exception {
	try {
		  downsampleConfig.getInt("tsd.downsample.server."+place+".port");
	  } catch (Exception e) {
		  throw new Exception("port not found for tsd number " + place + ".");
	  }
	try {
		  downsampleConfig.getString("tsd.downsample.server."+place+".address");
	  } catch (Exception e) {
		  throw new Exception("address not found for tsd number " + place + ".");
	  }
	
	if (isDownsample) {
		  try {
			  downsampleConfig.getInt("tsd.downsample.server."+place+".downsample_period");
		  } catch (Exception e) {
			  throw new Exception("downsample_period not found for tsd number " + place + ".");
		  }
		  try {
			  downsampleConfig.getInt("tsd.downsample.server."+place+".downsample_interval");
		  } catch (Exception e) {
			  throw new Exception("downsample_interval not found for tsd number " + place + ".");
		  }
	}
  }
  
  private static void checkDownsampleConfig() throws Exception {
	  int tsdCount = downsampleConfig.getInt("tsd.downsample.server.count");
	  
	  if (tsdCount<2) throw new Exception("Invalid count of available TSDs.");

	  int origTrueCount = 0;
	  int origIndex = 0;
	  for (int i=1;i<=tsdCount;i++) {
		  try {
			  if (downsampleConfig.getBoolean("tsd.downsample.server."+i+".original")){
				  origTrueCount++;
				  origIndex = i;
				  checkCorrectServer(i,false);
			  }	else {
				  checkCorrectServer(i,true);
			  }
		  } catch (Exception e) {
			  checkCorrectServer(i,true);
		  }	  
	  }

	  if (origTrueCount == 0) throw new Exception("No original TSD defined.");
	  if (origTrueCount > 1) throw new Exception("Multiple original TSD defined.");
	  origTSDBIndex = origIndex;
  }

  private static DataPoints[] doMetricTagsQuery(long startTime, long endTime, String metric, 
		  Map<String, String> tag) {
    final Query query = tsdb.newQuery();;
    final DataPoints[] datapoints;
    
    query.setStartTime(startTime);
    if (endTime > 0) {
      query.setEndTime(endTime);
    }
    query.setTimeSeries(metric, tag, Aggregators.get("sum"), false);
    datapoints = query.run();
    
    return datapoints;
  }
  
  /**
   * Returns the given string trimed or null if is null 
   * @param string The string be trimmed of 
   * @return The string trimed or null
  */  
  private final String sanitize(final String string) {
    if (string == null) {
      return null;
    }

    return string.trim();
  }
  
  private static class DownsamplingServerThread extends Thread {
	  private DownsampleServer server;
	  
	   public DownsamplingServerThread(DownsampleServer server){
		   this.server = server;
	   }

	   public void run() {
		   while (true) {			   
				long start = getStartTime(server);
			    long end = getEndTime(server);
			    	
			    Date now = new Date();      
				Long longTime = new Long(now.getTime()/1000);
							    
			    if (end>=start+server.downsampleInterval) {
			    	System.out.println("downsampling server " + server.indexInConfig + "  start " + start + "  end " +end + " current " + longTime + " ");
					downsampleAllMetrics(server.address, server.port, server.downsampleInterval, start, end);		
				    lastDownsampleTime.put("tsd.downsample.server."+
				    		server.indexInConfig+".last_time",end);
			    } else {
				    try {
						Thread.sleep(server.downsampleInterval*1000);
					} catch (InterruptedException e) {
						LOG.error("Error suspending downsampling loop for server " + server.address,e);
					}
			    	System.out.println("waiting server " + server.indexInConfig + "   " + server.downsampleInterval + "  start " + start + "  end " +end + " current " + longTime + " ");
			    }			    
		}
	}
  }
  
  
  
  private static class DownsampleServer{
	  private String address;
	  private int port;
	  private int downsampleInterval;
	  private int downsamplePeriod;
	  private int indexInConfig;
	
	  public DownsampleServer(String a, int p, int dp, int di,int index) {
		  address = a;
		  port = p;
		  downsampleInterval = di;
		  downsamplePeriod = dp;
		  indexInConfig = index;
	  }
	  
  }
}


