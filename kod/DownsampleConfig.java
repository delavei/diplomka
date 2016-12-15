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
package net.opentsdb.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

/**
 * OpenTSDB Configuration Class
 * 
 * This handles all of the user configurable variables for a TSD. On
 * initialization default values are configured for all variables. Then
 * implementations should call the {@link #loadConfig()} methods to search for a
 * default configuration or try to load one provided by the user.
 * 
 * To add a configuration, simply set a default value in {@link #setDefaults()}.
 * Wherever you need to access the config value, use the proper helper to fetch
 * the value, accounting for exceptions that may be thrown if necessary.
 * 
 * The get<type> number helpers will return NumberFormatExceptions if the
 * requested property is null or unparseable. The {@link #getString(String)} 
 * helper will return a NullPointerException if the property isn't found.
 * <p>
 * Plugins can extend this class and copy the properties from the main
 * TSDB.config instance. Plugins should never change the main TSD's config
 * properties, rather a plugin should use the Config(final Config parent)
 * constructor to get a copy of the parent's properties and then work with the
 * values locally.
 * @since 2.0
 */
public class DownsampleConfig {
  private static final Logger LOG = LoggerFactory.getLogger(DownsampleConfig.class);

  /** Flag to determine if we're running under Windows or not */
  public static final boolean IS_WINDOWS = 
      System.getProperty("os.name", "").contains("Windows");
    
  /**
   * The list of properties configured to their defaults or modified by users
   */
  protected final HashMap<String, String> properties = 
		    new HashMap<String, String>();

  /** Tracks the location of the file that was actually loaded */
  protected String config_location;

  /**
   * Constructor that initializes default configuration values. May attempt to
   * search for a config file if configured.
   * @param auto_load_config When set to true, attempts to search for a config
   *          file in the default locations
   * @throws IOException Thrown if unable to read or parse one of the default
   *           config files
   */
  public DownsampleConfig() throws IOException {
      loadConfig();
   }
  
  public DownsampleConfig(final String file) throws IOException {
	    loadConfig(file);
	  }

  /** @return The file that generated this config. May be null */
  public String configLocation() {
    return config_location;
  }
 

  /**
   * Searches a list of locations for a valid opentsdb.conf file
   * 
   * The config file must be a standard JAVA properties formatted file. If none
   * of the locations have a config file, then the defaults or command line
   * arguments will be used for the configuration
   * 
   * Defaults for Linux based systems are: ./opentsdb.conf /etc/opentsdb.conf
   * /etc/opentsdb/opentdsb.conf /opt/opentsdb/opentsdb.conf
   * 
   * @throws IOException Thrown if there was an issue reading a file
   */
  protected void loadConfig() throws IOException {
    if (config_location != null && !config_location.isEmpty()) {
      loadConfig(config_location);
      return;
    }

    final ArrayList<String> file_locations = new ArrayList<String>();

    // search locally first
    file_locations.add("opentsdb-downsample.conf");

    // add default locations based on OS
    if (System.getProperty("os.name").toUpperCase().contains("WINDOWS")) {
      file_locations.add("C:\\Program Files\\opentsdb\\opentsdb-downsample.conf");
      file_locations.add("C:\\Program Files (x86)\\opentsdb\\opentsdb-downsample.conf");
    } else {
      file_locations.add("/etc/opentsdb-downsample.conf");
      file_locations.add("/etc/opentsdb/opentsdb-downsample.conf");
      file_locations.add("/opt/opentsdb/opentsdb-downsample.conf");
    }

    for (String file : file_locations) {
      try {
        FileInputStream file_stream = new FileInputStream(file);
        Properties props = new Properties();
        props.load(file_stream);
        
        // load the hash map
        loadHashMap(props);        
      } catch (Exception e) {
        // don't do anything, the file may be missing and that's fine
        LOG.debug("Unable to find or load " + file, e);
        continue;
      }

      // no exceptions thrown, so save the valid path and exit
      LOG.info("Successfully loaded configuration file: " + file);
      config_location = file;
      return;
    }

    LOG.warn("No downsample configuration found, downsampling will not be available");
  }

  /**
   * Attempts to load the configuration from the given location
   * @param file Path to the file to load
   * @throws IOException Thrown if there was an issue reading the file
   * @throws FileNotFoundException Thrown if the config file was not found
   */
  protected void loadConfig(final String file) throws FileNotFoundException,
      IOException {
    final FileInputStream file_stream = new FileInputStream(file);
    try {
      final Properties props = new Properties();
      props.load(file_stream);
  
      // load the hash map
      loadHashMap(props);
  
      // no exceptions thrown, so save the valid path and exit
      LOG.info("Successfully loaded downsampling configuration file: " + file);
      config_location = file;
    } finally {
      file_stream.close();
    }
  }

  /**
   * Returns the given property as a String
   * @param property The property to load
   * @return The property value as a string
   * @throws NullPointerException if the property did not exist
   */
  public final String getString(final String property) {
    return properties.get(property);
  }

  /**
   * Returns the given property as an integer
   * @param property The property to load
   * @return A parsed integer or an exception if the value could not be parsed
   * @throws NumberFormatException if the property could not be parsed
   * @throws NullPointerException if the property did not exist
   */
  public final int getInt(final String property) {
    return Integer.parseInt(sanitize(properties.get(property)));
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

  /**
   * Returns the given property as a boolean
   * 
   * Property values are case insensitive and the following values will result
   * in a True return value: - 1 - True - Yes
   * 
   * Any other values, including an empty string, will result in a False
   * 
   * @param property The property to load
   * @return A parsed boolean
   * @throws NullPointerException if the property was not found
   */
  public final boolean getBoolean(final String property) {
    final String val = properties.get(property).trim().toUpperCase();
    if (val.equals("1"))
      return true;
    if (val.equals("TRUE"))
      return true;
    if (val.equals("YES"))
      return true;
    return false;
  }
  
  /**
   * Called from {@link #loadConfig} to copy the properties into the hash map
   * Tsuna points out that the Properties class is much slower than a hash
   * map so if we'll be looking up config values more than once, a hash map
   * is the way to go 
   * @param props The loaded Properties object to copy
   */
  private void loadHashMap(final Properties props) {
    properties.clear();
    
    @SuppressWarnings("rawtypes")
    Enumeration e = props.propertyNames();
    while (e.hasMoreElements()) {
      String key = (String) e.nextElement();
      properties.put(key, props.getProperty(key));
    }
  }
}
