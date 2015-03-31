/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.reidin.ppd.listings.date;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;
import org.apache.solr.common.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;

/**
 * Converts listing's publish date to Reidin date format which is "yyyy-MM-dd". 
 */
public class DateListingsFilter implements IndexingFilter {
  public static final Logger LOG = LoggerFactory
      .getLogger(DateListingsFilter.class);

  private Configuration conf;
  
  private static final String DATE_CONFIG_FILE = "date.listingsfilter.file";
  private static final String DATE_DEFAULT_FORMAT = "date.listingsfilter.default.format";
  private static final String DATE_OUTPUT_FORMAT = "date.listingsfilter.output.format";
  private static final String DATE_DEFAULT_LOCALE = "date.listingsfilter.default.locale";
  
  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  private static HashMap<String, HashMap<String, String>> rules;

  static {
    FIELDS.add(WebPage.Field.METADATA);
}

  /**
   * The {@link DateListingsFilter} filter object supports configurable values for host specific
   * date format and date local
   * @see {@code date.listingsfilter.file}, {@code date.listingsfilter.default.format} and
   *      {@code date.listingsfilter.default.locale} in nutch-default.xml
   * @param doc The {@link NutchDocument} object
   * @param url URL to be filtered for Publish Date
   * @param page {@link WebPage} object relative to the URL
   * @return filtered NutchDocument
   */
  public NutchDocument filter(NutchDocument doc, String url, WebPage page)
 throws IndexingException {
    if (doc == null) {
      return doc;
    }
    String reprUrl = null;
    reprUrl = TableUtil.toString(page.getReprUrl());

    String host = null;
    try {
      URL u;
      if (reprUrl != null) {
        u = new URL(reprUrl);
      } else {
        u = new URL(url);
      }
      host = u.getHost();
    } catch (MalformedURLException e) {
      throw new IndexingException(e);
    }

    HashMap<String, String> rule = this.rules.get(host);
    String format = conf.get(DATE_DEFAULT_FORMAT, "dd-MM-yyyy");
    if (rule.containsKey("format")) {
      format = rule.get("format");
    }

    String locale = conf.get(DATE_DEFAULT_LOCALE, "US");
    if (rule.containsKey("locale")) {
      locale = rule.get("locale");
    }

    String rawDate = Bytes.toString(page.getMetadata().get("pd"));

    try {
      Date date = new SimpleDateFormat(format, Locale.forLanguageTag(locale)).parse(rawDate);
      String outputFormat = conf.get(DATE_OUTPUT_FORMAT, "yyyy-MM-dd");  
      String formattedDate = new SimpleDateFormat(outputFormat).format(date);
      Long reverseDate = Long.MAX_VALUE - date.getTime();
      doc.add("listingDate", formattedDate);
      doc.add("reverseDate", reverseDate.toString());
    } catch (ParseException e) {
      LOG.warn("Could not format date:" + url);
      doc = null;
    }

    return doc;
  }

  /**
   * Set the {@link Configuration} object
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
    try {
      this.rules = readRules(conf);
    } catch (IOException e) {
      LOG.error("Could not read configuration file.");
    }
  }

  private HashMap<String, HashMap<String, String>> readRules(Configuration conf) throws IOException{
    BufferedReader reader = new BufferedReader(new InputStreamReader(conf.getConfResourceAsInputStream(conf.get(DATE_CONFIG_FILE))));
    String line = null;
    HashMap<String, HashMap<String, String>> map = new HashMap<String, HashMap<String, String>>();
    while ((line = reader.readLine()) != null) {
        if (!line.startsWith("#") && line.contains("=") && line.contains("|")) {
            String[] strings = line.split("=");
            String[] keys = strings[0].split("\\|");
            HashMap<String, String> values = map.get(keys[0]);          
            if (values == null){
              values = new HashMap<String, String>();
            }
            values.put(keys[1], strings[1]);
            map.put(keys[0], values);
        }
    }
    return map;
    
  }
  
  /**
   * Get the {@link Configuration} object
   */
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Gets all the fields for a given {@link WebPage} Many datastores need to
   * setup the mapreduce job by specifying the fields needed. All extensions
   * that work on WebPage are able to specify what fields they need.
   */
  @Override
  public Collection<WebPage.Field> getFields() {
    return FIELDS;
  }

  public static void main(String[] args) throws ParseException {
//    String oldstring = "27 11 2015";
//    Date date = new SimpleDateFormat("dd-MM-yyyy", Locale.forLanguageTag("US")).parse(oldstring); 
//    
//    String newstring = new SimpleDateFormat("yyyy-MM-dd").format(date);
//    System.out.println(newstring); // 2011-01-18
    
//    DateListingsFilter dlf = new DateListingsFilter();
//    Configuration conf = NutchConfiguration.create();
//    try {
//      dlf.readRules(conf);
//    } catch (IOException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
}
  
}
