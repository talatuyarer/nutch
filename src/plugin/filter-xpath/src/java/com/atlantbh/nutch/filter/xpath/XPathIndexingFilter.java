package com.atlantbh.nutch.filter.xpath;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.Bytes;

import com.atlantbh.nutch.filter.xpath.config.FieldType;
import com.atlantbh.nutch.filter.xpath.config.XPathFilterConfiguration;
import com.atlantbh.nutch.filter.xpath.config.XPathIndexerProperties;
import com.atlantbh.nutch.filter.xpath.config.XPathIndexerPropertiesField;

/**
 * Second stage of {@link XPathHtmlParserFilter} the IndexingFilter.
 * It takes the prepared data located in the metadata and indexes
 * it to solr.
 * 
 * 
 * @author Emir Dizdarevic
 * @version 1.4
 * @since Apache Nutch 1.4
 *
 */
public class XPathIndexingFilter implements IndexingFilter {

	// Constants
	private static final String CONFIG_FILE_PROPERTY = "parser.xmlhtml.file";
	private static final Logger log = Logger.getLogger(XPathIndexingFilter.class);
	
	// Configuration
	private Configuration configuration;
	private XPathFilterConfiguration xpathFilterConfiguration;
	
	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	static {
		  FIELDS.add(WebPage.Field.METADATA);
	}	
	
	public XPathIndexingFilter() {}
	
	private void initConfig() {
		
		// Initialize configuration
		xpathFilterConfiguration  = XPathFilterConfiguration.getInstance(configuration);
	}
	
	@Override
	public Configuration getConf() {
		return configuration;
	}

	@Override
	public void setConf(Configuration configuration) {
		this.configuration = configuration;
		initConfig();
	}

	@Override
	public NutchDocument filter(NutchDocument doc, String url, WebPage page)
			throws IndexingException {
	  Map<CharSequence, ByteBuffer> metadata = page.getMetadata(); 
	  
		List<XPathIndexerProperties> xPathIndexerPropertiesList = xpathFilterConfiguration.getXPathIndexerPropertiesList();
		for(XPathIndexerProperties xPathIndexerProperties : xPathIndexerPropertiesList) {
			
			if(FilterUtils.isMatch(xPathIndexerProperties.getPageUrlFilterRegex(), url)) {

				List<XPathIndexerPropertiesField> xPathIndexerPropertiesFieldList = xPathIndexerProperties.getXPathIndexerPropertiesFieldList();
				for(XPathIndexerPropertiesField xPathIndexerPropertiesField : xPathIndexerPropertiesFieldList) {
					
//					ByteBuffer buffer = page.getFromMetadata(new Utf8(xPathIndexerPropertiesField.getName()));
					
					 ByteBuffer rawMetaData = metadata.get(new Utf8(xPathIndexerPropertiesField.getName()));
		        String arr = new String(Bytes.toBytes(rawMetaData));
		        String[] arra = arr.split("½é½");
					
					/*
					String stringValue = "";
					if(buffer != null) 
						stringValue = Bytes.toString(buffer.array());
					*/
		        for(String stringValue : arra)
		          doc.add(xPathIndexerPropertiesField.getName(), stringValue);
				}
			}
		}
		
		return doc;
	}

	@Override
	public Collection<Field> getFields() {
		return FIELDS;
	}
}
