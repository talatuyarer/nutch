package com.atlantbh.nutch.filter.xpath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseFilter;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.EncodingDetector;
import org.htmlcleaner.CleanerProperties;
import org.htmlcleaner.DomSerializer;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;
import org.jaxen.JaxenException;
import org.jaxen.XPath;
import org.jaxen.dom.DOMXPath;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.atlantbh.nutch.filter.xpath.config.XPathFilterConfiguration;
import com.atlantbh.nutch.filter.xpath.config.XPathIndexerProperties;
import com.atlantbh.nutch.filter.xpath.config.XPathIndexerPropertiesField;

/**
 * A Xml-Html xpath filter implementation that fetches data
 * from the content, depending on the supplied xpath,
 * and prepares it for the {@link XPathIndexingFilter} to
 * index it into solr.
 * 
 * @author Emir Dizdarevic
 * @version 1.4
 * @since Apache Nutch 1.4
 */
public class XPathHtmlParserFilter implements ParseFilter {
	
	// Constants
	private static final Logger log = Logger.getLogger(XPathHtmlParserFilter.class);
	private static final List<String> htmlMimeTypes = Arrays.asList(new String[] {"text/html", "application/xhtml+xml"});
	
	// OLD WAY TO DETERMIN IF IT'S AN XML FORMAT
	//private static final List<String> xmlMimeTypes = Arrays.asList(new String[] {"text/xml", "application/xml"});
	
	// Configuration
	private Configuration configuration;
	private XPathFilterConfiguration xpathFilterConfiguration;
	private String defaultEncoding;
	
	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	static {
	    FIELDS.add(WebPage.Field.METADATA);
	}
	
	// Internal data
	private HtmlCleaner cleaner;
	private DomSerializer domSerializer;
	private DocumentBuilder documentBuilder;

	public XPathHtmlParserFilter() {
		init();
	}

	private void init() {
		
		// Initialize HTMLCleaner
		cleaner = new HtmlCleaner();
		CleanerProperties props = cleaner.getProperties();
		props.setAllowHtmlInsideAttributes(true);
		props.setAllowMultiWordAttributes(true);
		props.setRecognizeUnicodeChars(true);
		props.setOmitComments(true);
		props.setNamespacesAware(false);
		
		// Initialize DomSerializer
		domSerializer = new DomSerializer(props);
		
		// Initialize xml parser		
		try {
			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			documentBuilder = documentBuilderFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			// THIS CAN NEVER HAPPEN
		}
	}
	
	private void initConfig() {

		// Initialize configuration
		xpathFilterConfiguration  = XPathFilterConfiguration.getInstance(configuration);
		defaultEncoding = configuration.get("parser.character.encoding.default", "UTF-8");
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
	
	private void removeAll(Node node, short nodeType, String name) {
	    if (node.getNodeType() == nodeType && (name == null || node.getNodeName().equals(name))) {
	    	node.getParentNode().removeChild(node);
	    } 
	    else {
	    	NodeList list = node.getChildNodes();
	    	for (int i = 0; i < list.getLength(); i++) {
	    		removeAll(list.item(i), nodeType, name);
	    	}
	    }
	}
	
	
	@Override
	public Parse filter(String url, WebPage page, Parse parse,
			HTMLMetaTags metaTags, DocumentFragment doc) {

		byte[] rawContent = page.getContent().array();
		Metadata metadata = new Metadata();
		try {
			Document cleanedXmlHtml = documentBuilder.newDocument();
			if(htmlMimeTypes.contains(page.getContentType().toString())) {
		
				
				String encoding = defaultEncoding;
				ByteBuffer buffer = page.getMetadata().get(new Utf8(Metadata.ORIGINAL_CHAR_ENCODING));
				if(buffer != null)
					encoding = Bytes.toString(buffer.array());
				
				// Create reader so the input can be read in UTF-8
				Reader rawContentReader = new InputStreamReader(new ByteArrayInputStream(rawContent), encoding);
				
				// Use the cleaner to "clean" the HTML and return it as a TagNode object
				TagNode tagNode = cleaner.clean(rawContentReader);
				cleanedXmlHtml = domSerializer.createDOM(tagNode);
			} else if(page.getContentType().toString().contains(new StringBuilder("/xml")) || page.getContentType().toString().contains(new StringBuilder("+xml"))) {
				
				// Parse as xml - don't clean
				cleanedXmlHtml = documentBuilder.parse(new InputSource(new ByteArrayInputStream(rawContent)));	
			} 
			
			removeAll(cleanedXmlHtml, Node.ELEMENT_NODE, "script");
			
			cleanedXmlHtml.normalize();
			
			// Once the HTML is cleaned, then you can run your XPATH expressions on the node, 
			// which will then return an array of TagNode objects 
			List<XPathIndexerProperties> xPathIndexerPropertiesList = xpathFilterConfiguration.getXPathIndexerPropertiesList();
			for(XPathIndexerProperties xPathIndexerProperties : xPathIndexerPropertiesList) {
				
				
				//****************************
				// CORE XPATH EVALUATION
				//****************************
				if(pageToProcess(xPathIndexerProperties, cleanedXmlHtml, page.getBaseUrl().toString())) {
					
					List<XPathIndexerPropertiesField> xPathIndexerPropertiesFieldList = xPathIndexerProperties.getXPathIndexerPropertiesFieldList();
					for(XPathIndexerPropertiesField xPathIndexerPropertiesField : xPathIndexerPropertiesFieldList) {
						
						// Evaluate xpath			
						XPath xPath = new DOMXPath(xPathIndexerPropertiesField.getXPath());
						List nodeList = xPath.selectNodes(cleanedXmlHtml);
						
						// Trim?
						boolean trim = FilterUtils.getNullSafe(xPathIndexerPropertiesField.getTrimXPathData(), true);
						
						if(FilterUtils.getNullSafe(xPathIndexerPropertiesField.isConcat(), false)) {
								
							// Iterate trough all found nodes
							String value = new String();
							String concatDelimiter = FilterUtils.getNullSafe(xPathIndexerPropertiesField.getConcatDelimiter(), "");
							for (Object node : nodeList) {

								// Extract data	
								String tempValue = FilterUtils.extractTextContentFromRawNode(node);
								tempValue = filterValue(tempValue, trim);
								
								// Concatenate tempValue to value
								if(tempValue != null) {
									if(value.isEmpty()) {
										value = tempValue;
									} else {
										value = value + concatDelimiter + tempValue;
									}
								}
							}
							
							// Add the extracted data to meta
							if(value != null) {
							  page.getMetadata().put(new Utf8(xPathIndexerPropertiesField.getName()), ByteBuffer.wrap(value.getBytes()));
							}
							
						} else {
							
							// Iterate trough all found nodes
							for (Object node : nodeList) {

								// Add the extracted data to meta
								String value = FilterUtils.extractTextContentFromRawNode(node);					
								value = filterValue(value, trim);
								if(value != null) {
								    page.getMetadata().put(new Utf8(xPathIndexerPropertiesField.getName()), ByteBuffer.wrap(value.getBytes()));
								}
							}
						}
						
					}
				}
			}
			
		} catch (IOException e) {
			// This can never happen because it's an in memory stream
		} catch(PatternSyntaxException e) {
			System.err.println(e.getMessage());
			log.error("Error parsing urlRegex: " + e.getMessage());
			return ParseStatusUtils.getEmptyParse(ParseStatusCodes.FAILED, page.getBaseUrl().toString(), configuration);
		} catch (ParserConfigurationException e) {
			System.err.println(e.getMessage());
			log.error("HTML Cleaning error: " + e.getMessage());
			return ParseStatusUtils.getEmptyParse(ParseStatusCodes.FAILED, page.getBaseUrl().toString(), configuration);
		} catch (SAXException e) {
			System.err.println(e.getMessage());
			log.error("XML parsing error: " + e.getMessage());
			return ParseStatusUtils.getEmptyParse(ParseStatusCodes.FAILED, page.getBaseUrl().toString(), configuration);
		} catch (JaxenException e) {
			System.err.println(e.getMessage());
			log.error("XPath error: " + e.getMessage());
			return ParseStatusUtils.getEmptyParse(ParseStatusCodes.FAILED, page.getBaseUrl().toString(), configuration);
		}
		
		return parse;
	}
	
	private boolean pageToProcess(XPathIndexerProperties xPathIndexerProperties, Document cleanedXmlHtml, String url) throws JaxenException {

		boolean processPage = true;

		// *************************************
		// URL REGEX CONTENT PAGE FILTERING
		// *************************************
		processPage = processPage && FilterUtils.isMatch(xPathIndexerProperties.getPageUrlFilterRegex(), url);

		// Check return status
		if (!processPage) {
			return false;
		}

		// *************************************
		// XPATH CONTENT PAGE FILTERING
		// *************************************

		if (xPathIndexerProperties.getPageContentFilterXPath() != null) {
			XPath xPathPageContentFilter = new DOMXPath(xPathIndexerProperties.getPageContentFilterXPath());
			List pageContentFilterNodeList = xPathPageContentFilter.selectNodes(cleanedXmlHtml);
			boolean trim = FilterUtils.getNullSafe(xPathIndexerProperties.isTrimPageContentFilterXPathData(), true);
			
			if (FilterUtils.getNullSafe(xPathIndexerProperties.isConcatPageContentFilterXPathData(), false)) {

				// Iterate trough all found nodes
				String value = new String();
				String concatDelimiter = FilterUtils.getNullSafe(xPathIndexerProperties.getConcatPageContentFilterXPathDataDelimiter(), "");

				for (Object node : pageContentFilterNodeList) {

					// Extract data
					String tempValue = FilterUtils.extractTextContentFromRawNode(node);
					tempValue = filterValue(tempValue, trim);

					// Concatenate tempValue to value
					if(tempValue != null) {
						if (value.isEmpty()) {
							value = tempValue;
						} else {
							value = value + concatDelimiter + tempValue;
						}
					}
				}

				processPage = processPage && FilterUtils.isMatch(xPathIndexerProperties.getPageContentFilterRegex(), value);
			} else {
				for (Object node : pageContentFilterNodeList) {

					// Add the extracted data to meta
					String value = FilterUtils.extractTextContentFromRawNode(node);
					value = filterValue(value, trim);
					if(value != null) {
						processPage = processPage && FilterUtils.isMatch(xPathIndexerProperties.getPageContentFilterRegex(), value);
					}
				}
			}
		}

		return processPage;
	}
	
	private String filterValue(String value, boolean trim) {

		String returnValue = null;
		
		// Filter out empty strings and strings made of space, carriage return and tab characters
		if(!value.isEmpty() && !FilterUtils.isMadeOf(value, " \n\t")) {
			
			// Trim data?
			returnValue = trimValue(value, trim);
		}
		
		return returnValue == null ? null : StringEscapeUtils.unescapeHtml(returnValue);
	}
	
	private String trimValue(String value, boolean trim) {
		
		String returnValue;
		if (trim) {
			returnValue = value.trim();
		} else {
			returnValue = value;
		}
		
		return returnValue;
	}

	@Override
	public Collection<Field> getFields() {
		return FIELDS;
	}
}
