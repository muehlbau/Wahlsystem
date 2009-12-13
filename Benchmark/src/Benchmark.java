import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Benchmark client for the Wahlinformationssystem. For additional information on how to tweak
 * the configuration file please consult the documentation.
 * 
 * Usage: java Benchmark configuration.xml
 * 
 * @author Tobias Muehlbauer
 * @author Wolf Roediger
 */
public class Benchmark {
	/**
	 * Logger for benchmark results.
	 */
    private static Logger LOGGER = LogManager.getLogger("Benchmark.class");
    
    /**
     * Reads configuration file and creates terminals according to it.
     * @param args Configuration XML.
     */
    public static void main(String[] args) {
    	// Configure logger.
        PropertyConfigurator.configure("bin/log4j.configuration");
        
        // Check for configuration XML.
        if (args.length != 1) {
            LOGGER.error("You must specify a configuration file!");
            System.exit(-1);
        }
        
        try {
            int terminals = 0;
            float latencyExpectation = 0;
            int cycleCount = 0;
            
            // Parse configuration XML.
            DocumentBuilderFactory dobuf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db;
            db = dobuf.newDocumentBuilder();
            Document doc = db.parse(args[0]);
            doc.getDocumentElement().normalize();
            
            // Read number of terminals.
            NodeList numTerminals = doc.getElementsByTagName("numberOfTerminals");
            if (numTerminals.getLength() == 1) {
                terminals = Integer.parseInt(numTerminals.item(0).getTextContent());
                LOGGER.debug(String.format("Number of terminals: %s", terminals));
            } else {
                LOGGER.error("Ambiguous or missing specification of number of terminals!");
                System.exit(-1);
            }
            
            // Read latency.
            NodeList latExpectation = doc.getElementsByTagName("latencyExpectation");
            if (latExpectation.getLength() == 1) {
                latencyExpectation = Float.parseFloat(latExpectation.item(0).getTextContent());
                LOGGER.debug(String.format("Latency expectation: %s", latencyExpectation));
            } else {
                LOGGER.error("Ambiguous or missing specification of latency expectation!");
                System.exit(-1);
            }
            
            // Read cycle count.
            NodeList numCycles = doc.getElementsByTagName("cycleCount");
            if (numCycles.getLength() == 1) {
            	cycleCount = Integer.parseInt(numCycles.item(0).getTextContent());
                LOGGER.debug(String.format("Number of cycles: %s", cycleCount));
            } else {
                LOGGER.error("Ambiguous or missing specification of number of cycles!");
                System.exit(-1);
            }
            
            // Determine gcd of probability of the different URLs. (for minimal card deck)
            NodeList websites = doc.getElementsByTagName("website");
            int gcd = 0;
            for (int i = 0; i < websites.getLength(); i++) {
                NodeList children = websites.item(i).getChildNodes();
                // Read frequency of each URL.
                for (int j = 0; j < children.getLength(); j++) {
                    if (children.item(j).getNodeName().equals("probability")) {
                        gcd = GCD(gcd, Integer.parseInt(children.item(j).getTextContent()));
                    }
                }
            }
            
            // Create website configurations for each website to be tested.
            ArrayList<WebsiteConfiguration> websiteConfigurations
                                                        = new ArrayList<WebsiteConfiguration>();
            for (int i = 0; i < websites.getLength(); i++) {
                NodeList children = websites.item(i).getChildNodes();
                String url = new String();
                List<String> supplementaryURLs = new LinkedList<String>();
                int probability = 0;
                // Read main URL, frequency and supplementary URLs.
                for (int j = 0; j < children.getLength(); j++) {
                    if (children.item(j).getNodeName().equals("url")) {
                        url = children.item(j).getTextContent();
                    } else if (children.item(j).getNodeName().equals("probability")) {
                    	probability = Integer.parseInt(children.item(j).getTextContent()) 
                        			/ ((gcd != 0) ? gcd : 1);
                    } else if (children.item(j).getNodeName().equals("supplementaryUrl")) {
                        supplementaryURLs.add(children.item(j).getTextContent());
                    }
                }
                // Add website to configurations.
                if (!url.isEmpty()) {
                    for (int j = 0; j < probability; j++) {
                        websiteConfigurations.add(new WebsiteConfiguration(url,
                                supplementaryURLs));
                    }
                }
            }

            // Create HTTP client.
            BasicHttpParams params = new BasicHttpParams();
            SchemeRegistry schemeRegistry = new SchemeRegistry();
            schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));

            ClientConnectionManager cm = new ThreadSafeClientConnManager(params, schemeRegistry);
            HttpClient httpClient = new DefaultHttpClient(cm, params);

            // Create terminals.
            BenchmarkThread[] threads = new BenchmarkThread[terminals];
            for (int i = 0; i < terminals; i++) {
                threads[i] = new BenchmarkThread(httpClient,
                		new ArrayList<WebsiteConfiguration>(websiteConfigurations),
                		latencyExpectation, cycleCount);
            }
            
            // Start terminals.
            for (int i = 0; i < terminals; i++) {
                threads[i].start();
            }

            // Wait for terminals to finish.
            for (int i = 0; i < terminals; i++) {
                threads[i].join();
            }
        } catch (ParserConfigurationException pce) {
            LOGGER.error("Serious configuration error!", pce);
        } catch(SAXException se) {
            LOGGER.error("The configuration file cannot be parsed!", se);
        } catch(IOException ioe) {
            LOGGER.error("I/O of configuration file interrupted!", ioe);
        } catch(NumberFormatException nfe) {
            LOGGER.error("Cannot parse number!", nfe);
        } catch(InterruptedException ine) {
            LOGGER.error("Benchmark interrupted!", ine);
        }
    }
    
    /**
     * Determine gcd of a and b.
     * @param a First parameter.
     * @param b Second parameter.
     * @return GCD of first and second parameter.
     */
    public static int GCD(int a, int b)
    {
       if (b == 0) return a;
       return GCD(b, a % b);
    }
    
    /**
     * Benchmark thread that shuffles the order in which websites are called, establishes HTTP
     * connections and measures response times.
     */
    private static class BenchmarkThread extends Thread {
    	/** Client used to load web pages. */
        private final HttpClient httpClient;
        /** Client context. */
        private final HttpContext context;
        /** Websites to test. */
        private final ArrayList<WebsiteConfiguration> configurations;
        /** Latency between website calls. */
        private final float latencyExpectation;
        /** Cycle count. */
        private final int cycleCount;
        
        /**
         * Constructor.
         * @param httpClient Client used to load webpages.
         * @param configurations Websites to test.
         * @param latencyExpectation
         */
        public BenchmarkThread(HttpClient httpClient, ArrayList<WebsiteConfiguration> configurations,
        		float latencyExpectation, int cycleCount) {
            this.httpClient = httpClient;
            this.context = new BasicHttpContext();
            this.configurations = configurations;
            this.latencyExpectation = latencyExpectation;
            this.cycleCount = cycleCount;
        }
        
        /**
         * Run the terminal and test all websites.
         */
        @Override
        public void run() {
        	// Test website mix the defined number of times.
        	for (int i = 0; i < cycleCount; i++)
        	{
                // Shuffle websites.
                Collections.shuffle(configurations);
	            try {
	            	// Load the websites one after the other.
	                for (WebsiteConfiguration config : configurations) {
	                    long start = System.currentTimeMillis();
	                    HttpGet getURL = new HttpGet(config.getURL());
	                    HttpResponse responseURL = httpClient.execute(getURL, context);
	                    HttpEntity entityURL = responseURL.getEntity();
	                    entityURL.consumeContent();
	                    // Get all supplementary URLs.
	                    for (String url : config.getSupplementaryURLs()) {
	                        HttpGet get = new HttpGet(url);
	                        HttpResponse response = httpClient.execute(get, context);
	                        HttpEntity entity = response.getEntity();
	                        entity.consumeContent();
	                    }
	            		Date date = new Date(start);
	            		// Log result.
	                    LOGGER.info(String.format("URL: %s, Start: %tT, Time: %s ms", config.getURL(), date,
	                    		System.currentTimeMillis() - start));
	                    // Wait some time to achieve latency expectation.
	                    long timeToWait = (long) (latencyExpectation + (0.5 - Math.random()) * latencyExpectation);
	                    Thread.sleep(timeToWait);
	                }
	            } catch (Exception exc) {
	                LOGGER.error("An error occured during the benchmark!", exc);
	            }
	        LOGGER.info("End of cycle no. " + (i+1) + ".");
        	}
        }
    }
    
    /**
     * A website configuration that is processed by the benchmark threads.
     * It contains information about the URL to test and any supplementary URLs
     * which have to be loaded, too.
     */
    private static class WebsiteConfiguration {
    	/** URL to test. */
        private String url;
        /** Supplementary URLs to load. */
        private List<String> supplementaryURLs;
        
        /**
         * Constructor.
         * @param url URL to test.
         * @param supplementaryURLs Supplementary URLs to load.
         */
        public WebsiteConfiguration(String url, List<String> supplementaryURLs) {
            this.url = url;
            this.supplementaryURLs = supplementaryURLs;
        }
        
        /**
         * Get URL which is to be tested.
         * @return URL to test.
         */
        public String getURL() {
            return url;
        }
        
        /**
         * Get list of supplementary URLs.
         * @return Supplementary URLs.
         */
        public List<String> getSupplementaryURLs() {
            return supplementaryURLs;
        }
    }
}
