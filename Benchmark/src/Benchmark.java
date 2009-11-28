import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
 * Benchmark Client for the Wahlinformationssystem. For additional information on how to modify
 * the configuration file please consult the documentation.
 * 
 * Usage: java Benchmark configuration.xml
 * 
 * @author Tobias Muehlbauer
 * @author Wolf Roediger
 */
public class Benchmark {
    private static Logger LOGGER = LogManager.getLogger("Benchmark.class");
    public static final String DATE_FORMAT_NOW = "yyyy-MM-dd-HH-mm";
    
    public static void main(String[] args) {
        PropertyConfigurator.configure("bin/log4j.configuration");
        
        if (args.length != 1) {
            LOGGER.error("You must specify a configuration file!");
            System.exit(-1);
        }
        
        try {
            int terminals = 0;
            float latencyExpectation;
            
            DocumentBuilderFactory dobuf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db;
            db = dobuf.newDocumentBuilder();
            Document doc = db.parse(args[0]);
            doc.getDocumentElement().normalize();
            
            NodeList numTerminals = doc.getElementsByTagName("numberofterminals");
            if (numTerminals.getLength() == 1) {
                terminals = Integer.parseInt(numTerminals.item(0).getTextContent());
                LOGGER.debug(String.format("Number of terminals: %s", terminals));
            } else {
                LOGGER.error("Ambiguous or missing specification of number of terminals!");
                System.exit(-1);
            }
            
            NodeList latExpectation = doc.getElementsByTagName("latencyexpectation");
            if (latExpectation.getLength() == 1) {
                latencyExpectation = Float.parseFloat(latExpectation.item(0).getTextContent());
                LOGGER.debug(String.format("Latency expectation: %s", latencyExpectation));
            } else {
                LOGGER.error("Ambiguous or missing specification of latency expectation!");
                System.exit(-1);
            }
            
            ArrayList<WebsiteConfiguration> websiteConfigurations
                                                        = new ArrayList<WebsiteConfiguration>();
            NodeList websites = doc.getElementsByTagName("website");
            for (int i = 0; i < websites.getLength(); i++) {
                NodeList children = websites.item(i).getChildNodes();
                String url = new String();
                List<String> supplementaryURLs = new LinkedList<String>();
                int frequency = 0;
                for (int j = 0; j < children.getLength(); j++) {
                    if (children.item(j).getNodeName().equals("url")) {
                        url = children.item(j).getTextContent();
                    } else if (children.item(j).getNodeName().equals("frequency")) {
                        frequency = Integer.parseInt(children.item(j).getTextContent());
                    } else if (children.item(j).getNodeName().equals("supplementaryurl")) {
                        supplementaryURLs.add(children.item(j).getTextContent());
                    }
                }
                if (!url.isEmpty()) {
                    for (int j = 0; j < frequency; j++) {
                        websiteConfigurations.add(new WebsiteConfiguration(url,
                                supplementaryURLs));
                    }
                }
            }
            
            BasicHttpParams params = new BasicHttpParams();
            SchemeRegistry schemeRegistry = new SchemeRegistry();
            schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));

            ClientConnectionManager cm = new ThreadSafeClientConnManager(params, schemeRegistry);
            HttpClient httpClient = new DefaultHttpClient(cm, params);

            BenchmarkThread[] threads = new BenchmarkThread[terminals];
            for (int i = 0; i < terminals; i++) {
                threads[i] = new BenchmarkThread(httpClient,
                        new ArrayList<WebsiteConfiguration>(
                                websiteConfigurations));
            }
            
            for (int i = 0; i < terminals; i++) {
                threads[i].start();
            }

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
     * Benchmark thread that shuffles the order in which websites are called, establishes HTTP
     * connections and measures response times.
     */
    private static class BenchmarkThread extends Thread {
        private final HttpClient httpClient;
        private final HttpContext context;
        private final ArrayList<WebsiteConfiguration> configurations;
        
        public BenchmarkThread(HttpClient httpClient, ArrayList<WebsiteConfiguration> configurations) {
            this.httpClient = httpClient;
            this.context = new BasicHttpContext();
            this.configurations = configurations;
            Collections.shuffle(configurations);
        }
        
        @Override
        public void run() {
            try {
                for (WebsiteConfiguration config : configurations) {
                    long start = System.currentTimeMillis();
                    HttpGet getURL = new HttpGet(config.getURL());
                    HttpResponse responseURL = httpClient.execute(getURL, context);
                    HttpEntity entityURL = responseURL.getEntity();
                    entityURL.consumeContent();
                    for (String url : config.getSupplementaryURLs()) {
                        HttpGet get = new HttpGet(url);
                        HttpResponse response = httpClient.execute(get, context);
                        HttpEntity entity = response.getEntity();
                        entity.consumeContent();
                    }
                    LOGGER.info(String.format("%s,%s", config.getURL(), System
                            .currentTimeMillis()
                            - start));
                }
            } catch (Exception exc) {
                LOGGER.error("An error occured during the benchmark!", exc);
            }
        }
    }
    
    /**
     * A website configuration that is processed by the benchmark threads.
     */
    private static class WebsiteConfiguration {
        private String url;
        private List<String> supplementaryURLs;
        
        public WebsiteConfiguration(String url, List<String> supplementaryURLs) {
            this.url = url;
            this.supplementaryURLs = supplementaryURLs;
        }
        
        public String getURL() {
            return url;
        }
        
        public List<String> getSupplementaryURLs() {
            return supplementaryURLs;
        }
    }
}
