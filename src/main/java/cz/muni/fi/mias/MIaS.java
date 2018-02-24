package cz.muni.fi.mias;

import cz.muni.fi.mias.indexing.Indexing;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * Main class witch main method.
 *
 * @author Martin Liska
 * @since 14.5.2010
 */
public class MIaS {

    public static void main(String[] args) {
        Options options = Settings.getMIaSOptions();

        // ES initialize a client
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));

        try {
            if (args.length == 0) {
                printHelp(options);
                System.exit(1);
            }
            
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);
            
            Settings.init(cmd.getOptionValue(Settings.OPTION_CONF));
            
            if (cmd.hasOption(Settings.OPTION_ADD)) {
                Indexing i = new Indexing(client);
                i.createIndex();
                i.indexFiles(cmd.getOptionValues(Settings.OPTION_ADD)[0], cmd.getOptionValues(Settings.OPTION_ADD)[1]);
            }
            if (cmd.hasOption(Settings.OPTION_OVERWRITE)) {
                Indexing i = new Indexing(client);
                i.deleteIndexDir();
                i.createIndex();
                i.indexFiles(cmd.getOptionValues(Settings.OPTION_OVERWRITE)[0], cmd.getOptionValues(Settings.OPTION_OVERWRITE)[1]);
            }
            if (cmd.hasOption(Settings.OPTION_OPTIMIZE)) {
                Indexing i = new Indexing(client);
                i.optimize();
            }
            if (cmd.hasOption(Settings.OPTION_DELETEINDEX)) {
                Indexing i = new Indexing(client);
                i.deleteIndexDir();
            }
            if (cmd.hasOption(Settings.OPTION_DELETE)) {
                Indexing i = new Indexing(client);
                i.deleteFiles(cmd.getOptionValue(Settings.OPTION_DELETE));
            }
            if (cmd.hasOption(Settings.OPTION_STATS)) {
                Indexing i = new Indexing(client);
                i.getStats();
            }
            if (cmd.hasOption(Settings.OPTION_INDOCPROCESS)) {
                InDocProcessing idp = new InDocProcessing(cmd.getOptionValues(Settings.OPTION_INDOCPROCESS)[0], cmd.getOptionValues(Settings.OPTION_INDOCPROCESS)[1]);
                idp.process();
            }

            client.close();

        } catch (ParseException ex) {
            printHelp(options);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void printHelp(Options options) {        
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "MIaS", options );
    }
}
