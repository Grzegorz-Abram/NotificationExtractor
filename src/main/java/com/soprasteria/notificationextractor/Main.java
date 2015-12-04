package com.soprasteria.notificationextractor;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

/**
 * Main class of the application.
 *
 * @author sgacka
 */
public class Main {

    static Logger logger = Logger.getLogger(Main.class);

    /**
     * @param args
     *            the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        // args = new String[]{"U:\\config.cfg", "-R"};

        Configuration.start = Calendar.getInstance().getTime();

        if (args.length > 3 || args.length < 1) {
            printUsage();

            System.exit(0);
        } else {
            if (args.length == 2) {
                if (args[1].toUpperCase().equals("-R")) {
                    Configuration.isReadOnly = Boolean.TRUE;
                } else if (args[1].toUpperCase().equals("-NA")) {
                    Configuration.ignoreAttachments = Boolean.TRUE;
                } else if (args[1].toUpperCase().startsWith("-LA")) {
                    Configuration.limitAttachments = Boolean.TRUE;

                    parseAttachmentLimit(args[1]);
                } else {
                    printUsage();

                    System.exit(0);
                }
            } else if (args.length == 3) {
                if (args[1].toUpperCase().equals("-R") && args[2].toUpperCase().equals("-NA")) {
                    Configuration.isReadOnly = Boolean.TRUE;
                    Configuration.ignoreAttachments = Boolean.TRUE;
                } else if (args[1].toUpperCase().equals("-R") && args[2].toUpperCase().startsWith("-LA")) {
                    Configuration.isReadOnly = Boolean.TRUE;
                    Configuration.limitAttachments = Boolean.TRUE;

                    parseAttachmentLimit(args[2]);
                } else {
                    printUsage();

                    System.exit(0);
                }
            } else {
                // go standard
            }
        }

        if (Configuration.loadConfigurationFile(args[0])) {
            logger.info("<------------  Starting Notification Extractor 1.3.1  ------------>");
            logger.info("Getting notifications for: " + Configuration.customer_tool);

            processing();
        } else {
            printUsage();

            System.exit(1);
        }
    }

    /**
     * Stars processing eventout table.
     *
     * @throws Exception
     * @throws InterruptedException
     */
    private static void processing() throws Exception, InterruptedException {
        if (Configuration.isReadOnly) {
            if (Configuration.ignoreAttachments) {
                logger.info("Process information: eventout is in read-only mode, attachments are ingored");
            } else if (Configuration.limitAttachments) {
                logger.info("Process information: eventout is in read-only mode, attachments limit: " + Configuration.attachmentLimit
                        + " file(s)");
            } else {
                logger.info("Process information: eventout is in read-only mode, attachments are processed");
            }
        } else {
            if (Configuration.ignoreAttachments) {
                logger.info("Process information: eventout is in read-write mode, attachments are ingored");
            } else if (Configuration.limitAttachments) {
                logger.info("Process information: eventout is in read-write mode, attachments limit: " + Configuration.attachmentLimit
                        + " file(s)");
            } else {
                logger.info("Process information: eventout is in read-write mode, attachments are processed");
            }
        }

        Database db = new Database(Configuration.db_user, Configuration.db_password, Configuration.db_host, Configuration.db_port,
                Configuration.db_sid);
        db.connect();

        EventOut eventOut = new EventOut(db);
        eventOut.getEventOut();

        if (eventOut.getRecordsCount() > 0) {
            logger.info("Starting eventout processing...");
            Date start = Calendar.getInstance().getTime();

            ExecutorService threadExecutor = Executors.newFixedThreadPool(100);
            ArrayList<EventOutRecord> eventRecords = new ArrayList<EventOutRecord>();

            EventOutRecord eor;
            for (int index = 0; index < eventOut.getRecordsCount(); index++) {
                eor = eventOut.getEventOutRecord(index);
                eventRecords.add(eor);
            }

            // only execute() can be here!
            for (int index = 0; index < eventRecords.size(); index++) {
                threadExecutor.execute(eventRecords.get(index));
            }

            threadExecutor.shutdown();
            while (!threadExecutor.isTerminated()) {
                threadExecutor.awaitTermination(1, TimeUnit.SECONDS);
            }

            Date stop = Calendar.getInstance().getTime();
            logger.info("Eventout processing complete in " + (double) (stop.getTime() - start.getTime()) / 1000 + " seconds");

            logger.info(Configuration.getNotificationsCount());
            Configuration.stop = Calendar.getInstance().getTime();
            logger.info("Execution time: " + (double) (Configuration.stop.getTime() - Configuration.start.getTime()) / 1000 + " seconds");
            logger.info("SUCCESS. Application ended with success.");
        } else {
            logger.info("No records found");

            Configuration.stop = Calendar.getInstance().getTime();
            logger.info("Execution time: " + (double) (Configuration.stop.getTime() - Configuration.start.getTime()) / 1000 + " seconds");
            logger.info("SUCCESS. Application ended with success.");
        }
    }

    /**
     * Prints usage information for this application.
     */
    private static void printUsage() {
        System.out.println("\r\nCorrect usage:\tjava -jar NotificationExtractor [CONFIG_PATH] <MODE> <ATTACHMENT>");
        System.out.println("where:\r\nCONFIG_PATH is:\r\n\tpath to the configuration file");
        System.out.println("MODE is:\r\n\t-R\tread only (optional)");
        System.out.println(
                "ATTACHMENT is:\r\n\t-NA\tignore attachments (optional)\r\n\t-LA:<num>\tlimit number of attachments to <num> value (optional)");
    }

    /**
     * Parse number provided in limit parameter for attachments.
     *
     * @param limitParam
     *            String with provided parameter
     */
    private static void parseAttachmentLimit(String limitParam) {
        if (limitParam.length() > 3) {
            String temp = limitParam.substring(4, limitParam.length());

            Configuration.attachmentLimit = Configuration.getNumber(temp);
        }
    }
}
