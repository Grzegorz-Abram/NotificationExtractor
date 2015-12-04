package com.soprasteria.notificationextractor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Date;
import java.util.Properties;
import org.apache.log4j.PropertyConfigurator;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.properties.EncryptableProperties;

/**
 * Class that represents application configuration.
 *
 * @author sgacka
 */
public class Configuration {

    /**
     * Customer tool for which ticket should be extracted
     */
    public static String customer_tool = null;
    /**
     * Database user
     */
    public static String db_user = null;
    /**
     * Database password
     */
    public static String db_password = null;
    /**
     * Database host
     */
    public static String db_host = null;
    /**
     * Database port
     */
    public static int db_port = 0;
    /**
     * Database sid
     */
    public static String db_sid = null;
    /**
     * Log path
     */
    private static String logPath = null;
    /**
     * Attachments are ignored if TRUE
     */
    public static Boolean ignoreAttachments = Boolean.FALSE;
    /**
     * Records are not removed TRUE
     */
    public static Boolean isReadOnly = Boolean.FALSE;
    /**
     * Attachments are limited if TRUE
     */
    public static Boolean limitAttachments = Boolean.FALSE;
    /**
     * Attachments limit value
     */
    public static int attachmentLimit = 0;
    /**
     * Execution time variable - start time
     */
    public static Date start;
    /**
     * Execution time variable - stop time
     */
    public static Date stop;
    /**
     * Notification Time Zone
     */
    public static String timeZoneCode = null;
    /**
     * Notification counter
     */
    private static int matchingNotificationsCount = 0;
    /**
     * Properties from config file
     */
    private static Properties properities;

    /**
     * Loads database, etc. configuration from file.
     *
     * @param configPath
     *            Path to the configuration file
     * @return TRUE if configuration was loaded successfully
     */
    public static Boolean loadConfigurationFile(String configPath) {
        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
        encryptor.setProvider(new BouncyCastleProvider());
        encryptor.setAlgorithm("PBEWITHSHA256AND256BITAES-CBC-BC");
        encryptor.setPassword("AS#*O&q5g\\/s/fg~asdf~2SH4JD345*T$[]wee");

        properities = new EncryptableProperties(encryptor);

        try {
            if (new File(configPath).exists()) {
                properities.load(new FileInputStream(configPath));
            } else {
                throw new FileNotFoundException();
            }

            // setting values
            validateBasicInfo();

            // database
            validateDatabaseInfo();

            // set log path for logger
            validatePaths();

            // set logger properties
            PropertyConfigurator.configure(properities);

            return Boolean.TRUE;
        } catch (FileNotFoundException fnfe) {
            System.out.println(
                    "\r\nNotificationExtractor - Unable to locate configuration file.\r\nPlease check if the following path is correct: \""
                            + configPath + "\"\r\n" + fnfe.toString());

            return Boolean.FALSE;
        } catch (Exception e) {
            System.out.println("\r\nNotificationExtractor - Exception while loading configuration file:\r\n" + e.toString());

            return Boolean.FALSE;
        }
    }

    /**
     * Validates basic configuration parameters.
     */
    private static void validateBasicInfo() {
        // validate customer tool
        customer_tool = properities.getProperty("customer_tool").toUpperCase();
        if (customer_tool == null || customer_tool.isEmpty()) {
            System.out.println("Customer tool name was not provided!");
            System.exit(1);
        }

        // validate notification Time Zone
        timeZoneCode = properities.getProperty("tz_code").toUpperCase();
        if (timeZoneCode == null || timeZoneCode.isEmpty()) {
            timeZoneCode = "UTC";
        }
    }

    /**
     * Validates database configuration parameters.
     */
    private static void validateDatabaseInfo() {
        db_user = properities.getProperty("db_user");
        db_password = properities.getProperty("db_password");
        db_host = properities.getProperty("db_host");
        db_port = getNumber(properities.getProperty("db_port"));
        db_sid = properities.getProperty("db_sid");

        if (db_user == null || db_user.isEmpty()) {
            System.out.println("Database user was not provided!");
            System.exit(1);
        }

        if (db_password == null || db_password.isEmpty()) {
            System.out.println("Warning! - Database password was not provided");
        }

        if (db_host == null || db_host.isEmpty()) {
            System.out.println("Database host was not provided!");
            System.exit(1);
        }

        if (db_port < 1 || db_port > 65535) {
            System.out.println("Database port is out of range!");
            System.exit(1);
        }

        if (db_sid == null || db_sid.isEmpty()) {
            System.out.println("Database user was not provided!");
            System.exit(1);
        }
    }

    /**
     * Validates provided paths.
     */
    private static void validatePaths() {
        boolean isAbsolute;

        // validate log path
        logPath = properities.getProperty("log_path");
        isAbsolute = isDirectory(logPath);

        if (!isAbsolute && (logPath == null || logPath.isEmpty())) {
            System.out.println("Log path was not provided!");
            System.exit(1);
        }

        properities.setProperty("log4j.appender.A2.file", logPath + File.separator + customer_tool + "_NOTIFICATION_EXTRACTOR.log");
    }

    /**
     * Increases matching notification counter.
     */
    public synchronized static void increaseNotificationsCount() {
        matchingNotificationsCount++;
    }

    /**
     * Gets matching notification counter.
     *
     * @return Number of notifications found
     */
    public static String getNotificationsCount() {
        return "Total notifications found: " + matchingNotificationsCount;
    }

    /**
     * Converts safely string to number.
     *
     * @param val
     *            String with number
     * @return Integer value of string
     */
    public static int getNumber(String val) {
        if (val == null || val.isEmpty()) {
            return 0;
        }

        int len = val.length();

        for (int i = 0; i < len; i++) {
            if (!Character.isDigit(val.charAt(i))) {
                return 0;
            }
        }

        return Integer.parseInt(val);
    }

    /**
     * Checks if directory path is absolute.
     *
     * @param path
     *            Directory path
     * @return TRUE if path is absolute
     */
    private static Boolean isDirectory(String path) {
        if (path != null) {
            File test = new File(path);

            if (test.isAbsolute()) {
                return Boolean.TRUE;
            } else {
                return Boolean.FALSE;
            }
        } else {
            return Boolean.FALSE;
        }
    }
}
