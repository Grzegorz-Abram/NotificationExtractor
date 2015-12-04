package com.soprasteria.notificationextractor;

import java.io.ByteArrayOutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.pool.OracleDataSource;
import org.apache.log4j.Logger;

/**
 * Class that represents database object.
 *
 * @author sgacka
 */
public class Database {

    private final String user;
    private final String password;
    private final String host;
    private final int port;
    private final String sid;
    private OracleConnection connection;
    private static final Logger logger = Logger.getLogger(Database.class);

    /**
     * Database constructor.
     *
     * @param user
     *            Database user name
     * @param password
     *            Database user password
     * @param host
     *            Database host name/IP
     * @param port
     *            Listening port
     * @param sid
     *            Database SID (name)
     */
    public Database(String user, String password, String host, int port, String sid) {
        this.user = user;
        this.password = password;
        this.host = host;
        this.port = port;
        this.sid = sid;
    }

    /**
     * Opens connection to database.
     */
    public void connect() {
        try {
            OracleDataSource ods = new OracleDataSource();

            ods.setDriverType("thin");
            ods.setUser(user);
            ods.setPassword(password);
            ods.setServerName(host);
            ods.setPortNumber(port);
            ods.setDatabaseName(sid); // sid

            logger.info("Connecting to database... (User: " + Configuration.db_user + ", Host: " + Configuration.db_host + ", Port: "
                    + Configuration.db_port + ")");

            connection = (OracleConnection) (ods.getConnection());
            connection.setDefaultExecuteBatch(100);
        } catch (Throwable e) {
            logger.fatal("Unable to connect to database", e);

            Configuration.stop = Calendar.getInstance().getTime();
            logger.info("Execution time: " + (double) (Configuration.stop.getTime() - Configuration.start.getTime()) / 1000 + " seconds");
            logger.info("ERROR. Application ended with error.");

            System.exit(1);
        } finally {
            logger.info("Connection to database has been opened");
        }
    }

    /**
     * Closes connection to database.
     */
    public void disconnect() {
        try {
            if (!connection.isClosed()) {
                connection.close();
            }
        } catch (Throwable e) {
            logger.error("Unable to close connection to database", e);
        } finally {
            logger.info("Connection to database has been closed");
        }
    }

    /**
     * Gets list of eventout records.
     *
     * @return List of eventout records
     * @throws Exception
     */
    public ArrayList<EventOutRecord> getEventOutRecords() throws Exception {
        ArrayList<EventOutRecord> records = new ArrayList<EventOutRecord>();

        String query = "SELECT evfields, CAST(FROM_TZ(CAST(evtime AS TIMESTAMP), 'utc') AT TIME ZONE sessiontimezone AS DATE), evsysseq FROM eventoutm1 WHERE evtype = 'page' and evtime IS NOT NULL and evsysseq IS NOT NULL";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);

        while (resultSet.next()) {
            records.add(new EventOutRecord(resultSet.getClob(1), resultSet.getTimestamp(2).getTime(), resultSet.getString(3), this));
        }

        logger.info("Total eventout records found: " + records.size());

        return records;
    }

    /**
     * Gets phase number for specified line item.
     *
     * @param ticketNumber
     *            Line item number
     * @return Line item phase number
     * @throws Exception
     */
    public synchronized String getLineItemPhaseNum(String ticketNumber) throws Exception {
        PreparedStatement pStatement = connection.prepareStatement("SELECT phase_num FROM ocmlm1 WHERE \"NUMBER\" = ?");
        pStatement.setString(1, ticketNumber);
        ResultSet resultSet = pStatement.executeQuery();
        String result;

        if (resultSet.next()) {
            if (logger.isTraceEnabled()) {
                logger.trace(getTicketNumber(ticketNumber) + "Phase num: " + resultSet.getString(1));
            }

            result = resultSet.getString(1);
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace(getTicketNumber(ticketNumber) + "Phase num not found");
            }

            result = "";
        }

        resultSet.close();
        pStatement.close();

        return result;
    }

    /**
     * Gets date boundaries based on notification date.
     *
     * @param source
     *            Name of the ticket source
     * @param number
     *            Ticket number
     * @param fileDate
     *            Date from the file (updated with database time zone)
     * @return Dates as long values
     * @throws Exception
     */
    public synchronized long[] getActivityBoundaries(String source, String number, String fileDate) throws Exception {
        Timestamp rightBoundary;
        Timestamp leftBoundary;

        // get current Date from UTC
        DateFormat formatCurrent = new SimpleDateFormat("yyyyMMddHHmmssZ");
        Date currentDate = formatCurrent.parse(fileDate);

        // get time according to Time Zone
        DateFormat formatForTZ = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        formatForTZ.setTimeZone(TimeZone.getTimeZone(Configuration.timeZoneCode));
        String data = formatForTZ.format(currentDate);

        Timestamp timeForTZ = Timestamp.valueOf(data);

        if (logger.isTraceEnabled()) {
            logger.trace(getTicketNumber(number) + "Ticket type: " + source);
            logger.trace(getTicketNumber(number) + "Notification date (" + Configuration.timeZoneCode + "): " + data);
        }

        leftBoundary = timeForTZ;
        rightBoundary = new Timestamp(timeForTZ.getTime() + 10000);

        if (logger.isTraceEnabled()) {
            logger.trace(getTicketNumber(number) + "Left boudary date: " + leftBoundary);
            logger.trace(getTicketNumber(number) + "Right boudary date: " + rightBoundary);
        }

        return new long[] { leftBoundary.getTime(), rightBoundary.getTime() };
    }

    /**
     * Gets attachments for current event record.
     *
     * @param ticketNumber
     *            Ticket number
     * @param boundaries
     *            Date boundaries
     * @return List of attachments
     * @throws Exception
     */
    public synchronized ArrayList<Attachment> getAttachments(String ticketNumber, long[] boundaries) throws Exception {
        ArrayList<Attachment> attachments = new ArrayList<Attachment>();
        PreparedStatement pStatement;
        String query, subQuery;

        subQuery = "SELECT s2.\"UID\" FROM sysattachmem1 s2 WHERE s2.topic = ? AND s2.sysmodtime BETWEEN ? AND ? AND s2.segment = 0";
        if (Configuration.limitAttachments) {
            subQuery += " AND rownum <= ?";
        }
        query = "SELECT s1.filename, s1.\"UID\", s1.\"DATA\", s1.compressed, s1.\"SIZE\", s1.compressed_size FROM sysattachmem1 s1 WHERE s1.topic = ? AND s1.sysmodtime BETWEEN ? AND ? AND s1.\"UID\" IN ("
                + subQuery + ") ORDER BY s1.sysmodtime, s1.\"UID\", s1.segment";

        pStatement = connection.prepareStatement(query);
        pStatement.setString(1, ticketNumber);
        pStatement.setTimestamp(2, new Timestamp(boundaries[0]));
        pStatement.setTimestamp(3, new Timestamp(boundaries[1]));
        pStatement.setString(4, ticketNumber);
        pStatement.setTimestamp(5, new Timestamp(boundaries[0]));
        pStatement.setTimestamp(6, new Timestamp(boundaries[1]));
        if (Configuration.limitAttachments) {
            pStatement.setInt(7, Configuration.attachmentLimit);
        }

        ResultSet resultSet = pStatement.executeQuery();

        String fileName;
        String fileNameOld = "";
        String uid;
        String uidOld = "";
        Boolean isCompressed = Boolean.FALSE;
        Boolean isCompressedOld = Boolean.FALSE;
        int normalSize = 0;
        int compressedSize = 0;
        Attachment attachment = null;
        ByteArrayOutputStream baos;
        ArrayList<byte[]> segmentsBytes = null;

        while (resultSet.next()) {
            fileName = resultSet.getString(1);
            uid = resultSet.getString(2);

            if (resultSet.getString(4).equals("t")) {
                isCompressed = Boolean.TRUE;
            } else if (resultSet.getString(4).equals("f")) {
                isCompressed = Boolean.FALSE;
            }

            if (fileName.equals(fileNameOld) && uid.equals(uidOld) && isCompressed.equals(isCompressedOld)) {
                // continue
            } else {
                if (attachment != null) {
                    if (isCompressedOld) {
                        attachment.setBytes(getBytes(segmentsBytes, compressedSize));
                    } else {
                        attachment.setBytes(getBytes(segmentsBytes, normalSize));
                    }

                    attachments.add(attachment);

                    if (logger.isTraceEnabled()) {
                        logger.trace(getTicketNumber(ticketNumber) + "Attachment: {filename=" + fileNameOld + ", compressed="
                                + isCompressedOld + "}");
                    }
                }

                attachment = new Attachment(fileName, uid, isCompressed);
                segmentsBytes = new ArrayList<byte[]>();

                normalSize = resultSet.getInt(5);
                compressedSize = resultSet.getInt(6);
            }

            baos = new ByteArrayOutputStream();
            baos.write(resultSet.getBytes(3));
            if (segmentsBytes != null) {
                segmentsBytes.add(baos.toByteArray());
            }
            baos.close();

            fileNameOld = fileName;
            uidOld = uid;
            isCompressedOld = isCompressed;
        }

        // last attachment if present
        if (attachment != null) {
            if (isCompressedOld) {
                attachment.setBytes(getBytes(segmentsBytes, compressedSize));
            } else {
                attachment.setBytes(getBytes(segmentsBytes, normalSize));
            }

            attachments.add(attachment);
            if (segmentsBytes != null) {
                segmentsBytes.clear();
            }

            if (logger.isTraceEnabled()) {
                logger.trace(getTicketNumber(ticketNumber) + "Attachment: {filename=" + fileNameOld + ", size= " + normalSize
                        + ", compressed=" + isCompressedOld + "}");
            }
        }

        resultSet.close();
        pStatement.close();

        return attachments;
    }

    /**
     * Gets byte array with attachment data from all extracted segments without header.
     *
     * @param byteList
     *            Byte list of all segments
     * @param size
     *            Attachment size (compressed or not)
     * @return Byte array with attachment data
     * @throws Exception
     */
    private byte[] getBytes(ArrayList<byte[]> byteList, int size) throws Exception {
        int totalSegmentSize = 0;

        // getting total size of all segments
        for (int i = 0; i < byteList.size(); i++) {
            totalSegmentSize += byteList.get(i).length;
        }
        logger.trace("Attachment SM size: " + totalSegmentSize);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // extracting valid data (without header) into single byte array
        for (int i = 0; i < byteList.size(); i++) {
            int headerSize = 0;

            // get the RC type indicator located at 8th byte
            byte indicatorLength = byteList.get(i)[7];
            // which means the next 1 byte is the length indicator
            if (indicatorLength == 0x2D) {
                headerSize = 9;

                logger.trace("Header size for segment " + i + ": " + headerSize);
            } // which means the next 2 bytes are the length indicator
            else if (indicatorLength == 0x2E) {
                headerSize = 10;

                logger.trace("Header size for segment + " + i + ": " + headerSize);
            }

            logger.trace("Segment " + i + " size: " + (byteList.get(i).length - headerSize));

            baos.write(byteList.get(i), headerSize, byteList.get(i).length - headerSize);
        }

        byte[] result = baos.toByteArray();
        baos.close();

        return result;
    }

    /**
     * Removes eventout record based on unique evsysseq value.
     *
     * @param evSysSeq
     *            evsysseq key value
     * @throws Exception
     */
    public synchronized void removeRecordFromEventOut(String evSysSeq) throws Exception {
        PreparedStatement pStatement = connection.prepareStatement("DELETE FROM eventoutm1 WHERE evsysseq = ? AND evtype = 'page'");
        pStatement.setString(1, evSysSeq);
        pStatement.executeUpdate();

        connection.commit();
        pStatement.close();

        logger.info("Record removed: evsysseq = " + evSysSeq);
    }

    /**
     * Gets formatted ticket number for logger.
     *
     * @param number
     *            Ticket number
     * @return Formatted ticket number
     */
    private String getTicketNumber(String number) {
        return "<" + number + "> -> ";
    }
}
