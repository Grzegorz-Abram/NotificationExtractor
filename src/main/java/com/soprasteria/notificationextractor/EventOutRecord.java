package com.soprasteria.notificationextractor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.StringReader;
import java.sql.Clob;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;

/**
 * Class that represents eventout record.
 *
 * @author sgacka
 */
public class EventOutRecord implements Runnable {

    private final Clob evFields;
    private final Calendar evTime;
    private final String evSysSeq;
    private final Database database;
    private String message;
    private ArrayList<Attachment> attachments;
    private String notificationTime;
    private String ticketSource;
    private String ticketNumber;
    private Boolean withAttachments;
    private String attachmentNames;
    private String fileName;
    private String destinationPath;
    private static final Logger logger = Logger.getLogger(EventOutRecord.class);

    /**
     * EventOutRecord constructor.
     *
     * @param evFields
     *            evFields with message from notification
     * @param evTime
     *            Date of the event
     * @param evSysSeq
     *            Unique event key
     * @param database
     *            Database object
     */
    public EventOutRecord(Clob evFields, long evTime, String evSysSeq, Database database) {
        this.evFields = evFields;
        this.evTime = Calendar.getInstance();
        this.evTime.setTimeInMillis(evTime);
        this.evSysSeq = evSysSeq;
        this.database = database;
        this.withAttachments = Boolean.FALSE;
        this.attachments = new ArrayList<Attachment>();
        this.attachmentNames = "";
    }

    /**
     * Parses content of the blob evFields.
     *
     * @throws Exception
     */
    public void parseRecord() throws Exception {
        String temp = evFields.getSubString(1, (int) evFields.length());

        if (logger.isTraceEnabled()) {
            logger.trace(getRecordNumber() + "evFields content:\r\n" + temp);
        }

        // getting end of the message (evfields)
        // it should be after last "$end" string in evFields
        int pos = temp.lastIndexOf("$end");
        if (pos != -1) {
            temp = temp.substring(0, pos);

            // getting beginnig of the message (evfields)
            // it should be after eight separator characters after "telalert" in evFields
            String textStart = "telalert";

            pos = temp.lastIndexOf(textStart);
            temp = temp.substring(pos, temp.length());

            String separator = String.valueOf(temp.charAt(textStart.length()));

            // looking for the beginning of the notification
            for (int i = 0; i < 8; i++) {
                pos = temp.indexOf(separator) + 1;
                temp = temp.substring(pos, temp.length());
            }

            // changing line ending to DOS format
            message = temp.replace("\n", "\r\n");

            if (logger.isTraceEnabled()) {
                logger.trace(getRecordNumber() + "Final message:\r\n" + message);
            }

            // getting first line of the message to get basic notification info
            BufferedReader reader = new BufferedReader(new StringReader(temp));
            temp = reader.readLine();

            if (logger.isTraceEnabled()) {
                logger.trace(getRecordNumber() + "First line:\r\n" + temp);
            }

            // removing first line that is now obsolete
            message = message.replace(temp + "\r\n", "");

            // parsing first line into tokens
            StringTokenizer st1 = new StringTokenizer(temp, "||");

            pos = 0;
            destinationPath = null;

            while (st1.hasMoreTokens()) {
                switch (pos) {
                case 0:
                    // first token - ticket source
                    ticketSource = st1.nextToken().replaceAll("\\W", "");

                    break;
                case 1:
                    // second token - attachment info
                    // if there is AA - there are attachments available
                    // if there is NA or something else - there are no attachments available
                    String attachment = st1.nextToken();
                    if (attachment.equals("AA")) {
                        withAttachments = Boolean.TRUE;
                    } else {
                        withAttachments = Boolean.FALSE;
                    }

                    break;
                case 2:
                    // third token - ticket number
                    ticketNumber = st1.nextToken();

                    break;
                case 3:
                    // fourth token - destination path for saving this message
                    destinationPath = st1.nextToken();
                    fileName = null;

                    // parsing path into tokens
                    StringTokenizer st2 = new StringTokenizer(destinationPath, "\\");

                    // getting last token with file name
                    while (st2.hasMoreTokens()) {
                        fileName = st2.nextToken();
                    }

                    break;
                default:
                    // just continue :-)
                    st1.nextToken();

                    break;
                }

                pos++;
            }

            // checking customer tool name
            if (destinationPath.contains(Configuration.customer_tool)) {
                // checking if message is complete
                if (!ticketSource.contains("Thismessagedidnotprovideenougharguments")) {
                    Configuration.increaseNotificationsCount();

                    // saving attachments if they are available
                    if (withAttachments && !Configuration.ignoreAttachments) {
                        setNotificationTime();

                        int number = getAttachments();

                        logger.info("Record: " + getRecordNumber() + "filename = " + fileName + " has " + number + " attachment(s)");
                    } else {
                        logger.info("Record: " + getRecordNumber() + "filename = " + fileName + " has 0 attachment(s)");
                    }

                    // save message on disk
                    saveNotification();

                    // remove record from table
                    if (!Configuration.isReadOnly) {
                        database.removeRecordFromEventOut(evSysSeq);
                    }
                } else {
                    logger.warn("Record: " + getRecordNumber() + "Record ignored - message is incomplete.");
                }
            } else {
                logger.info("Record: " + getRecordNumber() + "Record ignored - message for another interface.");
            }
        } else {
            logger.info("Record: " + getRecordNumber() + "Record ignored - invalid message format.");
        }
    }

    /**
     * Sets date from file name to support attachments retrieval.
     */
    private void setNotificationTime() {
        notificationTime = fileName.substring(0, 14) + "+0000";
    }

    /**
     * Gets attachments related with event record.
     *
     * @return Number of attachments
     * @throws Exception
     */
    private int getAttachments() throws Exception {
        long[] boundaries = database.getActivityBoundaries(ticketSource, ticketNumber, notificationTime);

        if (boundaries[0] <= boundaries[1]) {
            attachments = database.getAttachments(ticketNumber, boundaries);

            if (ticketSource.equals("LINEITEM") && attachments.isEmpty()) {
                String phaseNumber = database.getLineItemPhaseNum(ticketNumber);

                attachments = database.getAttachments(phaseNumber, boundaries);
            }

            if (!attachments.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                sb.append("&attachment=");

                for (int index = 0; index < attachments.size(); index++) {
                    try {
                        attachments.get(index).saveAttachment(destinationPath, fileName, index + 1);

                        sb.append(attachments.get(index).getFileName());

                        if (index + 1 != attachments.size()) {
                            sb.append("|");
                        }
                    } catch (Throwable e) {
                        logger.error(getRecordNumber() + "Attachment: " + attachments.get(index).getFileName()
                                + " couldn't be read and will be ignored:", e);
                    }
                }

                attachmentNames = sb.toString() + ";\r\n";
            }

            return attachments.size();
        } else {
            return 0;
        }
    }

    /**
     * Saves message from event record on disk.
     *
     * @throws Exception
     */
    private void saveNotification() throws Exception {
        File dir = new File(destinationPath.replace(fileName, ""));

        if (!dir.exists()) {
            dir.mkdirs();
        }

        FileOutputStream fos = new FileOutputStream(destinationPath);

        // BOM for utf-8
        byte[] bom = new byte[3];
        bom[0] = (byte) 0xEF;
        bom[1] = (byte) 0xBB;
        bom[2] = (byte) 0xBF;
        fos.write(bom);

        fos.write(message.getBytes("utf-8"));

        if (!attachmentNames.isEmpty()) {
            fos.write(attachmentNames.getBytes("utf-8"));
        }

        fos.close();
    }

    /**
     * Gets formatted eventout record number for logger.
     *
     * @return Formatted eventout record number
     */
    private String getRecordNumber() {
        return "<" + evSysSeq + "> -> ";
    }

    /**
     * Run method for ThreadExecutor. Executes event record parsing.
     */
    public void run() {
        try {
            parseRecord();
        } catch (Throwable e) {
            logger.error(getRecordNumber() + "Unable to parse eventout record:\r\n", e);
        }
    }
}
