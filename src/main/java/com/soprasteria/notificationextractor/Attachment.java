package com.soprasteria.notificationextractor;

import java.io.File;
import java.io.FileOutputStream;
import java.util.zip.Inflater;

/**
 * Class that represents attachment file.
 *
 * @author sgacka
 */
public class Attachment {

    private final String fileName;
    private String updatedFileName;
    private final String uid;
    private final Boolean isCompressed;
    private byte[] bytes;

    /**
     * Attachment constructor.
     *
     * @param fileName
     *            Name of the file
     * @param uid
     *            UID from DB
     * @param isCompressed
     *            TRUE if file is compressed with zlib
     */
    public Attachment(String fileName, String uid, Boolean isCompressed) {
        this.fileName = fileName.replace("?", "_");
        this.uid = uid;
        this.isCompressed = isCompressed;
    }

    /**
     * Saves file on disk in specified path.
     *
     * @param path
     *            Destination path for saving file
     * @param file
     *            Name of the file
     * @param number
     *            Number of the attachment
     * @throws java.lang.Exception
     */
    public void saveAttachment(String path, String file, int number) throws Exception {
        File dir = new File(path.replace(file, ""));

        if (!dir.exists()) {
            dir.mkdirs();
        }

        if (number < 10) {
            file = file.replace(".temp", "_0" + Integer.toString(number)) + "_";
        } else {
            file = file.replace(".temp", "_" + Integer.toString(number)) + "_";
        }

        updatedFileName = file + fileName;

        FileOutputStream fos = new FileOutputStream(dir.getPath() + File.separator + updatedFileName);

        // if file is compressed then use Inflater to decompress it
        if (isCompressed) {
            Inflater decompressor = new Inflater();
            decompressor.setInput(bytes);

            byte[] buf = new byte[1024];
            while (!decompressor.finished()) {
                int count = decompressor.inflate(buf);
                fos.write(buf, 0, count);
            }
        } else {
            fos.write(bytes);
        }

        fos.close();
    }

    /**
     * Sets byte content of the file.
     *
     * @param bytes
     *            Byte content
     */
    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    /**
     * Gets attachment file name.
     *
     * @return File name
     */
    public String getFileName() {
        return updatedFileName;
    }
}
