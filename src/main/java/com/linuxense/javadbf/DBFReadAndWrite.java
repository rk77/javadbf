package com.linuxense.javadbf;

//import android.util.Log;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
//import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
//import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class DBFReadAndWrite extends DBFBase implements Closeable {

    private static final String TAG = "AX";
    private static final long MILLISECS_PER_DAY = 24 * 60 * 60 * 1000;
    private static final long TIME_MILLIS_1_1_4713_BC = -210866803200000L;

    protected InputStream inputStream;
    protected DataInputStream dataInputStream;
    private DBFHeader header;
    private boolean trimRightSpaces = true;

    private DBFMemoFile memoFile = null;

    private boolean closed = false;

    private Map<String, Integer> mapFieldNames = new HashMap<String, Integer>();

    private boolean showDeletedRows = false;

    private String dbfPath = null;
    private int recordStartIndex = -1;

    /**
     * Intializes a DBFReader object.
     * <p>
     * Tries to detect charset from file, if failed uses default charset ISO-8859-1
     * When this constructor returns the object will have completed reading the
     * header (meta date) and header information can be queried there on. And it
     * will be ready to return the first row.
     *
     * @param in the InputStream where the data is read from.
     */
    public DBFReadAndWrite(InputStream in, String dbfPath) {
        this(in, null, false, dbfPath);
    }

    /**
     * Intializes a DBFReader object.
     * <p>
     * Tries to detect charset from file, if failed uses default charset ISO-8859-1
     * When this constructor returns the object will have completed reading the
     * header (meta date) and header information can be queried there on. And it
     * will be ready to return the first row.
     *
     * @param in              the InputStream where the data is read from.
     * @param showDeletedRows can be used to identify records that have been deleted.
     */
    // TODO Change to boolean in 2.0
    public DBFReadAndWrite(InputStream in, Boolean showDeletedRows, String dbfPath) {
        this(in, null, showDeletedRows, dbfPath);
    }

    /**
     * Initializes a DBFReader object.
     * <p>
     * When this constructor returns the object will have completed reading the
     * header (meta date) and header information can be queried there on. And it
     * will be ready to return the first row.
     *
     * @param in      the InputStream where the data is read from.
     * @param charset charset used to decode field names and field contents. If null, then is autedetected from dbf file
     */
    public DBFReadAndWrite(InputStream in, Charset charset, String dbfPath) {
        this(in, charset, false, dbfPath);
    }

    /**
     * Initializes a DBFReader object.
     * <p>
     * When this constructor returns the object will have completed reading the
     * header (meta date) and header information can be queried there on. And it
     * will be ready to return the first row.
     *
     * @param in              the InputStream where the data is read from.
     * @param charset         charset used to decode field names and field contents. If null, then is autedetected from dbf file
     * @param showDeletedRows can be used to identify records that have been deleted.
     */
    public DBFReadAndWrite(InputStream in, Charset charset, boolean showDeletedRows, String dbfPath) {
        try {
            this.dbfPath = dbfPath;
            this.showDeletedRows = showDeletedRows;
            this.inputStream = in;
            this.dataInputStream = new DataInputStream(this.inputStream);
            this.header = new DBFHeader();
            this.header.read(this.dataInputStream, charset, showDeletedRows);
            setCharset(this.header.getUsedCharset());
            /* it might be required to leap to the start of records at times */
            int fieldSize = this.header.getFieldDescriptorSize();
            int tableSize = this.header.getTableHeaderSize();
            int t_dataStartIndex = this.header.headerLength - (tableSize + (fieldSize * this.header.fieldArray.length)) - 1;
            recordStartIndex = this.header.headerLength + 1;
            //Log.i(TAG, "init construct, recordStartIndex: " + recordStartIndex);
            skip(t_dataStartIndex);

            this.mapFieldNames = createMapFieldNames(this.header.userFieldArray);
        } catch (IOException e) {
            DBFUtils.close(dataInputStream);
            DBFUtils.close(in);
            throw new DBFException(e.getMessage(), e);
        }
    }

    public void writeItem(int rowIdx, String columeName, Object objectValue) {
        if (columeName == null) {
            throw new DBFException("wirteItem, colume name null, columeName: " + columeName);
        }
        if (dbfPath == null) {
            throw new DBFException("wirteItem, File no exist, dbfPath: " + dbfPath);
        }
        if (rowIdx < 0 || rowIdx > getRecordCount() - 1) {
            throw new DBFException("wirteItem, rowIdx error, rowIdx: " + rowIdx);
        }
        boolean hasColume = false;
        DBFField rightField = null;
        for (int i = 0; i < this.header.fieldArray.length; i++) {
            DBFField field = this.header.fieldArray[i];
            if (columeName.equals(field.getName())) {
                hasColume = true;
                rightField = field;
            }
        }
        if (!hasColume) {
            throw new DBFException("wirteItem, not has this colume: " + columeName);
        }

        int fieldIdx = getFieldValueBeginIndex(rowIdx, columeName);
        //Log.i(TAG, "writeItem, fieldIdx: " + fieldIdx);
        if (fieldIdx < 0) {
            return;
        }

        if (rightField == null) {
            return;
        }

        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(dbfPath, "rw");
            //byte[] buffer = new byte[1];
            //int pos = getRowDataBeginIndex(9);
            raf.seek(fieldIdx - 1);
//            for (int i = 0; i < rightField.getLength(); i++) {
//                raf.read(buffer, 0, buffer.length);
//                Log.i(TAG, "writeItem, " + i + " get item: " + (char) buffer[0]);
//            }

            switch (rightField.getType()) {

                case CHARACTER:
                    String strValue = "";
                    if (objectValue != null) {
                        strValue = objectValue.toString();
                    }
                    raf.write(DBFUtils.textPadding(strValue, getCharset(), rightField.getLength(), DBFAlignment.LEFT, (byte) ' '));

                    break;

                case DATE:
                    if (objectValue != null) {
                        GregorianCalendar calendar = new GregorianCalendar();
                        calendar.setTime((Date) objectValue);
                        raf.write(String.valueOf(calendar.get(Calendar.YEAR)).getBytes(StandardCharsets.US_ASCII));
                        raf.write(DBFUtils.textPadding(String.valueOf(calendar.get(Calendar.MONTH) + 1),
                                StandardCharsets.US_ASCII, 2, DBFAlignment.RIGHT, (byte) '0'));
                        raf.write(DBFUtils.textPadding(String.valueOf(calendar.get(Calendar.DAY_OF_MONTH)),
                                StandardCharsets.US_ASCII, 2, DBFAlignment.RIGHT, (byte) '0'));
                    } else {
                        raf.write(("        ".getBytes(StandardCharsets.US_ASCII)));
                    }

                    break;
                case NUMERIC:
                case FLOATING_POINT:

                    if (objectValue != null) {
                        raf.write(DBFUtils.doubleFormating((Number) objectValue, getCharset(),
                                rightField.getLength(), rightField.getDecimalCount()));
                    } else {
                        raf.write(DBFUtils.textPadding(" ", getCharset(), rightField.getLength(), DBFAlignment.RIGHT, (byte) ' '));
                    }

                    break;

                case LOGICAL:
                    byte[] data = new byte[1];
                    if (objectValue instanceof Boolean) {

                        if ((Boolean) objectValue) {
                            data[0] = 'T';
                            raf.write((data));
                        } else {
                            data[0] = 'F';
                            raf.write((data));
                        }
                    } else {
                        data[0] = '?';
                        raf.write((data));
                    }

                    break;

                default:
                    throw new DBFException("Unknown field type " + rightField.getType());
            }

            raf.close();
        } catch (Exception e) {
            //Log.i(TAG, "error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (raf != null) {
                    raf.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        /*
        FileOutputStream dataOutput = null;
        try {
            dataOutput = new FileOutputStream(dbfPath);

            FileChannel ch = dataOutput.getChannel();

            ch.position(filedIdx);

            switch (rightField.getType()) {

                case CHARACTER:
                    String strValue = "";
                    if (objectValue != null) {
                        strValue = objectValue.toString();
                    }
                    ch.write(ByteBuffer.wrap(DBFUtils.textPadding(strValue, getCharset(), rightField.getLength(), DBFAlignment.LEFT, (byte) ' ')));

                    break;

                case DATE:
                    if (objectValue != null) {
                        GregorianCalendar calendar = new GregorianCalendar();
                        calendar.setTime((Date) objectValue);
                        ch.write(ByteBuffer.wrap(String.valueOf(calendar.get(Calendar.YEAR)).getBytes(StandardCharsets.US_ASCII)));
                        ch.write(ByteBuffer.wrap(DBFUtils.textPadding(String.valueOf(calendar.get(Calendar.MONTH) + 1),
                                StandardCharsets.US_ASCII, 2, DBFAlignment.RIGHT, (byte) '0')));
                        ch.write(ByteBuffer.wrap(DBFUtils.textPadding(String.valueOf(calendar.get(Calendar.DAY_OF_MONTH)),
                                StandardCharsets.US_ASCII, 2, DBFAlignment.RIGHT, (byte) '0')));
                    } else {
                        ch.write(ByteBuffer.wrap("        ".getBytes(StandardCharsets.US_ASCII)));
                    }

                    break;
                case NUMERIC:
                case FLOATING_POINT:

                    if (objectValue != null) {
                        ch.write(ByteBuffer.wrap(DBFUtils.doubleFormating((Number) objectValue, getCharset(),
                                rightField.getLength(), rightField.getDecimalCount())));
                    } else {
                        ch.write(ByteBuffer.wrap(DBFUtils.textPadding(" ", getCharset(), rightField.getLength(), DBFAlignment.RIGHT, (byte) ' ')));
                    }

                    break;

                case LOGICAL:
                    byte[] data = new byte[1];
                    if (objectValue instanceof Boolean) {

                        if ((Boolean) objectValue) {
                            data[0] = 'T';
                            ch.write(ByteBuffer.wrap(data));
                        } else {
                            data[0] = 'F';
                            ch.write(ByteBuffer.wrap(data));
                        }
                    } else {
                        data[0] = '?';
                        ch.write(ByteBuffer.wrap(data));
                    }

                    break;

                default:
                    throw new DBFException("Unknown field type " + rightField.getType());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (dataOutput != null) {
                    dataOutput.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        */
    }


    private int getRowDataBeginIndex(int rowIdx) {
        int idx = -1;
        int rowDataLength = 0;
        for (int i = 0; i < this.header.fieldArray.length; i++) {
            rowDataLength = rowDataLength + this.header.fieldArray[i].getLength();
        }
        // every one data row begins with one space
        rowDataLength = rowDataLength + 1;
        //Log.i(TAG, "getRowDataBeginIndex, dataStartIndex: " + recordStartIndex + ", rowDataLength: " + rowDataLength);
        idx = this.recordStartIndex + rowIdx * rowDataLength;
        return idx;
    }

    private int getFieldValueBeginIndex(int rowIdx, String columeName) {
        if (columeName == null) {
            throw new DBFException("wirteItem, colume name null, columeName: " + columeName);
        }
        if (rowIdx < 0 || rowIdx > getRecordCount() - 1) {
            throw new DBFException("wirteItem, rowIdx error, rowIdx: " + rowIdx);
        }

        int rowDataIdx = getRowDataBeginIndex(rowIdx);
       // Log.i(TAG, "getFieldValueBeginIndex, rowDataIdx: " + rowDataIdx);
        int offset = rowDataIdx;

        boolean hasField = false;
        for (int i = 0; i < this.header.fieldArray.length; i++) {
            offset = offset + this.header.fieldArray[i].getLength();
            if (columeName.equals(this.header.fieldArray[i].getName())) {
                hasField = true;
                offset = offset - this.header.fieldArray[i].getLength();
                break;
            }
        }
        if (hasField) {
            return (offset + 1);
        } else {
            return -1;
        }
    }

    private Map<String, Integer> createMapFieldNames(DBFField[] fieldArray) {
        Map<String, Integer> fieldNames = new HashMap<String, Integer>();
        for (int i = 0; i < fieldArray.length; i++) {
            String name = fieldArray[i].getName();
            fieldNames.put(name.toLowerCase(), i);
        }
        return Collections.unmodifiableMap(fieldNames);
    }


    /**
     * Returns the number of records in the DBF. This number includes deleted (hidden) records
     *
     * @return number of records in the DBF file.
     */
    public int getRecordCount() {
        return this.header.numberOfRecords;
    }

    /**
     * Returns the last time the file was modified
     *
     * @return the las time the file was modified
     */
    public Date getLastModificationDate() {
        if (this.header != null) {
            return this.header.getLastModificationDate();
        }
        return null;
    }


    /**
     * Returns the asked Field. In case of an invalid index, it returns a
     * ArrayIndexOutofboundsException.
     *
     * @param index Index of the field. Index of the first field is zero.
     * @return Field definition for selected field
     */
    public DBFField getField(int index) {
        if (index < 0 || index >= this.header.userFieldArray.length) {
            throw new IllegalArgumentException("Invalid index field: (" + index + "). Valid range is 0 to " + (this.header.userFieldArray.length - 1));
        }
        return new DBFField(this.header.userFieldArray[index]);
    }

    /**
     * Returns the number of field in the DBF.
     *
     * @return number of fields in the DBF file
     */
    public int getFieldCount() {
        return this.header.userFieldArray.length;
    }

    /**
     * Reads the returns the next row in the DBF stream.
     *
     * @return The next row as an Object array. Types of the elements these
     * arrays follow the convention mentioned in the class description.
     */
    public Object[] nextRecord() {
        if (this.closed) {
            throw new IllegalArgumentException("this DBFReader is closed");
        }
        List<Object> recordObjects = new ArrayList<>(this.getFieldCount());
        try {
            boolean isDeleted = false;

            do {
                try {
                    if (isDeleted && !showDeletedRows) {
                        skip(this.header.recordLength - 1);
                    }
                    int t_byte = this.dataInputStream.readByte();
                    if (t_byte == END_OF_DATA || t_byte == -1) {
                        return null;
                    }
                    isDeleted = t_byte == '*';
                } catch (EOFException e) {
                    return null;
                }
            } while (isDeleted && !showDeletedRows);

            if (showDeletedRows) {
                recordObjects.add(isDeleted);
            }

            for (int i = 0; i < this.header.fieldArray.length; i++) {
                DBFField field = this.header.fieldArray[i];
                Object o = getFieldValue(field);
                if (field.isSystem() || field.getType() == DBFDataType.NULL_FLAGS) {
                    if (field.getType() == DBFDataType.NULL_FLAGS && o instanceof BitSet) {
                        BitSet nullFlags = (BitSet) o;
                        int currentIndex = -1;
                        for (int j = 0; j < this.header.fieldArray.length; j++) {
                            DBFField field1 = this.header.fieldArray[j];
                            if (field1.isNullable()) {
                                currentIndex++;
                                if (nullFlags.get(currentIndex)) {
                                    recordObjects.set(j, null);
                                }
                            }
                            if (field1.getType() == DBFDataType.VARBINARY || field1.getType() == DBFDataType.VARCHAR) {
                                currentIndex++;
                                if (recordObjects.get(j) instanceof byte[]) {
                                    byte[] data = (byte[]) recordObjects.get(j);
                                    int size = field1.getLength();
                                    if (!nullFlags.get(currentIndex)) {
                                        // Data is not full
                                        // lenght is stored in the last position
                                        size = data[data.length - 1];
                                    }
                                    byte[] newData = new byte[size];
                                    System.arraycopy(data, 0, newData, 0, size);
                                    Object o1 = newData;
                                    if (field1.getType() == DBFDataType.VARCHAR) {
                                        o1 = new String(newData, getCharset());
                                    }
                                    recordObjects.set(j, o1);
                                }
                            }
                        }
                    }
                } else {
                    recordObjects.add(o);
                }
            }
        } catch (EOFException e) {
            throw new DBFException(e.getMessage(), e);
        } catch (IOException e) {
            throw new DBFException(e.getMessage(), e);
        }
        return recordObjects.toArray();
    }

    /**
     * Reads the returns the next row in the DBF stream.
     *
     * @return The next row as an DBFRow
     */
    public DBFRow nextRow() {
        Object[] record = nextRecord();
        if (record == null) {
            return null;
        }
        return new DBFRow(record, mapFieldNames, this.header.fieldArray);
    }

    protected Object getFieldValue(DBFField field) throws IOException {
        int bytesReaded = 0;
        switch (field.getType()) {
            case CHARACTER:
                byte b_array[] = new byte[field.getLength()];
                bytesReaded = this.dataInputStream.read(b_array);
                if (bytesReaded < field.getLength()) {
                    throw new EOFException("Unexpected end of file");
                }
                if (this.trimRightSpaces) {
                    return new String(DBFUtils.trimRightSpaces(b_array), getCharset());
                } else {
                    return new String(b_array, getCharset());
                }

            case VARCHAR:
            case VARBINARY:
                byte b_array_var[] = new byte[field.getLength()];
                bytesReaded = this.dataInputStream.read(b_array_var);
                if (bytesReaded < field.getLength()) {
                    throw new EOFException("Unexpected end of file");
                }
                return b_array_var;
            case DATE:

                byte t_byte_year[] = new byte[4];
                bytesReaded = this.dataInputStream.read(t_byte_year);
                if (bytesReaded < 4) {
                    throw new EOFException("Unexpected end of file");
                }

                byte t_byte_month[] = new byte[2];
                bytesReaded = this.dataInputStream.read(t_byte_month);
                if (bytesReaded < 2) {
                    throw new EOFException("Unexpected end of file");
                }

                byte t_byte_day[] = new byte[2];
                bytesReaded = this.dataInputStream.read(t_byte_day);
                if (bytesReaded < 2) {
                    throw new EOFException("Unexpected end of file");
                }

                try {
                    GregorianCalendar calendar = new GregorianCalendar(Integer.parseInt(new String(t_byte_year, StandardCharsets.US_ASCII)),
                            Integer.parseInt(new String(t_byte_month, StandardCharsets.US_ASCII)) - 1,
                            Integer.parseInt(new String(t_byte_day, StandardCharsets.US_ASCII)));
                    return calendar.getTime();
                } catch (NumberFormatException e) {
                    // this field may be empty or may have improper value set
                    return null;
                }


            case FLOATING_POINT:
            case NUMERIC:
                return DBFUtils.readNumericStoredAsText(this.dataInputStream, field.getLength());

            case LOGICAL:
                byte t_logical = this.dataInputStream.readByte();
                return DBFUtils.toBoolean(t_logical);
            case LONG:
            case AUTOINCREMENT:
                int data = DBFUtils.readLittleEndianInt(this.dataInputStream);
                return data;
            case CURRENCY:
                int c_data = DBFUtils.readLittleEndianInt(this.dataInputStream);
                String s_data = String.format("%05d", c_data);
                String x1 = s_data.substring(0, s_data.length() - 4);
                String x2 = s_data.substring(s_data.length() - 4);

                skip(field.getLength() - 4);
                return new BigDecimal(x1 + "." + x2);
            case TIMESTAMP:
            case TIMESTAMP_DBASE7:
                int days = DBFUtils.readLittleEndianInt(this.dataInputStream);
                int time = DBFUtils.readLittleEndianInt(this.dataInputStream);

                if (days == 0 && time == 0) {
                    return null;
                } else {
                    Calendar calendar = new GregorianCalendar();
                    calendar.setTimeInMillis(days * MILLISECS_PER_DAY + TIME_MILLIS_1_1_4713_BC + time);
                    calendar.add(Calendar.MILLISECOND, -TimeZone.getDefault().getOffset(calendar.getTimeInMillis()));
                    return calendar.getTime();
                }
            case MEMO:
            case GENERAL_OLE:
            case PICTURE:
            case BLOB:
                return readMemoField(field);
            case BINARY:
                if (field.getLength() == 8) {
                    return readDoubleField(field);
                } else {
                    return readMemoField(field);
                }
            case DOUBLE:
                return readDoubleField(field);
            case NULL_FLAGS:
                byte[] data1 = new byte[field.getLength()];
                int readed = dataInputStream.read(data1);
                if (readed != field.getLength()) {
                    throw new EOFException("Unexpected end of file");
                }
                return BitSet.valueOf(data1);
            default:
                skip(field.getLength());
                return null;
        }
    }

    private Object readDoubleField(DBFField field) throws IOException {
        byte[] data = new byte[field.getLength()];
        int bytesReaded = this.dataInputStream.read(data);
        if (bytesReaded < field.getLength()) {
            throw new EOFException("Unexpected end of file");
        }
        return ByteBuffer.wrap(
                new byte[]{
                        data[7], data[6], data[5], data[4],
                        data[3], data[2], data[1], data[0]
                }).getDouble();
    }

    private Object readMemoField(DBFField field) throws IOException {
        Number nBlock = null;
        if (field.getLength() == 10) {
            nBlock = DBFUtils.readNumericStoredAsText(this.dataInputStream, field.getLength());
        } else {
            nBlock = DBFUtils.readLittleEndianInt(this.dataInputStream);
        }
        if (this.memoFile != null && nBlock != null) {
            return memoFile.readData(nBlock.intValue(), field.getType());
        }
        return null;
    }

    /**
     * Safely skip bytesToSkip bytes (in some bufferd scenarios skip doesn't really skip all requested bytes)
     *
     * @param bytesToSkip number of bytes to skip
     * @throws IOException if some IO error happens
     */
    protected void skip(int bytesToSkip) throws IOException {
        DBFUtils.skip(this.dataInputStream, bytesToSkip);
    }

    /**
     * Skip records from reading. Treat "deleted" records as normal records.
     *
     * @param recordsToSkip Number of records to skip.
     * @throws IOException if some IO error happens
     */
    public void skipRecords(int recordsToSkip) throws IOException {
        skip(recordsToSkip * this.header.recordLength);
    }

    protected DBFHeader getHeader() {
        return this.header;
    }

    /**
     * Determine if character fields should be right trimmed (default true)
     *
     * @return true if data is right trimmed
     */
    public boolean isTrimRightSpaces() {
        return this.trimRightSpaces;
    }

    /**
     * Determine if character fields should be right trimmed (default true)
     *
     * @param trimRightSpaces if reading fields should trim right spaces
     */
    public void setTrimRightSpaces(boolean trimRightSpaces) {
        this.trimRightSpaces = trimRightSpaces;
    }


    /**
     * Sets the memo file (DBT or FPT) where memo fields will be readed.
     * If no file is provided, then this fields will be null.
     *
     * @param file the file containing the memo data
     */
    public void setMemoFile(File file) {
        if (this.memoFile != null) {
            throw new IllegalStateException("Memo file is already setted");
        }
        setMemoFile(file, file.length() < (8 * 1024 * 1024));
    }

    /**
     * Sets the memo file (DBT or FPT) where memo fields will be readed.
     * If no file is provided, then this fields will be null.
     *
     * @param file     the file containing the memo data
     * @param inMemory if the memoFile shoud be loaded in memory (caution, it may hang your jvm if memo file is too big)
     */
    public void setMemoFile(File file, boolean inMemory) {
        if (this.memoFile != null) {
            throw new IllegalStateException("Memo file is already setted");
        }
        if (!file.exists()) {
            throw new DBFException("Memo file " + file.getName() + " not exists");
        }
        if (!file.canRead()) {
            throw new DBFException("Cannot read Memo file " + file.getName());
        }
        this.memoFile = new DBFMemoFile(file, this.getCharset());
    }

    @Override
    public void close() {
        this.closed = true;
        DBFUtils.close(this.dataInputStream);
        DBFUtils.close(this.memoFile);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(128);
        sb.append(this.header.getYear()).append("/");
        sb.append(this.header.getMonth()).append("/");
        sb.append(this.header.getDay()).append("\n");
        sb.append("Total records: ").append(this.header.numberOfRecords).append("\n");
        sb.append("Header length: ").append(this.header.headerLength).append("\n");
        sb.append("Columns:\n");
        for (DBFField field : this.header.fieldArray) {
            sb.append(field.getName());
            sb.append("\n");
        }
        return sb.toString();
    }


    protected int getEstimatedOutputSize() {
        return this.getHeader().numberOfRecords * this.getHeader().recordLength;
    }
}
