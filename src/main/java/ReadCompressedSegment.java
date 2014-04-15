import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

public class ReadCompressedSegment {
    static final int STRING = 0x00;
    static final int BYTE_ARR = 0x01;
    static final int NUMERIC_INT = 0x02;
    static final int NUMERIC_FLOAT = 0x03;
    static final int NUMERIC_LONG = 0x04;
    static final int NUMERIC_DOUBLE = 0x05;

    static final int TYPE_BITS = PackedInts.bitsRequired(NUMERIC_DOUBLE);
    static final int TYPE_MASK = (int) PackedInts.maxValue(TYPE_BITS);

    static final String CODEC_NAME_DAT = "Lucene41StoredFieldsData";

    static final int VERSION_START = 0;
    static final int VERSION_BIG_CHUNKS = 1;
    static final int VERSION_CURRENT = VERSION_BIG_CHUNKS;

    // Takes directory arg 1, file arg 2.
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: ReadCompressedSegment directory_name file_name");
            System.exit(1);
        }
        Directory dir = FSDirectory.open(new File(args[0]));
        SeekingIndexInput fieldsStream = new SeekingIndexInput(dir.openInput(args[1], IOContext.READ), 0);
        CompressingStoredFieldsReader fieldReader = new CompressingStoredFieldsReader(fieldsStream, CompressionMode.FAST);
        int index = 0;
        while (true) {
            try {
                fieldReader.visitDocument(index++, new ValueVisitor() {
                    @Override
                    public void binaryField(byte[] value) throws java.io.IOException {
                        System.out.println(new String(value));
                    }

                    @Override
                    public void stringField(java.lang.String value) throws java.io.IOException {
                        System.out.println(value);
                    }
                });
            } catch (IOException ioe) {
                if (!ioe.getMessage().contains("read past EOF")) {
                    throw ioe;
                } else {
                    System.exit(0);
                }
            }
        }
    }

    public static final class CompressingStoredFieldsReader {

        // Do not reuse the decompression buffer when there is more than 32kb to decompress
        private static final int BUFFER_REUSE_THRESHOLD = 1 << 15;
        private final int version = 1;
        private final SeekingIndexInput fieldsStream;
        private final int chunkSize;
        private final int packedIntsVersion;
        private final Decompressor decompressor;
        private final BytesRef bytes;
        private boolean closed;
        private long index = 0;
        private long accummulatedChunks = 0;
        private long previousLocation = 37;
        private long startPointer = 37;

        /**
         * Sole constructor.
         */
        public CompressingStoredFieldsReader(SeekingIndexInput fieldStream, CompressionMode compressionMode) throws IOException {
            boolean success = false;
            IndexInput indexStream = null;
            try {
                // Open the data file and read metadata
                this.fieldsStream = fieldStream;
                final int fieldsVersion = CodecUtil.checkHeader(fieldsStream, CODEC_NAME_DAT, VERSION_START, VERSION_CURRENT);
                if (version != fieldsVersion) {
                    throw new CorruptIndexException("Version mismatch between stored fields index and data: " + version + " != " + fieldsVersion);
                }
                assert CodecUtil.headerLength(CODEC_NAME_DAT) == fieldsStream.getFilePointer();

                if (version >= VERSION_BIG_CHUNKS) {
                    chunkSize = fieldsStream.readVInt();
                } else {
                    chunkSize = -1;
                }
                packedIntsVersion = fieldsStream.readVInt();
                decompressor = compressionMode.newDecompressor();
                this.bytes = new BytesRef();

                success = true;
            } finally {
                if (!success) {
                    this.close();
                    if (indexStream != null) {
                        indexStream.close();
                    }
                }
            }
        }

        public void close() throws IOException {
            if (!closed) {
                IOUtils.close(fieldsStream);
                closed = true;
            }
        }

        private static void readField(ValueVisitor visitor, DataInput in, int bits) throws IOException {
            switch (bits & TYPE_MASK) {
                case BYTE_ARR:
                    int length = in.readVInt();
                    byte[] data = new byte[length];
                    in.readBytes(data, 0, length);
                    visitor.binaryField(data);
                    break;
                case STRING:
                    length = in.readVInt();
                    data = new byte[length];
                    in.readBytes(data, 0, length);
                    visitor.stringField(new String(data, IOUtils.CHARSET_UTF_8));
                    break;
                case NUMERIC_INT:
                    visitor.intField(in.readInt());
                    break;
                case NUMERIC_FLOAT:
                    visitor.floatField(Float.intBitsToFloat(in.readInt()));
                    break;
                case NUMERIC_LONG:
                    visitor.longField(in.readLong());
                    break;
                case NUMERIC_DOUBLE:
                    visitor.doubleField(Double.longBitsToDouble(in.readLong()));
                    break;
                default:
                    throw new AssertionError("Unknown type flag: " + Integer.toHexString(bits));
            }
        }

        public void visitDocument(int docID, ValueVisitor visitor) throws IOException {
            boolean flipped = false;
            if (index >= accummulatedChunks) {
                startPointer = previousLocation;
                flipped = true;
            }
            index++;
            fieldsStream.seek(startPointer);

            final int docBase = fieldsStream.readVInt();
            final int chunkDocs = fieldsStream.readVInt();

            if (flipped) {
                accummulatedChunks += chunkDocs;
            }

            final int numStoredFields, offset, length, totalLength;
            if (chunkDocs == 1) {
                numStoredFields = fieldsStream.readVInt();
                offset = 0;
                length = fieldsStream.readVInt();
                totalLength = length;
            } else {
                final int bitsPerStoredFields = fieldsStream.readVInt();
                if (bitsPerStoredFields == 0) {
                    numStoredFields = fieldsStream.readVInt();
                } else if (bitsPerStoredFields > 31) {
                    throw new CorruptIndexException("bitsPerStoredFields=" + bitsPerStoredFields + " (resource=" + fieldsStream + ")");
                } else {
                    final long filePointer = fieldsStream.getFilePointer();
                    final PackedInts.Reader reader = PackedInts.getDirectReaderNoHeader(fieldsStream, PackedInts.Format.PACKED, packedIntsVersion, chunkDocs, bitsPerStoredFields);
                    numStoredFields = (int) (reader.get(docID - docBase));
                    fieldsStream.seek(filePointer + PackedInts.Format.PACKED.byteCount(packedIntsVersion, chunkDocs, bitsPerStoredFields));
                }

                final int bitsPerLength = fieldsStream.readVInt();
                if (bitsPerLength == 0) {
                    length = fieldsStream.readVInt();
                    offset = (docID - docBase) * length;
                    totalLength = chunkDocs * length;
                } else if (bitsPerStoredFields > 31) {
                    throw new CorruptIndexException("bitsPerLength=" + bitsPerLength + " (resource=" + fieldsStream + ")");
                } else {
                    final PackedInts.ReaderIterator it = PackedInts.getReaderIteratorNoHeader(fieldsStream, PackedInts.Format.PACKED, packedIntsVersion, chunkDocs, bitsPerLength, 1);
                    int off = 0;
                    for (int i = 0; i < docID - docBase; ++i) {
                        off += it.next();
                    }
                    offset = off;
                    length = (int) it.next();
                    off += length;
                    for (int i = docID - docBase + 1; i < chunkDocs; ++i) {
                        off += it.next();
                    }
                    totalLength = off;
                }
            }

            if ((length == 0) != (numStoredFields == 0)) {
                throw new CorruptIndexException("length=" + length + ", numStoredFields=" + numStoredFields + " (resource=" + fieldsStream + ")");
            }

            if (numStoredFields == 0) {
                // nothing to do
                return;
            }

            final DataInput documentInput;
            if (version >= VERSION_BIG_CHUNKS && totalLength >= 2 * chunkSize) {
                assert chunkSize > 0;
                assert offset < chunkSize;

                decompressor.decompress(fieldsStream, chunkSize, offset, Math.min(length, chunkSize - offset), bytes);
                documentInput = new DataInput() {

                    int decompressed = bytes.length;

                    void fillBuffer() throws IOException {
                        assert decompressed <= length;
                        if (decompressed == length) {
                            throw new EOFException();
                        }
                        final int toDecompress = Math.min(length - decompressed, chunkSize);
                        decompressor.decompress(fieldsStream, toDecompress, 0, toDecompress, bytes);
                        decompressed += toDecompress;
                    }

                    @Override
                    public byte readByte() throws IOException {
                        if (bytes.length == 0) {
                            fillBuffer();
                        }
                        --bytes.length;
                        return bytes.bytes[bytes.offset++];
                    }

                    @Override
                    public void readBytes(byte[] b, int offset, int len) throws IOException {
                        while (len > bytes.length) {
                            System.arraycopy(bytes.bytes, bytes.offset, b, offset, bytes.length);
                            len -= bytes.length;
                            offset += bytes.length;
                            fillBuffer();
                        }
                        System.arraycopy(bytes.bytes, bytes.offset, b, offset, len);
                        bytes.offset += len;
                        bytes.length -= len;
                    }

                };
            } else {
                final BytesRef bytes = totalLength <= BUFFER_REUSE_THRESHOLD ? this.bytes : new BytesRef();
                decompressor.decompress(fieldsStream, totalLength, offset, length, bytes);
                assert bytes.length == length;
                documentInput = new ByteArrayDataInput(bytes.bytes, bytes.offset, bytes.length);
            }

            for (int fieldIDX = 0; fieldIDX < numStoredFields; fieldIDX++) {
                final long infoAndBits = documentInput.readVLong();
                final int fieldNumber = (int) (infoAndBits >>> TYPE_BITS);
                final int bits = (int) (infoAndBits & TYPE_MASK);
                assert bits <= NUMERIC_DOUBLE : "bits=" + Integer.toHexString(bits);
                readField(visitor, documentInput, bits);
                previousLocation = fieldsStream.getCurrentLocation();
            }
        }
    }
}
