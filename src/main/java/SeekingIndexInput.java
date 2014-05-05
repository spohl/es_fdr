import org.apache.lucene.store.IndexInput;

import java.io.IOException;

public final class SeekingIndexInput extends IndexInput {

    private final IndexInput input;
    private long currentLocation;

    public SeekingIndexInput(IndexInput input, long startIndex) {
        super(input.toString());
        this.input = input;
        this.currentLocation = startIndex;
    }

    public long getCurrentLocation() {
        return currentLocation;
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    @Override
    public long getFilePointer() {
        return input.getFilePointer();
    }

    @Override
    public void seek(long l) throws IOException {
        this.currentLocation = l;
        input.seek(l);
    }

    @Override
    public long length() {
        return input.length();
    }

    @Override
    public byte readByte() throws IOException {
        this.currentLocation++;
        return input.readByte();
    }

    @Override
    public void readBytes(byte[] bytes, int offset, int length) throws IOException {
        this.currentLocation += length;
        input.readBytes(bytes, offset, length);
    }
}
