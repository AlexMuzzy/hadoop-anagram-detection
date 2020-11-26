import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.shaded.org.apache.curator.shaded.com.google.common.collect.ComparisonChain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnagramCompositeKey implements Writable, WritableComparable<AnagramCompositeKey> {

    public IntWritable frequency;
    public Text keyName;

    /**
     * Blank default constructor. This is never used but it
     * throws a initialisation exception if it does not exist.
     */
    public AnagramCompositeKey() {
        this.frequency = new IntWritable();
        this.keyName = new Text();
    }

    /**
     * Default Constructor. Accepts key name and frequency as parameters.
     *
     * @param keyName   Value of anagram key name.
     * @param frequency Anagram key name count.
     */
    public AnagramCompositeKey(Text keyName, IntWritable frequency) {
        this.frequency = frequency;
        this.keyName = keyName;
    }

    public IntWritable getFrequency() {
        return frequency;
    }

    public void setFrequency(IntWritable frequency) {
        this.frequency = frequency;
    }

    public Text getKeyName() {
        return this.keyName;
    }

    public void setKeyName(Text keyName) {
        this.keyName = keyName;
    }

    /**
     * Gets the given attributes into a string and annotates the
     * given values for output.
     *
     * @return Key pair string value.
     */
    public Text getKeyPair() {
        return new Text(String.join(" ",
                "Count: " + this.frequency.toString(),
                " Anagram Key: " + this.keyName.toString()));
    }

    /**
     * Write out class's frequency and key name to IO output.
     *
     * @param dataOutput Data IO output.
     * @throws IOException On attempt to write IO file.
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.getFrequency().get());
        dataOutput.writeUTF(String.valueOf(this.keyName));
    }

    /**
     * Read frequency and key name values from IO files.
     *
     * @param dataInput Data IO input
     * @throws IOException On attempt to read IO file.
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.setFrequency(new IntWritable(dataInput.readInt()));
        this.setKeyName(new Text(dataInput.readUTF()));
    }

    /**
     * Secondary sort comparison used to compare in the order of frequency first,
     * then key name order.
     *
     * @param pair Pair of anagram composite keys.
     * @return Sorting result.
     */
    @Override
    public int compareTo(AnagramCompositeKey pair) {
        return ComparisonChain.start()
                .compare(this.frequency, pair.frequency)
                .compare(this.keyName, pair.keyName)
                .result();
    }
}