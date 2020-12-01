import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

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

    public void setFrequency(IntWritable frequency) {
        this.frequency = frequency;
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
        dataOutput.writeInt(this.frequency.get());
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

        int frequencySort = AnagramReducer.optionalParamsValues[0]
                * this.frequency.compareTo(pair.frequency);
        int keyNameSort = AnagramReducer.optionalParamsValues[1]
                * this.keyName.compareTo(pair.keyName);

        //If word count comparison are different, return its positions.
        if (frequencySort != 0) return frequencySort;
        //Return Position of anagram words given the same word count.
        return keyNameSort;
    }
}