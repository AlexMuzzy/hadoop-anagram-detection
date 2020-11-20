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

    public AnagramCompositeKey() {
        this.frequency = new IntWritable();
        this.keyName = new Text();
    }

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

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.getFrequency().get());
        dataOutput.writeUTF(String.valueOf(this.keyName));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.setFrequency(new IntWritable(dataInput.readInt()));
        this.setKeyName(new Text(dataInput.readUTF()));
    }

    @Override
    public int compareTo(AnagramCompositeKey pair) {
        return ComparisonChain.start().compare(this.frequency, pair.frequency).compare(this.keyName, pair.keyName).result();
    }
}