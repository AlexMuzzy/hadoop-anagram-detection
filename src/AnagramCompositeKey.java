import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class AnagramCompositeKey {

    public IntWritable frequency;
    public Text keyName;

    public AnagramCompositeKey(Text keyName) {
        this.keyName = keyName;
        this.frequency = new IntWritable(1);
    }

    public int getFrequency() {
        return frequency.get();
    }

    public void incrementFrequency() {
        this.frequency.set(this.frequency.get() + 1);
    }

    public Text getKeyName() {
        return keyName;
    }

    public void setKeyName(Text keyName) {
        this.keyName = keyName;
    }
}
