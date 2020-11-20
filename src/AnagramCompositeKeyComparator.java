import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AnagramCompositeKeyComparator extends WritableComparator {

    public AnagramCompositeKeyComparator() {
        super(AnagramCompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        AnagramCompositeKey pair = (AnagramCompositeKey) a;
        AnagramCompositeKey pair2 = (AnagramCompositeKey) b;

        return pair.getKeyName().compareTo(pair2.getKeyName());
    }
}
