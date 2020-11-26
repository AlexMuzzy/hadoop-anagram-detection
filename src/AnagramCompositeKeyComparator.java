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

        // Set sort to ascending or descending depending on optional command line
        // parameters.
        return AnagramCountJob.paramArgs[1]*pair.getKeyName().compareTo(pair2.getKeyName());
    }
}
