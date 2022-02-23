package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbField;
    private final int aField;
    private final Op what;
    private final TupleDesc td;
    private AggRecord result;
    private ConcurrentHashMap<Field, AggRecord> groupResults;

    /**
     * Aggregate constructor
     * @param gbField the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbFieldType the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param aField the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbField, Type gbFieldType, int aField, Op what) {
        // some code goes here
        this.gbField = gbField;
        this.what = what;
        this.aField = aField;
        if (gbField == NO_GROUPING) {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
            result = new AggRecord(what, aField, td);
        } else {
            this.td = new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE});
            this.groupResults = new ConcurrentHashMap<>(16);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbField == NO_GROUPING) {
            result.mergeOne(tup);
        } else {
            Field gbFieldValue = tup.getField(gbField);
            if (groupResults.containsKey(gbFieldValue)) {
                AggRecord record = groupResults.get(gbFieldValue);
                record.mergeOne(tup);
            } else {
                AggRecord record = new AggRecord(what, aField, td, gbFieldValue);
                record.mergeOne(tup);
                groupResults.put(gbFieldValue, record);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
//        throw new UnsupportedOperationException("please implement me for lab2");
        Tuple[] resultTuples;
        int numTuples = 1;
        if (gbField != NO_GROUPING) {
            numTuples = groupResults.size();
        }
        resultTuples = new Tuple[numTuples];
        if (gbField == NO_GROUPING) {
            resultTuples[0] = result.getResultTuple();
        } else {
            int i = 0;
            for (ConcurrentHashMap.Entry<Field, AggRecord> entry: groupResults.entrySet()) {
                resultTuples[i++] = entry.getValue().getResultTuple();
            }
        }
        return new AggResultIterator(resultTuples, td);
    }

}
