package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.concurrent.ConcurrentHashMap;


/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private int aField;
    private Op what;
    private AggRecord result;
    private ConcurrentHashMap<Field, AggRecord> groupResults;
    private TupleDesc td;


    /**
     * Aggregate constructor
     * 
     * @param gbField
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbFieldType
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param aField
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbField, Type gbFieldType, int aField, Op what) {
        // some code goes here
        this.gbField = gbField;
        this.aField = aField;
        this.what = what;
        if (gbField == NO_GROUPING) {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
            this.result = new AggRecord(what, aField, td);
        } else {
            this.td = new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE});
            this.groupResults = new ConcurrentHashMap<Field, AggRecord>(16);
        }
    }


    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
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
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
//        throw new
//        UnsupportedOperationException("please implement me for lab2");
        Tuple[] resultTuples;
        int numResults = 1;
        if (gbField != NO_GROUPING) {
            numResults = groupResults.size();
        }
        resultTuples = new Tuple[numResults];
        if (gbField == NO_GROUPING) {
            resultTuples[0] = result.getResultTuple();
        } else {
            int i = 0;
            for (ConcurrentHashMap.Entry<Field, AggRecord> entry : groupResults.entrySet()) {
                resultTuples[i++] = entry.getValue().getResultTuple();
            }
        }
        return new AggResultIterator(resultTuples, td);
    }

}
