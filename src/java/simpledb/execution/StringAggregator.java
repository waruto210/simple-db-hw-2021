package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op what;
    private TupleDesc td;
    private Tuple result;
    private ConcurrentHashMap<Field, Tuple> groupResults;

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
        this.gbFieldType = gbFieldType;
        this.aField = aField;
        this.what = what;
        if (gbField == NO_GROUPING) {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
            result = new Tuple(td);
            result.setField(0, new IntField(0));
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
        switch (what) {
            case COUNT:
                if (gbField == NO_GROUPING) {
                    result.setField(0, new IntField(((IntField)result.getField(0)).getValue() + 1));
                } else {
                    Field gbVal = tup.getField(gbField);
                    if (groupResults.containsKey(gbVal)) {
                        groupResults.get(gbVal).setField(1,
                                new IntField(((IntField)groupResults.get(gbVal).getField(1)).getValue() + 1));
                    } else {
                        Tuple newTup = new Tuple(td);
                        newTup.setField(0, gbVal);
                        newTup.setField(1, new IntField(1));
                        groupResults.put(gbVal, newTup);
                    }
                }
                break;
            default:
                System.out.println("unsupported operation");
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
        Tuple[] tuples = null;
        int numTuples = 1;
        if (gbField != NO_GROUPING) {
            numTuples = groupResults.size();
        }
        tuples = new Tuple[numTuples];
        if (gbField == NO_GROUPING) {
            tuples[0] = result;
        } else {
            int i = 0;
            for (ConcurrentHashMap.Entry<Field, Tuple> entry: groupResults.entrySet()) {
                tuples[i++] = entry.getValue();
            }
        }
        return new IntegerAggregator.AggResultIterator(tuples, td);
    }

}
