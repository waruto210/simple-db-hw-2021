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

    private static class AvgTuple {
        private int preSum = 0;
        private int merged = 0;
        private int aField = 0;
        private Tuple resultTuple = null;
        private Field gbFieldValue = null;
        private TupleDesc td;

        public AvgTuple(int aField, TupleDesc td) {
            this(aField, td, null);
        }

        public AvgTuple(int aField, TupleDesc td, Field gbFieldValue) {
            this.aField = aField;
            this.td = td;
            if (gbFieldValue == null) {
                this.resultTuple = new Tuple(td);
                this.resultTuple.setField(0, new IntField(0));
            } else {
                this.resultTuple = new Tuple(td);
                this.resultTuple.setField(0, gbFieldValue);
                this.resultTuple.setField(1, new IntField(0));
            }
            this.gbFieldValue = gbFieldValue;
        }

        public Tuple getResultTuple() {
            return resultTuple;
        }

        public void mergeOne(Tuple tup) {
            preSum += ((IntField) tup.getField(aField)).getValue();
            merged++;
            if (gbFieldValue == null) {
                resultTuple.setField(0, new IntField(preSum / merged));
            } else {
                resultTuple.setField(1, new IntField(preSum / merged));
            }

        }
    }

    public static class AggResultIterator extends Operator {
        private Tuple[] resultTuples;
        private TupleDesc td;
        private int curr = 0;

        public AggResultIterator(Tuple[] resultTuples, TupleDesc td) {
            this.resultTuples = resultTuples;
            this.td = td;
        }


        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.curr = 0;
        }

        @Override
        protected Tuple fetchNext() throws DbException, TransactionAbortedException {
            if (curr < resultTuples.length) {
                return resultTuples[curr++];
            } else {
                return null;
            }
        }

        @Override
        public OpIterator[] getChildren() {
            return null;
        }

        @Override
        public void setChildren(OpIterator[] children) {

        }

        @Override
        public TupleDesc getTupleDesc() {
            return td;
        }
    }

    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op what;
    private Object result = null;
    private ConcurrentHashMap<Field, Object> groupResult = null;
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
        this.gbFieldType = gbFieldType;
        this.aField = aField;
        this.what = what;
        if (gbField == NO_GROUPING) {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
            if (what == Op.COUNT || what == Op.SUM) {
                this.result = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
                ((Tuple) this.result).setField(0, new IntField(0));
            } else if (what == Op.AVG) {
                this.result = new AvgTuple(aField, td);
            }
            // MIN, MAX null
        } else {
            this.td = new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE});
            this.groupResult = new ConcurrentHashMap<Field, Object>(16);
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
        switch (what) {
            case SUM:
                if (gbField == NO_GROUPING) {
                    ((Tuple) result).setField(0,
                            new IntField(((IntField)((Tuple) result).getField(0)).getValue() +
                                    ((IntField) tup.getField(aField)).getValue()));
                } else {
                    Field gbFieldValue = tup.getField(gbField);
                    if (groupResult.containsKey(gbFieldValue)) {
                        Tuple preSum = (Tuple) groupResult.get(gbFieldValue);
                        preSum.setField(1, new IntField(((IntField) preSum.getField(1)).getValue() +
                                ((IntField)tup.getField(aField)).getValue()));
                    } else {
                        Tuple s = new Tuple(td);
                        s.setField(0, gbFieldValue);
                        s.setField(1, tup.getField(aField));
                        groupResult.put(gbFieldValue, s);
                    }
                }
                break;
            case MIN:
                if (gbField == NO_GROUPING) {
                    if (result == null) {
                        // first time
                        Tuple tmp = new Tuple(td);
                        tmp.setField(0, tup.getField(aField));
                        result = tmp;
                    } else {
                        if ( tup.getField(aField).compare(Predicate.Op.LESS_THAN,
                                ((Tuple)result).getField(0))) {
                            ((Tuple)result).setField(0, tup.getField(aField));
                        }
                    }
                } else {
                    Field gbFieldValue = tup.getField(gbField);
                    if (groupResult.containsKey(gbFieldValue)) {
                        Tuple preMin = (Tuple) groupResult.get(gbFieldValue);
                        if (tup.getField(aField).compare(Predicate.Op.LESS_THAN, preMin.getField(1))) {
                            preMin.setField(1, tup.getField(aField));
                        }
                    } else {
                        Tuple s = new Tuple(td);
                        s.setField(0, gbFieldValue);
                        s.setField(1, tup.getField(aField));
                        groupResult.put(gbFieldValue, s);
                    }
                }
                break;
            case MAX:
                if (gbField == NO_GROUPING) {
                    if (result == null) {
                        // first time
                        Tuple tmp = new Tuple(td);
                        tmp.setField(0, tup.getField(aField));
                        result = tmp;
                    } else {
                        if ( tup.getField(aField).compare(Predicate.Op.GREATER_THAN,
                                ((Tuple)result).getField(0))) {
                            ((Tuple)result).setField(0, tup.getField(aField));
                        }
                    }
                } else {
                    Field gbFieldValue = tup.getField(gbField);
                    if (groupResult.containsKey(gbFieldValue)) {
                        Tuple preMin = (Tuple) groupResult.get(gbFieldValue);
                        if (tup.getField(aField).compare(Predicate.Op.GREATER_THAN, preMin.getField(1))) {
                            preMin.setField(1, tup.getField(aField));
                        }
                    } else {
                        Tuple s = new Tuple(td);
                        s.setField(0, gbFieldValue);
                        s.setField(1, tup.getField(aField));
                        groupResult.put(gbFieldValue, s);
                    }
                }
                break;
            case AVG:
                if (gbField == NO_GROUPING) {
                    ((AvgTuple)result).mergeOne(tup);
                } else {
                    Field gbFieldValue = tup.getField(gbField);
                    if (groupResult.containsKey(gbFieldValue)) {
                        ((AvgTuple)groupResult.get(gbFieldValue)).mergeOne(tup);
                    } else {
                        AvgTuple s = new AvgTuple(aField, td, gbFieldValue);
                        s.mergeOne(tup);
                        groupResult.put(gbFieldValue, s);
                    }
                }
                break;
            case COUNT:
                if (gbField == NO_GROUPING) {
                    ((Tuple)result).setField(0,
                            new IntField(((IntField)((Tuple)result).getField(0)).getValue() + 1));
                } else {
                    Field gbFieldValue = tup.getField(gbField);
                    if (groupResult.containsKey(gbFieldValue)) {
                        Tuple preCount = (Tuple) groupResult.get(gbFieldValue);
                        preCount.setField(1, new IntField(((IntField)preCount.getField(1)).getValue() + 1));
                    } else {
                        Tuple s = new Tuple(td);
                        s.setField(0, gbFieldValue);
                        s.setField(1, new IntField(1));
                        groupResult.put(gbFieldValue, s);
                    }
                }
                break;
            default:
                System.out.println("Unknown Aggregate Function");
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
        Tuple[] resultTuples =  null;
        int numResults = 1;
        if (gbField != NO_GROUPING) {
            numResults = groupResult.size();
        }
        resultTuples = new Tuple[numResults];
        if (gbField == NO_GROUPING) {
            if (what == Op.AVG) {
                resultTuples[0] = ((AvgTuple)result).getResultTuple();
            } else {
                resultTuples[0] = (Tuple)result;
            }
        } else {
            int i = 0;
            for (ConcurrentHashMap.Entry<Field, Object> entry : groupResult.entrySet()) {
                if (what == Op.AVG) {
                    resultTuples[i++] = ((AvgTuple)entry.getValue()).getResultTuple();
                }
                else {
                    resultTuples[i++] = (Tuple)entry.getValue();
                }

            }
        }
        return new AggResultIterator(resultTuples, td);
    }

}
