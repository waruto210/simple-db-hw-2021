package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private int aField;
    private int gField;
    private Aggregator.Op aop;
    private TupleDesc childTd;
    private TupleDesc td;
    private Aggregator agg;
    private OpIterator innerIter;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of aField, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param aField The column over which we are computing an aggregate.
     * @param gField The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int aField, int gField, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.aField = aField;
        this.gField = gField;
        this.aop = aop;
        this.childTd = child.getTupleDesc();
        String aggColName = aop.toString() + "(" + childTd.getFieldName(aField) + ")";
        if (gField == Aggregator.NO_GROUPING) {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{aggColName});
            if (childTd.getFieldType(aField) == Type.INT_TYPE) {
                this.agg = new IntegerAggregator(gField, null, aField, aop);
            } else {
                this.agg = new StringAggregator(gField, null, aField, aop);
            }
        } else {
            Type gFiledType = childTd.getFieldType(gField);
            this.td = new TupleDesc(new Type[]{gFiledType, Type.INT_TYPE},
                    new String[]{childTd.getFieldName(gField), aggColName});
            if (childTd.getFieldType(aField) == Type.INT_TYPE) {
                this.agg = new IntegerAggregator(gField, gFiledType, aField, aop);
            } else {
                this.agg = new StringAggregator(gField, gFiledType, aField, aop);
            }
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
//        return -1;
        return gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
//        return null;

        if (gField == Aggregator.NO_GROUPING) {
            return null;
        }
        return childTd.getFieldName(gField);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
//        return -1;
        return aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
//        return null;
        return childTd.getFieldName(aField);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
//        return null;
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        while (child.hasNext()) {
            Tuple t = child.next();
            agg.mergeTupleIntoGroup(t);
        }
        innerIter = agg.iterator();
        innerIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
//        return null;
        if (innerIter.hasNext()) {
            Tuple t = innerIter.next();
            t.resetTupleDesc(td);
            return t;
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        innerIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        return null;
        return td;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
//        return null;
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child =  children[0];
    }

}
