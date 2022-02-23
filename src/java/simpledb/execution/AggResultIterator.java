package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

public class AggResultIterator extends Operator {
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
