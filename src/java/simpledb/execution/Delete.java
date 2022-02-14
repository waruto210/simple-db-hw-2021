package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator child;
    private TupleDesc td;
    private boolean delete = false;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"cnt"});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
//        return null;
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
//        child.rewind();
        throw new DbException("Delete operator does not support rewind()");
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
//        return null;
        if (delete) {
            return null;
        }
        int count = 0;
        while (child.hasNext()) {
            Tuple t = child.next();
            try {
                Database.getBufferPool().deleteTuple(tid, t);
                count++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        delete = true;
        Tuple ret = new Tuple(td);
        ret.setField(0, new IntField(count));
        return ret;
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
        child = children[0];
    }

}
