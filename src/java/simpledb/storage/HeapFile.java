package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    public static class HeapFileTupleIterator extends AbstractDbFileIterator {
        private final int tableId;
        private final int numPg;
        private TransactionId tid;

        private int currPgNo;
        private HeapPage currPage;
        private Iterator<Tuple> it;

        private boolean opened;


        public HeapFileTupleIterator(int tableId, int numPg, TransactionId tid) {
            this.tableId = tableId;
            this.numPg = numPg;
            this.tid = tid;

            this.currPgNo = 0;
            this.currPage = null;
            this.it = null;
            this.opened = false;
        }

        private boolean canReadNextPage() throws DbException, TransactionAbortedException {
            if (currPgNo < numPg) {
                HeapPageId pid = new HeapPageId(tableId, currPgNo);
                currPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                it = currPage.iterator();
                currPgNo++;
                return true;
            }
            return false;
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (!opened) {
                return null;
            }
            if (it != null && it.hasNext()) {
                return it.next();
            }
            if (canReadNextPage()) {
                if (it != null && it.hasNext()) {
                    return it.next();
                }
            }
            return null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            canReadNextPage();
            opened = true;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            currPgNo = 0;
            currPage = null;
            it = null;
        }

        @Override
        public void close() {
            super.close();
            opened = false;
            it = null;
            currPage = null;
            tid = null;
        }
    }

    private final File heapFile;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.heapFile = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getHeapFile() {
        // some code goes here
        return heapFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return heapFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
//        return null;
        int pageSize = BufferPool.getPageSize();
        int pageNo = pid.getPageNumber();
        byte[] data = new byte[pageSize];
        try {
            RandomAccessFile raf = new RandomAccessFile(heapFile, "rw");
            raf.seek((long) pageNo * pageSize);
            raf.readFully(data, 0, pageSize);
//            System.out.println("pageSize: " + pageSize + " pageNum: " + pageNo + " fileSize: " + heapFile.length());
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
//        return 0;
        return (int) (heapFile.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileTupleIterator(getId(), numPages(), tid);
    }

}

