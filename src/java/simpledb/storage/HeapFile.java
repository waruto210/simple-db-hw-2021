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
        private Iterator<Tuple> currIt;

        private boolean opened;


        public HeapFileTupleIterator(int tableId, int numPg, TransactionId tid) {
            this.tableId = tableId;
            this.numPg = numPg;
            this.tid = tid;

            this.currPgNo = 0;
            this.currPage = null;
            this.currIt = null;
            this.opened = false;
        }

        private Iterator<Tuple> canReadNextPage() throws DbException, TransactionAbortedException {
            if (currPgNo < numPg) {
                HeapPageId pid = new HeapPageId(tableId, currPgNo);
                currPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                currPgNo++;
                return currPage.iterator();
            }
            return null;
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (!opened) {
                return null;
            }
            if (currIt != null && currIt.hasNext()) {
                return currIt.next();
            }
            while ((currIt = canReadNextPage()) != null) {
                if (currIt.hasNext()) {
                    return currIt.next();
                }
            }
            return null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            currIt = canReadNextPage();
            opened = true;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            currPgNo = 0;
            currPage = null;
            currIt = null;
        }

        @Override
        public void close() {
            super.close();
            opened = false;
            currIt = null;
            currPage = null;
            tid = null;
        }
    }

    private final File heapFile;
    private final TupleDesc td;
    private int numPage;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        try {
            f.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.heapFile = f;
        this.td = td;
        this.numPage = (int) (f.length() / BufferPool.getPageSize());
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
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageNo = pid.getPageNumber();
        if (pageNo >= numPages()) {
            try {
                HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
//                System.out.println("numPage is:" + numPage);
                numPage += 1;
                return newPage;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }

        int pageSize = BufferPool.getPageSize();

        byte[] data = new byte[pageSize];
        try {
            RandomAccessFile raf = new RandomAccessFile(heapFile, "rw");
            raf.seek((long) pageNo * pageSize);
            raf.readFully(data, 0, pageSize);
            raf.close();
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
        RandomAccessFile raf = new RandomAccessFile(heapFile, "rw");
        int pageSize = BufferPool.getPageSize();
        int offset = page.getId().getPageNumber() * pageSize;
        raf.seek(offset);
        raf.write(page.getPageData());

        raf.close();

        numPage = Math.max(numPage, (int) (heapFile.length() / pageSize));
//        System.out.println("numPage is:" + numPage);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // an ugly testcase write pages without invoking the `writePage` method
        return (int) Math.max(numPage, heapFile.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        BufferPool bufferPool = Database.getBufferPool();
        // found the first page that has empty slot
        int i = 0;
        while (true) {
            PageId tmp = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) bufferPool.getPage(tid, tmp, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                return Collections.singletonList(page);
            }
            bufferPool.unsafeReleasePage(tid, page.getId());
            i++;
        }
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
//        return null;
        // not necessary for lab1
        BufferPool bufferPool = Database.getBufferPool();
        RecordId rid = t.getRecordId();
        HeapPage page = (HeapPage) bufferPool.getPage(tid, rid.getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
//        t.setRecordId(null);

        return new ArrayList<Page>(Collections.singletonList((Page) page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileTupleIterator(getId(), numPages(), tid);
    }

}

