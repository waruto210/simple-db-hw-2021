package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {

    private static final long TIMEOUT_BASE = 600;
    private static final int TIMEOUT_GAP = 200;
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;
    private final ConcurrentHashMap<PageId, Page> pagePool;
    private final Vector<PageId> pageQueue;
    private final PageLockManager pageLockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pagePool = new ConcurrentHashMap<>(numPages);
        this.pageQueue = new Vector<>(numPages);
        this.pageLockManager = new PageLockManager();
    }

    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

        // some code goes here
        long start = System.currentTimeMillis();
        long timeout = new Random().nextInt(TIMEOUT_GAP) + TIMEOUT_BASE;

        PageLockType lockType = getLockType(perm);
        while (!pageLockManager.acquireLock(tid, pid, lockType)) {
            if (System.currentTimeMillis() - start > timeout) {
                throw new TransactionAbortedException();
            }
        }

        synchronized (pagePool) {
            if (pagePool.containsKey(pid)) {
                pageQueue.remove(pid);
                pageQueue.add(pid);
                return pagePool.get(pid);
            }
        }

        synchronized (pagePool) {
            if (pagePool.size() == numPages) {
                evictPage();
            }
            Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pagePool.put(pid, page);
            pageQueue.add(pid);
            return page;
        }

    }

    private static PageLockType getLockType(Permissions perm) {
        switch (perm) {
            case READ_ONLY:
                return PageLockType.SHARED;
            case READ_WRITE:
                return PageLockType.EXCLUSIVE;
            default:
                return null;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        // 因为strict-2PL只能在事务提交时释放锁，所以这里逻辑上unsafe，可以在扫描empty slot之后释放page时使用
        pageLockManager.releaseLock(tid, pid);

    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
//        return false;
        return pageLockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        try {
            if (commit) {
                for (PageId pid: pagePool.keySet()) {
                    Page p = pagePool.get(pid);
                    if (p.isDirty() == tid) {
                        flushPage(pid);
                        p.setBeforeImage();
                    }
                }
            } else {
                restorePages(tid);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // release all locks
//        for (PageId pid : pagePool.keySet()) {
//            if (holdsLock(tid, pid)) {
//                pageLockManager.releaseLock(tid, pid);
//            }
//        }
        pageLockManager.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = file.insertTuple(tid, t);
        if (dirtyPages != null) {
            for (Page p: dirtyPages) {
                p.markDirty(true, tid);
                // just to pass BufferPoolWriteTest
                if (!pagePool.containsKey(p.getId())) {
                    if (pagePool.size() == numPages) {
                        evictPage();
                    }
                    // add new dirty page to bufferPool
                    pageQueue.add(p.getId());
                    pagePool.put(p.getId(), p);
                }
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(
                t.getRecordId().getPageId().getTableId());
        List<Page> dirtyPages = file.deleteTuple(tid, t);
        if (dirtyPages != null) {
            for (Page p: dirtyPages) {
                p.markDirty(true, tid);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid: pagePool.keySet()) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageQueue.remove(pid);
        pagePool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pagePool.get(pid);
        TransactionId dirtier = page.isDirty();
        if (dirtier != null) {
            Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
            Database.getLogFile().force();
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
        }
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid: pagePool.keySet()) {
            Page p = pagePool.get(pid);
            if (p.isDirty() == tid) {
                flushPage(pid);
            }
        }
    }

    /** Read all pages of the specified transaction from disk.
     */
    public synchronized void restorePages(TransactionId tid) throws IOException {
        for (PageId pid: pagePool.keySet()) {
            Page p = pagePool.get(pid);
            if (p.isDirty() == tid) {
                Page cleanPage = Database.getCatalog().getDatabaseFile(p.getId().getTableId()).readPage(pid);
                pagePool.put(pid, cleanPage);
            }
        }
    }


    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
//        try {
            for (PageId pid: pageQueue) {
                Page page = pagePool.get(pid);
                if (page.isDirty() == null) {
                    discardPage(pid);
                    return;
                }
            }
            throw new DbException("all pages are dirty");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }

}
