package simpledb.common;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class PageLockManager {

    private class PageRWLock {
        TransactionId tid;
        PageLockType lockType;

        public PageRWLock(TransactionId tid, PageLockType lockType) {
            this.tid = tid;
            this.lockType = lockType;
        }

        public void upgradeLock() {
            if (lockType == PageLockType.SHARED) {
                this.lockType = PageLockType.EXCLUSIVE;
            }

        }
    }

    private ConcurrentHashMap<PageId, Vector<PageRWLock>> lockMap;

    public PageLockManager() {
        this.lockMap = new ConcurrentHashMap<>();
    }

    public synchronized boolean acquireLock(TransactionId tid, PageId pid, PageLockType lockType) {
        // the page is not locked
        if (!lockMap.containsKey(pid)) {
            PageRWLock lock = new PageRWLock(tid, lockType);
            Vector<PageRWLock> locks = new Vector<>();
            locks.add(lock);
            lockMap.put(pid, locks);
            return true;
        }

        // the page is locked

        Vector<PageRWLock> locksOnPage = lockMap.get(pid);
        for (PageRWLock lock: locksOnPage) {
            // already hold a lock on the page
            if (lock.tid == tid) {
                if (lockType == PageLockType.SHARED || lock.lockType == PageLockType.EXCLUSIVE) {
                    return true;
                }
                // want to upgrade lock
                if (lock.lockType == PageLockType.SHARED && lockType == PageLockType.EXCLUSIVE) {
                    // only this tid hold shared lock
                    if (locksOnPage.size() == 1) {
                        lock.upgradeLock();
                        return true;
                    }
                }
                return false;
            }
        }

        // the page is locked but no lock held by this tid

        if (lockType == PageLockType.EXCLUSIVE ||
                locksOnPage.get(0).lockType == PageLockType.EXCLUSIVE) {
            return false;
        }

        // add a shared lock
        PageRWLock lock = new PageRWLock(tid, PageLockType.SHARED);
        locksOnPage.add(lock);
        return true;
    }

    public synchronized boolean releaseLock(TransactionId tid, PageId pid) {
        if (!lockMap.containsKey(pid)) {
            return false;
        }

        Vector<PageRWLock> locksOnPage = lockMap.get(pid);
        for (PageRWLock lock: locksOnPage) {
            if (lock.tid == tid) {
                locksOnPage.remove(lock);
                if (locksOnPage.size() == 0) {
                    lockMap.remove(pid);
                }
                return true;
            }
        }

        return false;
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        if (!lockMap.containsKey(pid)) {
            return false;
        }

        Vector<PageRWLock> locksOnPage = lockMap.get(pid);
        for (PageRWLock lock: locksOnPage) {
            if (lock.tid == tid) {
                return true;
            }
        }

        return false;
    }

}
