package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public class PageLockManager {

    private HashMap<PageId, HashMap<TransactionId, PageLockType>> lockMap;

    public PageLockManager() {
        this.lockMap = new HashMap<>(100);
    }

    public synchronized boolean acquireLock(TransactionId tid, PageId pid, PageLockType lockType) {
        // the page is not locked
        if (!lockMap.containsKey(pid) || lockMap.get(pid).size() == 0) {
             HashMap<TransactionId, PageLockType> locksOnPage = lockMap.getOrDefault(pid, new HashMap<>());
             locksOnPage.put(tid, lockType);
             lockMap.put(pid, locksOnPage);
             return true;
        }

        // the page is locked
        HashMap<TransactionId, PageLockType> locksOnPage = lockMap.get(pid);
        // if already hold a lock on the page
        if (locksOnPage.containsKey(tid)) {
            // hold same type lock or hold exclusive lock
            // acquire shared, hold shared, exclusive
            // acquire exclusive, hold exclusive
            if (locksOnPage.get(tid) == lockType || locksOnPage.get(tid) == PageLockType.EXCLUSIVE) {
                return true;
            }
            // hold a shared lock
            if (locksOnPage.get(tid) == PageLockType.SHARED) {
                // only this tx hold a shared lock, upgrade the lock
                if (locksOnPage.size() == 1) {
                    locksOnPage.put(tid, PageLockType.EXCLUSIVE);
                    return true;
                }
            }
            return false;
        }

        // the page is locked but no lock held by this tx
        if (lockType == PageLockType.EXCLUSIVE) {
            return false;
        }

        for (PageLockType lType: locksOnPage.values()) {
            // an exclusive lock is held by other tx
            if (lType == PageLockType.EXCLUSIVE) {
                return false;
            }
        }
        // add a shared lock
        locksOnPage.put(tid, PageLockType.SHARED);
        return true;
    }

    public synchronized boolean releaseLock(TransactionId tid, PageId pid) {
        if (!lockMap.containsKey(pid)) {
            return false;
        }

        HashMap<TransactionId, PageLockType> locksOnPage = lockMap.get(pid);
        if (!locksOnPage.containsKey(tid)) {
            return false;
        }
        locksOnPage.remove(tid);

        return true;
    }

    public synchronized void releaseAllLocks(TransactionId tid) {
        for (HashMap<TransactionId, PageLockType> locksOnPage: lockMap.values()) {
            locksOnPage.remove(tid);
        }
    }

    public synchronized PageLockType holdsLock(TransactionId tid, PageId pid) {
        if (!lockMap.containsKey(pid)) {
            return null;
        }

        HashMap<TransactionId, PageLockType> locksOnPage = lockMap.get(pid);
        if (locksOnPage.containsKey(tid)) {
            return locksOnPage.get(tid);
        }
        return null;
    }

}
