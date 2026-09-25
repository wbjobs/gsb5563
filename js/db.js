// IndexedDB 事件日志持久化
const EventLogDB = (() => {
  const DB_NAME = 'sse-event-log';
  const STORE = 'events';
  let dbPromise = null;

  function open() {
    if (dbPromise) return dbPromise;
    dbPromise = new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, 1);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(STORE)) {
          db.createObjectStore(STORE, { keyPath: 'seq', autoIncrement: true });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
    return dbPromise;
  }

  function tx(mode, fn) {
    return open().then(db => new Promise((resolve, reject) => {
      const t = db.transaction(STORE, mode);
      const store = t.objectStore(STORE);
      const out = fn(store);
      t.oncomplete = () => resolve(out && out._result !== undefined ? out._result : undefined);
      t.onerror = () => reject(t.error);
    }));
  }

  return {
    // 批量写入（单事务，1 万条无压力）
    addBatch(events) {
      return open().then(db => new Promise((resolve, reject) => {
        const t = db.transaction(STORE, 'readwrite');
        const store = t.objectStore(STORE);
        for (const ev of events) store.add(ev);
        t.oncomplete = () => resolve();
        t.onerror = () => reject(t.error);
      }));
    },
    getAll() {
      return open().then(db => new Promise((resolve, reject) => {
        const req = db.transaction(STORE).objectStore(STORE).getAll();
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
      }));
    },
    clear() { return tx('readwrite', s => s.clear()); },
    count() {
      return open().then(db => new Promise((resolve, reject) => {
        const req = db.transaction(STORE).objectStore(STORE).count();
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
      }));
    },
  };
})();
