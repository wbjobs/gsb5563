// IndexedDB 持久层：保存事件日志、去重用的已见事件 ID、以及最近一次 Last-Event-ID。
const DB_NAME = 'sse-lab';
const DB_VERSION = 1;
const STORE_EVENTS = 'events';
const STORE_META = 'meta';

export class EventLogDB {
  constructor() {
    this.db = null;
  }

  open() {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, DB_VERSION);
      req.onupgradeneeded = () => {
        const db = req.result;
        const ev = db.createObjectStore(STORE_EVENTS, { keyPath: 'seq' });
        ev.createIndex('byTime', 'ts');
        ev.createIndex('byId', 'id');
        db.createObjectStore(STORE_META);
      };
      req.onsuccess = () => { this.db = req.result; resolve(this); };
      req.onerror = () => reject(req.error);
    });
  }

  _tx(store, mode) {
    return this.db.transaction(store, mode).objectStore(store);
  }

  putMeta(key, value) {
    return new Promise((resolve, reject) => {
      const req = this._tx(STORE_META, 'readwrite').put(value, key);
      req.onsuccess = () => resolve();
      req.onerror = () => reject(req.error);
    });
  }

  getMeta(key) {
    return new Promise((resolve, reject) => {
      const req = this._tx(STORE_META, 'readonly').get(key);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  bulkPutEvents(items) {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(STORE_EVENTS, 'readwrite');
      const store = tx.objectStore(STORE_EVENTS);
      for (const item of items) store.put(item);
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  getAllEvents() {
    return new Promise((resolve, reject) => {
      const req = this._tx(STORE_EVENTS, 'readonly').getAll();
      req.onsuccess = () => resolve(req.result || []);
      req.onerror = () => reject(req.error);
    });
  }

  getAllSeenIds() {
    return new Promise((resolve, reject) => {
      const req = this._tx(STORE_META, 'readonly').get('seenIds');
      req.onsuccess = () => resolve(new Set(req.result || []));
      req.onerror = () => reject(req.error);
    });
  }

  putSeenIds(set) {
    return this.putMeta('seenIds', [...set]);
  }

  clearEvents() {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction([STORE_EVENTS, STORE_META], 'readwrite');
      tx.objectStore(STORE_EVENTS).clear();
      tx.objectStore(STORE_META).clear();
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  countEvents() {
    return new Promise((resolve, reject) => {
      const req = this._tx(STORE_EVENTS, 'readonly').count();
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }
}
