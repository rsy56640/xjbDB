#ifndef _BUFFER_POOL_H
#define _BUFFER_POOL_H
#include "hash_lru.h"
#include "disk_manager.h"

namespace DB::buffer
{
    using page::Page;

    // bufferpool caches some of the pages in the memory,
    // pages not cached in bufferpool are evicted and `page->ref==0`, flush automatically.
    // while others not cached in bufferpool but held in some position of the execution,
    // aka. `find()` then `evict()`, after the execution, the page should be `unref()`
    // finally the page was flushed.
    class BufferPoolManager
    {
    public:

        BufferPoolManager(const BufferPoolManager&) = delete;
        BufferPoolManager(BufferPoolManager&&) = delete;
        BufferPoolManager& operator=(const BufferPoolManager&) = delete;
        BufferPoolManager& operator=(BufferPoolManager&&) = delete;

        BufferPoolManager(disk::DiskManager* disk_manager);

        // return the corresponding Page*.
        // the Page* is `ref()` before return.
        // note that the Page* might be evicted from lru.
        // user should *`unref()`* the page after use ! ! !
        Page* FetchPage(page_id_t page_id);

        // flush the page in the bufferpool, return true.
        // if the page is not in the bufferpool, return false,
        // in which case, the page is held at some execution, and later to be `unref()`.
        bool FlushPage(page_id_t page_id);

        // the Page* has been `ref()` before return.
        // user should *`unref()`* the page after use ! ! !
        Page* NewPage(PageInitInfo);

        // who fucking else should invoke this ???
        // remove page_id from hash-lru
        bool DeletePage(page_id_t page_id);

        // flush dirty page into disk.
        void flush();


    public:
        disk::DiskManager* disk_manager_;

    private:

        Hash_LRU hash_lru_;
        //log::LogManager* log_manager_;

    }; // end class BufferPoolManager


} // end namespace DB::buffer

#endif // !_BUFFER_POOL_H