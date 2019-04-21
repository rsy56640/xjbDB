#include "include/hash_lru.h"
#include "include/debug_log.h"

namespace DB::buffer
{

    /****************  implementation for class PageList  ****************/

    PageList::PageList() :size_(0), head_(page::NOT_A_PAGE, nullptr)
    {
        head_.prev_hash_ = &head_;
        head_.next_hash_ = &head_;
    }

    void PageList::append(PageListHandle* handle) {
        handle->ref();
        size_++;
        handle->in_page_list_ = true;
        PageListHandle* old = head_.next_hash_;
        head_.next_hash_ = handle; handle->prev_hash_ = &head_;
        handle->next_hash_ = old; old->prev_hash_ = handle;
    }

    void PageList::remove(PageListHandle* handle) {
        size_--;
        handle->in_page_list_ = false;
        PageListHandle* prev = handle->prev_hash_;
        PageListHandle* next = handle->next_hash_;
        prev->next_hash_ = next; next->prev_hash_ = prev;
        handle->unref();
    }

    PageListHandle* PageList::find_handle(page_id_t page_id) const {
        PageListHandle* it = head_.next_hash_;
        while (it != &head_) {
            if (it->page_id_ == page_id) return it;
            it = it->next_hash_;
        }
        return nullptr;
    }

    PageList::~PageList() {
        PageListHandle* it = head_.next_hash_;
        while (it != &head_) {
            PageListHandle* temp = it;
            it = it->next_hash_;
            temp->unref();
        }
    }


    /****************  implementation for class Hash_LRU  ****************/

    // HACK: potential optimization:
    // make consensus with the caller on `page_id`,
    // such that the data race will not happen on the same handle !!!

    Hash_LRU::Hash_LRU() :
        bucket_num_(Hash_LRU::init_bucket),
        buckets_(bucket_num_),
        size_(0),
        lru_head_(page::NOT_A_PAGE, nullptr),
        //lru_size_(0),
        lru_max_size_(lru_init_size)
    {
        lru_head_.next_lru_ = &lru_head_;
        lru_head_.prev_lru_ = &lru_head_;
    }

    Hash_LRU::~Hash_LRU() {
        PageListHandle* it = lru_head_.next_lru_;
        while (it != &lru_head_) {
            PageListHandle* temp = it;
            it = it->next_lru_;
            temp->unref();
        }
    }


    // `ref_count` in PageList is tracked automatically, we only manage `ref_count` in LRU.
    // We manually manipulate `ref_count`, though, in some multi-thread conditions.

    bool Hash_LRU::insert(page_id_t page_id, Page* page)
    {
        if (size() > max_size())
            lru_evict();

        bool _rehash;
        {
            std::shared_lock<std::shared_mutex> lock(shared_mutex_);

            const uint32_t _hash = hash(page_id);
            PageList& page_list = buckets_[_hash];
            PageListHandle* _page_handle = nullptr;
            PageListHandle* newHandle = nullptr;

            {
                std::lock_guard<std::mutex> page_lg(page_list.mutex_);
                _rehash = page_list.size_ > (bucket_num_ << rehash_ratio);

                _page_handle = page_list.find_handle(page_id);
                if (_page_handle != nullptr)
                    return false;

                newHandle = new PageListHandle(page_id, page);
                page_list.append(newHandle);
            }

            size_++;

            {
                std::lock_guard<std::mutex> lru_lg(lru_mutex_);
                lru_append(newHandle);
            }
        }

        if (_rehash)
            rehash();

        return true;
    }


    bool Hash_LRU::insert_or_assign(page_id_t page_id, Page* page)
    {
        if (size() > max_size())
            lru_evict();

        bool _rehash;
        {
            std::shared_lock<std::shared_mutex> lock(shared_mutex_);

            const uint32_t _hash = hash(page_id);
            PageList& page_list = buckets_[_hash];
            PageListHandle* _page_handle = nullptr;
            PageListHandle* newHandle = nullptr;

            {
                std::lock_guard<std::mutex> page_lg(page_list.mutex_);
                _rehash = page_list.size_ > (bucket_num_ << rehash_ratio);

                _page_handle = page_list.find_handle(page_id);
                if (_page_handle != nullptr)
                {
                    _page_handle->page_ = page;
                    return false;
                }

                newHandle = new PageListHandle(page_id, page);
                page_list.append(newHandle);
            }

            size_++;

            {
                std::lock_guard<std::mutex> lru_lg(lru_mutex_);
                lru_append(newHandle);
            }
        }

        if (_rehash)
            rehash();

        return true;
    }


    Page* Hash_LRU::find(page_id_t page_id, const bool update)
    {
        std::shared_lock<std::shared_mutex> lock(shared_mutex_);

        uint32_t _hash = hash(page_id);
        const PageList& page_list = buckets_[_hash];

        PageListHandle* _page_handle = nullptr;
        Page* page_ptr = nullptr;

        // find the page and try to acquire the lock
        {
            std::lock_guard<std::mutex> page_lg(page_list.mutex_);

            _page_handle = page_list.find_handle(page_id);

            // return if no such page.
            if (_page_handle == nullptr)
                return nullptr;

            page_ptr = _page_handle->page_;

            // in case that handle should be evited and page was ref to 0.
            page_ptr->ref();
            _page_handle->ref();
        }

        {
            std::lock_guard<std::mutex> lru_lg(lru_mutex_);

            if (_page_handle->in_lru_ == false)
            {
                _page_handle->unref(); // pairwise with the early `ref()`
                return page_ptr;
            }

            _page_handle->unref(); // pairwise with the early `ref()`

            if (update)
                lru_update(_page_handle);
        }

        return page_ptr;
    }


    bool Hash_LRU::erase(page_id_t page_id)
    {
        std::shared_lock<std::shared_mutex> lock(shared_mutex_);

        uint32_t _hash = hash(page_id);
        PageList& page_list = buckets_[_hash];

        PageListHandle* _page_handle = nullptr;

        // remove handle from page_list
        {
            std::lock_guard<std::mutex> page_lg(page_list.mutex_);
            _page_handle = page_list.find_handle(page_id);

            if (_page_handle == nullptr)
                return false;

            // `ref()` the handle here to extend its life
            // at least after acquire the LRU_lock in case that:
            // another thread attempts to evict the handle from LRU,
            // if we leave this block, the `evict()` will do nothing but `unref()`,
            // therefore the handle will be deleted.
            _page_handle->ref();

            page_list.remove(_page_handle); // set `in_page_list_ = false` inside.

        }

        // remove handle from LRU
        {
            std::lock_guard<std::mutex> lru_lg(lru_mutex_);

            if (_page_handle->in_lru_ == false)
            {
                // `size--` is in LRU
                _page_handle->unref(); // pairwise with the early `ref()`
                return true;
            }
            // `size--` is outside the block for less time of holding the lock.
            lru_remove(_page_handle);

            _page_handle->unref(); // pairwise with the early `ref()` 
        }
        size_--;

        return true;
    }


    inline uint32_t Hash_LRU::size() const {
        return size_.load(std::memory_order_relaxed);
    }


    inline uint32_t Hash_LRU::max_size() const {
        return lru_max_size_.load(std::memory_order_relaxed);
    }


    void Hash_LRU::flush() {
        std::lock_guard<std::shared_mutex> lg(shared_mutex_);
        for (PageList& bucket : buckets_) {
            PageListHandle* it = bucket.head_.next_hash_;
            while (it != &bucket.head_) {
                it->page_->flush();
                it = it->next_hash_;
            }
        }
    }


    // The read of `bucket_num_` does not contend with modifying it in `rehash()`.
    // Becase the modification must acquire the write-lock,
    // thus any find/insert/erase call will block.
    inline uint32_t Hash_LRU::hash(page_id_t page_id) const noexcept {
        return (Hash_LRU::magic * page_id) & (bucket_num_ - 1);
    }


    // Do not lock LRU !!!
    void Hash_LRU::rehash()
    {
        // must immediately acquire the write-lock
        std::lock_guard<std::shared_mutex> lg(shared_mutex_);

        if (bucket_num_ >= Hash_LRU::max_bucket)
            return;

        // `bucket number` and `lru_max_size` multiply by 2.
        const uint32_t old_bucket_num_ = bucket_num_;
        bucket_num_ <<= 1;
        lru_max_size_.fetch_add(
            lru_max_size_.load(std::memory_order_seq_cst),
            std::memory_order_seq_cst);

        // move the element into new buckets
        buckets_.resize(bucket_num_);
        for (uint32_t i = 0; i < old_bucket_num_; i++)
        {
            // we hold write-lock already.
            PageList& page_list = buckets_[i];
            PageList& new_page_list = buckets_[i + old_bucket_num_];
            PageListHandle* it = page_list.head_.next_hash_;
            while (it != &(page_list.head_))
            {
                PageListHandle* temp = it;
                it = it->next_hash_;
                if (i != hash(temp->page_id_))
                {
                    // `temp` will not be destructed in that we don't leave this block,
                    // `lru_evict()` does not `unref()` until erase the handle in hash-table,
                    // thus we do not need to `ref()` here.
                    page_list.remove(temp);
                    new_page_list.append(temp);
                }
            }
        }
    } // end function `void Hash_LRU::rehash()`


    void Hash_LRU::lru_append(PageListHandle* handle)
    {
        handle->ref();
        handle->in_lru_ = true;
        PageListHandle* second = lru_head_.next_lru_;
        lru_head_.next_lru_ = handle; handle->prev_lru_ = &lru_head_;
        handle->next_lru_ = second; second->prev_lru_ = handle;
    }


    void Hash_LRU::lru_update(PageListHandle* handle)
    {
        PageListHandle* prev = handle->prev_lru_;
        PageListHandle* next = handle->next_lru_;

        if (prev == &lru_head_) return;

        prev->next_lru_ = next; next->prev_lru_ = prev;

        PageListHandle* second = lru_head_.next_lru_;

        lru_head_.next_lru_ = handle; handle->prev_lru_ = &lru_head_;
        handle->next_lru_ = second; second->prev_lru_ = handle;
    }


    void Hash_LRU::lru_remove(PageListHandle* handle)
    {
        PageListHandle* prev = handle->prev_lru_;
        PageListHandle* next = handle->next_lru_;
        prev->next_lru_ = next; next->prev_lru_ = prev;
        handle->in_lru_ = false;
        handle->unref();
    }


    void Hash_LRU::lru_evict()
    {
        /*
        page_id_t page_id;
        PageListHandle* victim;

        // if conflicts with `erase()`, no size decrement in `erase()`.
        size_--;

        {
            std::lock_guard<std::mutex> lru_lg(lru_mutex_);

            victim = lru_head_.prev_lru_;
            PageListHandle* last = victim->prev_lru_;

            lru_head_.prev_lru_ = last; last->next_lru_ = &lru_head_;

            page_id = victim->page_id_;

            victim->in_lru_ = false;
            // victim->unref(); // must not do `unref` here, but later.
        }

        {
            std::shared_lock<std::shared_mutex> lock(shared_mutex_);
            PageList& page_list = buckets_[hash(page_id)];

            std::lock_guard<std::mutex> page_lg(page_list.mutex_);

            if (victim->in_page_list_)
                page_list.remove(victim);

            victim->page_->set_dirty(); // in case that the victim has resided in memory.
                                        // block `FetchPage` until the victim has been flushed.

            victim->unref(); // `unref` for LRU
        }

        debug::DEBUG_LOG(debug::LRU_EVICT,
            "Hash_LRU::lru_evict() evict: [evict_id = %d]n", page_id);
        //*/

    } // end function `void Hash_LRU::lru_evict()`


} // end namespace DB::buffer