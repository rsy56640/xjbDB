#ifndef _DISK_MANAGER_H
#define _DISK_MANAGER_H
#include "page.h"
#include "env.h"
#include <fstream>
#include <string>
#include <atomic>
#include <shared_mutex>
#include <unordered_set>
#include <condition_variable>

namespace DB::disk
{
    using page::page_id_t;
    using page::PAGE_SIZE;

    static const char* db_name = "db.xjbDB";
    static const char* log_name = "db.xjbDB.log";

    enum log_state_t { OK, UNDO, REDO };

    /*
     * record any DB file, and record LOG
     */
    class DiskManager
    {
    private:
        /*
        struct WaitInfo {
            std::condition_variable cv_;
            std::atomic_flag flag_;
            std::atomic<uint32_t> wait_count_;
            WaitInfo() :wait_count_(1) {}
        };
        */
    public:
        static constexpr uint32_t dirty_hash_bucket = 1 << 4;

        DiskManager();
        ~DiskManager();

        page_id_t get_cut_page_id() const;

        void set_cur_page_id(page_id_t);

        // no validation on `page_id`
        // the length of `page_data` should not surpass `PAGE_SIZE`
        void WritePage(page_id_t page_id, const char(&page_data)[page::PAGE_SIZE]);

        // Concurrency: maybe wait until the page has been flushed.
        // no validation on `page_id`
        // the length of `page_data` should be enough to hold `PAGE_SIZE`
        // return true on read success,
        //        false when the page maybe has been read by other threads.
        bool ReadPage(page_id_t page_id, char(&page_data)[page::PAGE_SIZE]);

        void WriteLog(char *log_data, uint32_t size);

        // return true if read successfully
        bool ReadLog(char *log_data, uint32_t offset, uint32_t size);

        log_state_t check_log();

        page_id_t AllocatePage();

        uint32_t log_size() const;

        uint32_t hash(page_id_t page_id) const noexcept;

        bool is_dirty(page_id_t page_id) const;

        void set_dirty(page_id_t page_id);


        DiskManager(const DiskManager&) = delete;
        DiskManager(DiskManager&&) = delete;
        DiskManager& operator=(const DiskManager&) = delete;
        DiskManager& operator=(DiskManager&&) = delete;

        bool dn_init_;

    private:

        std::atomic<page_id_t> cur_page_no_; // the last used page_id

        std::unordered_set<page_id_t> dirty_page_sets_[dirty_hash_bucket];
        mutable std::shared_mutex dirty_page_sets_mtx_[dirty_hash_bucket];
        //std::unordered_map<page_id_t, WaitInfo*> wait_sets_[dirty_hash_bucket];
        //mutable std::mutex wait_sets_mtx_[dirty_hash_bucket];

        // stream to write db file
        const std::string file_name_;
        std::fstream db_io_;

        // stream to write log file
        const std::string log_name_;
        std::fstream log_io_;
        uint32_t log_size_;

    }; // end class DiskManager

} // end namespace DB::disk

#endif // !_DISK_MANAGER_H