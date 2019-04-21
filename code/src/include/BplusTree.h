#ifndef _BPLUSTREE_H
#define _BPLUSTREE_H
#include "page.h"
#include <atomic>
#include <shared_mutex>

namespace DB::disk { class DiskManager; }
namespace DB::vm { class StorageEngine; }
namespace DB::buffer { class BufferPoolManager; }
namespace DB::tree
{
    using namespace ::DB::page;

    struct KVEntry {
        KeyEntry kEntry;
        ValueEntry vEntry; // tuple's max size is 67B 
    };

    // used for init B+Tree
    struct OpenTableInfo {
        bool isInit;
        page_id_t root_id; // only needed when (!init)
    };


    // return:
    // < 0, if KeyEntry < keys[index]
    // = 0, if KeyEntry = keys[index]
    // > 0, if KeyEntry > keys[index]
    int32_t key_compare(const KeyEntry&, const BTreePage*, uint32_t key_index);


    constexpr uint32_t MIN_KEY_SIZE = BTdegree - 1;         // 7
    constexpr uint32_t MAX_KEY_SIZE = BTNodeKeySize;        // 15
    constexpr uint32_t MIN_BRANCH_SIZE = BTdegree;          // 8
    constexpr uint32_t MAX_BRANCH_SIZE = BTNodeBranchSize;  // 16
    constexpr uint32_t MIN_LEAF_SIZE = MIN_KEY_SIZE;        // 7
    constexpr uint32_t MAX_LEAF_SIZE = MAX_KEY_SIZE;        // 15
    constexpr uint32_t KEY_MIDEIUM = MAX_KEY_SIZE >> 1;     // 7 in [0...6] [7] [8...14]

    static constexpr uint32_t
        INSERT_NOTHING = 0,
        INSERT_KV = 1,
        ERASE_NOTHING = 0,
        ERASE_KV = 1;


    // The usage of BTiterator: 
    // e.g. 
    //      WHERE 3 <= pk < 11
    //
    //  bt.range_query_begin_lock();
    //  BTit it = bt.range_query_from_left_begin(3, true);
    //  BTit end = bt.range_query_from_right_end(11, false);
    //  while(it != end) {
    //      // do something on it
    //      // ...
    //      ++it;
    //  }
    //  it.destroy();
    //  end.destroy();
    //  bt.range_query_end_unlock();
    //
    class BTit {
    public:
        BTit(buffer::BufferPoolManager*, BTreePage* leaf, uint32_t cur_index);
        void operator++();                  // programmer promise the state before ++ is valid.
        bool operator!=(const BTit&) const;
        void destroy();                     // destroy the iterator when not use
        KeyEntry getK() const;
        ValueEntry getV() const;
        void updateV(const ValueEntry&);
    private:
        BTreePage * leaf_;
        uint32_t cur_index_;
        buffer::BufferPoolManager* buffer_pool_;
    };


    // NB: in the B+Tree, the root page should be `ref()` in the whole execution,
    //     while other pages are fetched from buffer-pool whenever used.
    // Internal Page structure:
    //      [branch, key, ..., branch, key, branch]
    //      branch <= key < branch <= key < ... < branch <= key < branch
    //           ____ LeafPage____
    //          /                 \____   (no inheritence)
    // BTreePage                       \
    //          \____ InternalPage_____RootPage
    class BTree
    {
    public:
        using Key = uint32_t; // direct value for `INTEGER`, offset for `VARCHAR`
        using Value = char*;
        using base_ptr = BTreePage * ;
        using link_ptr = InternalPage * ;
        using leaf_ptr = LeafPage * ;
        using root_ptr = RootPage * ;

    public:
        BTree(OpenTableInfo, buffer::BufferPoolManager*, key_t_t, uint32_t str_len = 0);
        ~BTree();
        BTree(const BTree&) = delete;
        BTree(BTree&&) = delete;
        BTree& operator=(const BTree&) = delete;
        BTree& operator=(BTree&&) = delete;


        void debug() const;
        void debug_page(bool config, page_id_t) const;


        page_id_t get_root_id() const;

        uint32_t size() const;



        void range_query_begin_lock();

        BTit range_query_from_begin();
        BTit range_query_from_end();            // the `leaf == nullptr` means it's end.

        // `equal` means whether or not it can take `=`
        // handle [kEntry <= it] and [kEntry < it]
        BTit range_query_from_left_begin(const KeyEntry& kEntry, bool equal);

        // `equal` means whether or not it can take `=`
        // handle [it <= kEntry] and [it < kEntry]
        BTit range_query_from_right_end(const KeyEntry& kEntry, bool equal);

        void range_query_end_unlock();



        // return state: `OBSOLETE` denotes no such key exists.
        //               `INUSED` ensures the value cotent is 
        ValueEntry find(const KeyEntry&) const;


        // return: `INSERT_NOTHING`, the key exists, do nothing.
        //         `INSERT_KV`,      no such key exists, and insert k-v.
        uint32_t insert(const KVEntry&);


        // return: `ERASE_NOTHING`, no such key exists.
        //         `ERASE_KV`,      the key-value has been erased.
        // operation:
        //          I. root is ROOT_LEAF
        //              1. directly delete if exists.
        //          II. root is ROOT_INTERNAL
        //              1.  a. root.nEntry = 1, call it (the founded one) as child.
        //                     1) child is LEAF (remember to release root write-locks)
        //                          1. child.nEntry > MIN_KEY, directly delete. (return)
        //                          2. child.nEntry = MIN_KEY
        //                              1] other-child.nEntry > MIN_KEY
        //                                  if delete child.key successfully,
        //                                  steal 1 k-v from other-child.
        //                              2] other-child.nEntry = MIN_KEY
        //                                  *** This is the only case root -> ROOT_LEAF *** (height-1 = 1)
        //                                  change root to ROOT_LEAF.
        //                                  steal the 2 childs' k-v.
        //                                  delete 2 childs.
        //                     2) child is INTERNAL
        //                          1. child.nEntry > MIN_KEY
        //                              find K_index, recusively go down (from child).
        //                          2. child.nEntry = MIN_KEY
        //                              1] other-child.nEntry > MIN_KEY
        //                                  *steal* one key/branch (carefully set key/branch)
        //                              2] other-child.nEntry = MIN_KEY
        //                                  *merge* 2 childs into root. (height-1)
        //                  b. root.nEntry > 1
        //                     find K_index, recusively go down.
        uint32_t erase(const KeyEntry&);


    private:


        void BT_create(OpenTableInfo);


        // do nothing if BTree is empty.
        // return the min leaf.key that *** KeyEntry <= leaf.key ***
        struct SearchInfo {
            page_id_t leaf_id = NOT_A_PAGE;
            uint32_t key_index = 0;
        };
        void search(const KeyEntry&, SearchInfo&) const;
        void doSearch(base_ptr node, const KeyEntry&, SearchInfo&) const;


        // Page* has been `ref()` before return.
        // `unref()` the Page after use ! ! !
        BTreePage* fetch_node(page_id_t page_id) const;
        BTreePage* fetch_node(base_ptr node, uint32_t index) const;


        // Page* has been `ref()` before return.
        // `unref()` the Page after use ! ! !
        BTreePage* allocate_node(PageInitInfo) const;


        // the only way to increase the height.
        // called when root is full.
        // opeartion:
        //          I. root is LEAF
        //              1. create 2 LEAF, L and R
        //              2. move root.k-v[0..7] into L, root.k-v[8..14] into R
        //              3. change root to INTERNAL
        //              4. set root.k[0] = root.k[7], set branch, adjust the relation
        //          II. root is INTERNAL
        //              1. create 2 INTERNAL, L and R
        //              2. 1) move root.k[0..6] + root.br[0..7] into L
        //                 2) move root.k[8..14] + root.br[8..15] into R
        //                 3) update parent_id
        //              3. set root.k[0] = root.k[7], set branch, adjust the relation
        void split_root();


        // REQUIREMENT: caller should hold the write-lock of both `node` and `L`.
        // node is non-full parent, split node.branch[index].
        // invoke `split_internal()` or `split_leaf()` on top of the page_t_t of `L`.
        void split(link_ptr node, uint32_t split_index, base_ptr L) const;


        // node is non-full parent, split node.branch[index].
        // node should hold the write lock.
        // operation:
        //              1. create 1 INTERNAL R, call node.branch[index] as L
        //              2. move L.k[8..14] into R.k[0..6], L.br[8..15] into R.br[0..7], update parent_id.
        //              3. shift node.k/br to right
        //              4. move L.k[7] upto node.k[index], set node.br[index+1] = R
        void split_internal(link_ptr node, uint32_t index, link_ptr L) const;


        // node is non-full parent, split node.branch[index].
        // node should hold the write lock.
        // operation:
        //              1. create 1 LEAF R, call node.branch[index] as L
        //              2. move L.k-v[8..14] into R.k-v[0..6], adjust relation
        //              3. shift node.k/br to right
        //              4. set node.k[index] = L.k[7], set node.br[index+1] = R
        void split_leaf(link_ptr node, uint32_t split_index, leaf_ptr L) const;


        // REQUIREMENT: caller should hold the write-lock of node, callee unlock.
        //              node is non-full, non-root, insert if LEAF, else go down recursively.
        // return: `INSERT_NOTHING`, the key exists, do nothing.
        //         `INSERT_KV`,      no such key exists, and insert k-v.
        // operation:
        //          I:  node is LEAF
        //              1. find index such that kEntry <= node.k[index]
        //              2. if equal, return `INSERT_NOTHING`. (note to validate index)
        //              3. move the index to right, then insert(++BT.size), release write-lock.
        //          II:  node is INTERNAL
        //              1. find index such that kEntry <= node.k[index]
        //              2. hold write-lock of node.br[index]
        //                 if need to split node.br[index], then split.
        //              3. release write-lock of node, then hold the read-lock of node.
        //              4. maybe update index and child=node.br[index] after split.
        //              5. recursively go down then release read-lock.
        uint32_t INSERT_NONFULL(base_ptr node, const KVEntry&);


        // return the maximum KeyEntry in the tree from page_id.
        KeyEntry max_KeyEntry(page_id_t);


        // all write-lock held, L.nEntry = R.nEtnry = KEY_MIN_SIZE
        // move R into L.
        //     move R.key[0..6] -> L.key[8..14], R.br[0..7] -> L.br[..15]
        //     move node.key[index] -> L.key[7]
        //     adjust node.key and node.branch
        void merge_internal(link_ptr node, uint32_t merge_index, link_ptr L, link_ptr R);


        // all write-lock held, L.nEntry = R.nEtnry = KEY_MIN_SIZE
        // move R into L, erase node.k[index], node.br[index+1]
        //     move R.k-v[0..6] -> L.k-v[7.13], adjust relation
        //     erase node.k[index], node.br[index+1]
        //     adjust node.key and node.branch
        void merge_leaf(link_ptr node, uint32_t merge_index, leaf_ptr L, leaf_ptr R);


        // REQUIREMENT:
        //              1. caller hold the write-lock of leaf.
        uint32_t erase_from_leaf(leaf_ptr, const KeyEntry&);


        // Arg:
        //              1. child = node.branch[index], ***index maybe the last branch***.
        // REQUIREMENT: 
        //              1. when call this function, the root must be ROOT_INTERNAL.
        //              2. if node is INTERNAL, node.nEntry >= MIN_KEY+1
        //              3. if node is root, root.nEntry >= 2
        //              4. caller hold write-lock of node, callee should unlock. (UNDONE: read-lock??)
        // return: `ERASE_NOTHING`,     the key does not exist, do nothing.
        //         `ERASE_KV`,          the key exist and k-v has been deleted.
        // operation:
        //          I. child is LEAF
        //              1.  a. child.nEntry > MIN_KEY
        //                     directly deleted if key exists. (return)
        //                  b. child.nEntry = MIN_KEY
        //                     1) left(right)-leaf.nEntry > MIN_KEY
        //                          *steal* 1 k-v from that leaf.
        //                          adjust node.key, child, leaf.
        //                          directly deleted if key exists. (return)
        //                     2) left(right)-leaf.nEntry = MIN_KEY
        //                          *merge* 2 leaf. (NB: manage ValuePage lifetime, left-right)
        //                          delete noed.key[index], adjust node.branch, node.nEntry--
        //                          directly deleted if key exists. (return)
        //          II. child is INTERNAL
        //              1.  a. child.nEntry > MIN_KEY
        //                     find K_index, such that child.key[K_index] <= kEntry
        //                     recusively go down.
        //                  b. child.nEntry = MIN_KEY, call node.branch[index+1] (if has) as R.
        //                     1) R.nEntry > MIN_KEY
        //                          *steal* R.branch[0] to child.branch[8]
        //                          set node.key[index] = old-R.key[0], update parent.
        //                          set L.key[7] = old-node.key[index]
        //                          find K_index, recusively go down.
        //                     2) R.nEntry = MIN_KEY
        //                          *merge* child and R into child.
        //                              move R.key[0..6] -> child.key[8..14], R.br[0..7] -> L.br[8..15], update parent_id
        //                              move noed.key[index] -> child.key[7]
        //                              adjust node.key and node.branch
        //                          find K_index, recusively go down.
        //                  c. child.nEntry = MIN_KEY, call node.branch[index-1] as L.
        //                     1) L.nEntry > MIN_KEY
        //                          *steal* L.branch[L.nEntry] to child.branch[0]
        //                          set child.key[0] = node.key[index-1], update parent.
        //                          set node.key[index-1] = old-L.key[L.nEntry-1]
        //                          find K_index, recusively go down.
        //                     2) L.nEntry = MIN_KEY
        //                          *merge* L and child into child.
        //                          move child.key[0..6] -> L.key[8..14], child.br[0..7] -> L.br[8..15], update parent_id
        //                          move noed.key[index-1] -> L.key[7]
        //                          adjust node.key and node.branch
        //                          find K_index, recusively go down.
        uint32_t ERASE_NONMIN(link_ptr node, uint32_t index, base_ptr child, const KeyEntry&);


        // it > kEntry
        BTit find_first_greater_than(const KeyEntry& kEntry);
        // it >= kEntry
        BTit find_first_greater_than_or_equal_to(const KeyEntry& kEntry);


    private:
        buffer::BufferPoolManager* buffer_pool_;
        const key_t_t key_t_;
        const uint32_t str_len_;
        base_ptr root_;
        std::atomic<uint32_t> size_;
        mutable std::shared_mutex range_query_lock_; // hold write-lock when do range query,
                                                     // otherwise hole read-lock.
    }; // end class BTree


} // end namespace DB::tree

#endif // !_BPLUSTREE_H