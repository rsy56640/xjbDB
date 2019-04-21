#include "include/BplusTree.h"
#include "include/disk_manager.h"
#include "include/page.h"
#include "include/debug_log.h"
#include "include/buffer_pool.h"
#include <cstring>
#include <string>
#include <stack>

// UNDONE: read-write lock, does 1 operation holds just 2 locks at one time?
//                                        or holds a sequence of locks from root to bottom?

namespace DB::tree
{

    using namespace ::DB::page;


    BTit::BTit(buffer::BufferPoolManager* buffer_pool, BTreePage* leaf, uint32_t cur_index)
        :buffer_pool_(buffer_pool), leaf_(leaf), cur_index_(cur_index) {}

    void BTit::operator++()
    {
        if (leaf_ == nullptr)
            debug::ERROR_LOG("BTit out of range, incorrect usage.\n");

        if (++cur_index_ == leaf_->nEntry_) {
            cur_index_ = 0;
            if (leaf_->get_page_t() == page_t_t::ROOT_LEAF) {
                leaf_ = nullptr;
            }
            else {
                BTreePage* temp = leaf_;
                const page_id_t right_page_id = static_cast<LeafPage*>(leaf_)->next_page_id_;
                if (right_page_id != NOT_A_PAGE)
                    leaf_ = static_cast<LeafPage*>(buffer_pool_->FetchPage(right_page_id));
                else
                    leaf_ = nullptr;
                temp->unref();
            }
        }
    }

    bool BTit::operator!=(const BTit& other) const {
        return leaf_ != other.leaf_ || cur_index_ != other.cur_index_;
    }

    void BTit::destroy() {
        if (leaf_ != nullptr && leaf_->get_page_t() != page_t_t::ROOT_LEAF)
            leaf_->unref();
    }

    KeyEntry BTit::getK() const {
        return leaf_->read_key(cur_index_);
    }

    ValueEntry BTit::getV() const {
        ValueEntry vEntry;
        if (leaf_->get_page_t() == page_t_t::ROOT_LEAF)
            static_cast<RootPage*>(leaf_)->read_value(cur_index_, vEntry);
        else
            static_cast<LeafPage*>(leaf_)->read_value(cur_index_, vEntry);
        return vEntry;
    }

    void BTit::updateV(const ValueEntry& vEntry) {
        if (leaf_->get_page_t() == page_t_t::ROOT_LEAF) {
            static_cast<RootPage*>(leaf_)->erase_value(cur_index_);
            static_cast<RootPage*>(leaf_)->insert_value(cur_index_, vEntry);
        }
        else {
            static_cast<LeafPage*>(leaf_)->erase_value(cur_index_);
            static_cast<LeafPage*>(leaf_)->insert_value(cur_index_, vEntry);
        }
    }





    // root is always resides in memory.
    // we don't care whether the root is in buffer-pool.
    // No one else may access root Page by using root_id!!!
    BTree::BTree(OpenTableInfo openTableInfo, buffer::BufferPoolManager* buffer_pool,
        key_t_t key_t, uint32_t str_len)
        :buffer_pool_(buffer_pool),
        key_t_(key_t), str_len_(str_len), root_(nullptr), size_(0)
    {
        BT_create(openTableInfo);
    }

    BTree::~BTree() {
        root_->unref();
    }

    page_id_t BTree::get_root_id() const { return root_->page_id_; }

    uint32_t BTree::size() const { return size_; }


    void BTree::range_query_begin_lock() {
        range_query_lock_.lock();
    }


    BTit BTree::range_query_from_begin()
    {
        if (root_->page_t_ == page_t_t::ROOT_LEAF) {
            if (root_->nEntry_ > 0)
                return BTit{ buffer_pool_, root_, 0 };
            else
                return BTit{ buffer_pool_, nullptr, 0 };
        }
        else {
            base_ptr node = root_;
            std::stack<base_ptr> stk;
            while (node->page_t_ != page_t_t::LEAF) {
                node = fetch_node(static_cast<link_ptr>(node)->branch_[0]);
                stk.push(node);
            }
            node->ref(); // `ref` the leaf
            while (!stk.empty()) {
                stk.top()->unref();
                stk.pop();
            }
            return BTit{ buffer_pool_, node, 0 };
        }
    }


    BTit BTree::range_query_from_end() {
        return BTit{ buffer_pool_, nullptr, 0 };
    }


    // `equal` means whether or not it can take `=`
    // handle [kEntry <= it] and [kEntry < it]
    BTit BTree::range_query_from_left_begin(const KeyEntry& kEntry, bool equal) {
        if (equal)
            return find_first_greater_than_or_equal_to(kEntry);
        else
            return find_first_greater_than(kEntry);
    }


    // `equal` means whether or not it can take `=`
    // handle [it <= kEntry] and [it < kEntry]
    BTit BTree::range_query_from_right_end(const KeyEntry& kEntry, bool equal) {
        if (equal)
            return find_first_greater_than(kEntry);
        else
            return find_first_greater_than_or_equal_to(kEntry);
    }


    // it > kEntry
    BTit BTree::find_first_greater_than(const KeyEntry& kEntry)
    {
        base_ptr node = root_;
        std::stack<base_ptr> stk;
        while (node->page_t_ != page_t_t::LEAF || node->page_t_ != page_t_t::ROOT_LEAF) {
            uint32_t index = 0;
            for (; index < node->nEntry_; index++)
                if (key_compare(kEntry, node, index) <= 0)
                    break;
            // internal.key[i] is the right-most key in internal.br[i]
            if (index < node->nEntry_ && key_compare(kEntry, node, index) == 0)
                node = fetch_node(node, index + 1);
            else
                node = fetch_node(node, index);
            stk.push(node);
        }
        if (node->page_t_ == page_t_t::LEAF)
            node->ref(); // `ref` the leaf
        while (!stk.empty()) {
            stk.top()->unref();
            stk.pop();
        }
        uint32_t index = 0;
        for (; index < node->nEntry_; index++)
            if (key_compare(kEntry, node, index) < 0) // not equal
                break;
        if (index == node->nEntry_)
            return BTit{ buffer_pool_, nullptr, 0 };
        else
            return BTit{ buffer_pool_, node, index };
    }


    // it >= kEntry
    BTit BTree::find_first_greater_than_or_equal_to(const KeyEntry& kEntry)
    {
        base_ptr node = root_;
        std::stack<base_ptr> stk;
        while (node->page_t_ != page_t_t::LEAF || node->page_t_ != page_t_t::ROOT_LEAF) {
            uint32_t index = 0;
            for (; index < node->nEntry_; index++)
                if (key_compare(kEntry, node, index) <= 0)
                    break;
            // internal.key[i] is the right-most key in internal.br[i]
            node = fetch_node(node, index);
            stk.push(node);
        }
        if (node->page_t_ == page_t_t::LEAF)
            node->ref(); // `ref` the leaf
        while (!stk.empty()) {
            stk.top()->unref();
            stk.pop();
        }
        uint32_t index = 0;
        for (; index < node->nEntry_; index++)
            if (key_compare(kEntry, node, index) <= 0)
                break;
        if (index == node->nEntry_)
            return BTit{ buffer_pool_, nullptr, 0 };
        else
            return BTit{ buffer_pool_, node, index };
    }


    void BTree::range_query_end_unlock() {
        range_query_lock_.unlock();
    }


    ValueEntry BTree::find(const KeyEntry& kEntry) const
    {
        std::shared_lock<std::shared_mutex> lg(range_query_lock_);

        ValueEntry vEntry;
        BTree::SearchInfo info;
        search(kEntry, info);
        if (info.leaf_id == NOT_A_PAGE)
            return vEntry;
        if (info.leaf_id != root_->get_page_id()) {
            leaf_ptr leaf = static_cast<leaf_ptr>(buffer_pool_->FetchPage(info.leaf_id));
            leaf->page_read_lock();
            leaf->read_value(info.key_index, vEntry);
            leaf->page_read_unlock();
            leaf->unref();
        }
        else {
            static_cast<root_ptr>(root_)->read_value(info.key_index, vEntry);
        }
        return vEntry;
    }


    // return: `INSERT_NOTHING`, the key exists, do nothing.
    //         `INSERT_KV`,      no such key exists, and insert k-v.
    uint32_t BTree::insert(const KVEntry& kvEntry)
    {
        std::shared_lock<std::shared_mutex> lg(range_query_lock_);

        root_->page_write_lock();

        // try to split full root
        if (root_->nEntry_ == MAX_KEY_SIZE)
            split_root();

        // find index such that kEntry <= node.k[index]
        uint32_t index = 0;
        for (; index < root_->nEntry_; index++)
            if (key_compare(kvEntry.kEntry, root_, index) <= 0)
                break;

        //
        // `ROOT_LEAF` insertion
        //
        if (root_->get_page_t() == page_t_t::ROOT_LEAF)
        {
            uint32_t insert_return;
            // if equal, return `INSERT_NOTHING`. (note to validate index)
            if (index != root_->nEntry_ && // kEntry > all root.keys, insert!!!
                key_compare(kvEntry.kEntry, root_, index) == 0)
                insert_return = INSERT_NOTHING;

            // move the index to right, then insert(++BT.size), release write-lock.
            else {
                size_++; // increase B+Tree size
                root_ptr leaf = static_cast<root_ptr>(root_);
                leaf->nEntry_++;
                // also valid when B+Tree is empty :)
                for (uint32_t i = leaf->nEntry_ - 1; i > index; i--) {
                    leaf->keys_[i] = leaf->keys_[i - 1];
                    leaf->values_[i] = leaf->values_[i - 1];
                }
                leaf->insert_key(index, kvEntry.kEntry);
                leaf->insert_value(index, kvEntry.vEntry);
                insert_return = INSERT_KV;
                leaf->set_dirty();
            }

            root_->page_write_unlock();
            return insert_return;

        } // end `ROOT_LEAF` insertion.


        //
        // root is INTERNAL
        //
        // hold write-lock of root.br[index]
        base_ptr node = fetch_node(static_cast<root_ptr>(root_)->branch_[index]);
        node->page_write_lock();

        // if need to split root.br[index], then split.
        if (node->nEntry_ == MAX_KEY_SIZE)
            split(static_cast<root_ptr>(root_), index, node);

        // release write-lock of root, then hold the read-lock of root.
        root_->page_write_unlock();
        root_->page_read_lock();

        //  maybe update index and node=root_.br[index] after split.
        if (index != root_->nEntry_ // if equal, no split happens.
            && key_compare(kvEntry.kEntry, root_, index) > 0) {
            node->page_write_unlock();
            node->unref();
            index++;
            node = fetch_node(static_cast<link_ptr>(root_)->branch_[index]);
            node->page_write_lock();
        }

        // recursively go down then release read-lock.
        uint32_t insert_return = INSERT_NONFULL(node, kvEntry);
        root_->page_read_unlock();

        node->unref();
        return insert_return;

    } // end function `BTree::insert(kv)`


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
    uint32_t BTree::erase(const KeyEntry& kEntry)
    {
        std::shared_lock<std::shared_mutex> lg(range_query_lock_);

        root_ptr root = static_cast<root_ptr>(root_);
        root->page_write_lock();


        // I.root is ROOT_LEAF
        if (root->get_page_t() == page_t_t::ROOT_LEAF)
        {
            debug::DEBUG_LOG(debug::ERASE_ROOT_LEAF,
                "erase ROOT_LEAF [root_id = %d]\n", root->get_page_id());

            // 1. directly delete if exists.
            uint32_t index = 0;
            for (; index < root->nEntry_; index++)
                if (key_compare(kEntry, root, index) == 0)
                    break;
            if (index == root->nEntry_) {
                root_->page_write_unlock();
                return ERASE_NOTHING;
            }
            else
            {
                size_--; // BT size
                root->erase_key(index);
                root->erase_value(index);
                root->nEntry_--;
                for (uint32_t i = index; i < root->nEntry_; i++)
                {
                    root->keys_[i] = root->keys_[i + 1];
                    root->values_[i] = root->values_[i + 1];
                }
                root->set_dirty();
                root_->page_write_unlock();
                return ERASE_KV;
            }

        } // end root is ROOT_LEAF


        // II.root is ROOT_INTERNAL
        else
        {
            debug::DEBUG_LOG(debug::ERASE_ROOT_INTERNAL,
                "erase ROOT_INTERNAL [root_id = %d]\n", root->get_page_id());

            // a. root.nEntry = 1, call it (the founded one) as child.
            if (root->nEntry_ == 1)
            {
                bool child_L = true;
                base_ptr child, other_child;
                if (key_compare(kEntry, root, 0) <= 0) {
                    child = fetch_node(root->branch_[0]);
                    other_child = fetch_node(root->branch_[1]);
                }
                else {
                    child_L = false;
                    other_child = fetch_node(root->branch_[0]);
                    child = fetch_node(root->branch_[1]);
                }
                child->page_write_lock();


                // 1) child is LEAF (remember to release root write-locks)
                if (child->get_page_t() == page_t_t::LEAF)
                {
                    debug::DEBUG_LOG(debug::ERASE_ROOT_INTERNAL,
                        "erase ROOT_INTERNAL, child is LEAF [root_id = %d], [child_id = %d]\n",
                        root->get_page_id(), child->get_page_id());

                    leaf_ptr child_leaf = static_cast<leaf_ptr>(child);
                    leaf_ptr other_child_leaf = static_cast<leaf_ptr>(other_child);

                    // 1. child.nEntry > MIN_KEY, directly delete. (return)
                    if (child_leaf->nEntry_ > MIN_KEY_SIZE)
                    {
                        uint32_t K_index = 0;
                        for (; K_index < child_leaf->nEntry_; K_index++)
                            if (key_compare(kEntry, child_leaf, K_index) == 0)
                                break;
                        if (K_index == child_leaf->nEntry_) {
                            child->page_write_unlock();
                            root_->page_write_unlock();
                            child->unref();
                            other_child->unref();
                            return ERASE_NOTHING;
                        }
                        else
                        {
                            size_--; // BT size
                            child_leaf->erase_key(K_index);
                            child_leaf->erase_value(K_index);
                            child_leaf->nEntry_--;
                            for (uint32_t i = K_index; i < child_leaf->nEntry_; i++)
                            {
                                child_leaf->keys_[i] = child_leaf->keys_[i + 1];
                                child_leaf->values_[i] = child_leaf->values_[i + 1];
                            }
                            child_leaf->set_dirty();

                            child->page_write_unlock();
                            root->page_write_unlock();
                            child->unref();
                            other_child->unref();

                            return ERASE_KV;
                        }
                    } // end child.nEntry > MIN_KEY, directly delete. (return)


                    // 2. child.nEntry = MIN_KEY
                    else
                    {
                        other_child->page_write_lock();

                        // 1] other-child.nEntry > MIN_KEY
                        if (other_child_leaf->nEntry_ > MIN_KEY_SIZE)
                        {
                            debug::DEBUG_LOG(debug::ERASE_ROOT_INTERNAL,
                                "erase ROOT_INTERNAL, child is LEAF [root_id = %d], [child_id = %d], [other_id = %d], other-child.nEntry > MIN_KEY\n",
                                root->get_page_id(), child->get_page_id(), other_child->get_page_id());

                            // if delete child.key successfully, steal 1 k-v from other-child.
                            uint32_t K_index = 0;
                            for (; K_index < child_leaf->nEntry_; K_index++)
                                if (key_compare(kEntry, child_leaf, K_index) == 0)
                                    break;
                            if (K_index == child_leaf->nEntry_) {
                                other_child->page_write_unlock();
                                child->page_write_unlock();
                                root_->page_write_unlock();
                                child->unref();
                                other_child->unref();
                                return ERASE_NOTHING;
                            }
                            else // successfully, steal 1 k-v from other-child.
                            {
                                size_--; // BT size
                                child_leaf->erase_key(K_index);
                                child_leaf->erase_value(K_index);
                                child_leaf->nEntry_--;

                                // steal from right.k-v[0]
                                if (child_L)
                                {
                                    debug::DEBUG_LOG(debug::ERASE_ROOT_INTERNAL,
                                        "erase ROOT_INTERNAL, child is LEAF [root_id = %d], [child_id = %d], [other_id = %d], other-child.nEntry > MIN_KEY\n child is left leaf, steal right.\n",
                                        root->get_page_id(), child->get_page_id(), other_child->get_page_id());

                                    // shift left
                                    for (uint32_t i = K_index; i < child_leaf->nEntry_; i++)
                                    {
                                        child_leaf->keys_[i] = child_leaf->keys_[i + 1];
                                        child_leaf->values_[i] = child_leaf->values_[i + 1];
                                    }

                                    // steal right.k-v[0]
                                    KeyEntry kEntry_steal = other_child_leaf->read_key(0);
                                    ValueEntry vEntry_steal;
                                    other_child_leaf->read_value(0, vEntry_steal);
                                    child_leaf->insert_key(child_leaf->nEntry_, kEntry_steal);
                                    child_leaf->insert_value(child_leaf->nEntry_, vEntry_steal);
                                    child_leaf->nEntry_++;

                                    // set root.key[0]
                                    root->erase_key(0);
                                    root->insert_key(0, kEntry_steal);

                                    // shift left
                                    other_child_leaf->erase_key(0);
                                    other_child_leaf->erase_value(0);
                                    other_child_leaf->nEntry_--;
                                    for (uint32_t i = 0; i < other_child_leaf->nEntry_; i++)
                                    {
                                        other_child_leaf->keys_[i] = other_child_leaf->keys_[i + 1];
                                        other_child_leaf->values_[i] = other_child_leaf->values_[i + 1];
                                    }

                                } // end steal from right.k-v[0]

                                // steal from left.k-v[nEntry-1]
                                else
                                {
                                    debug::DEBUG_LOG(debug::ERASE_ROOT_INTERNAL,
                                        "erase ROOT_INTERNAL, child is LEAF [root_id = %d], [child_id = %d], [other_id = %d], other-child.nEntry > MIN_KEY\n child is right leaf, steal left.\n",
                                        root->get_page_id(), child->get_page_id(), other_child->get_page_id());

                                    // shift right
                                    for (uint32_t i = K_index; i > 0; i--)
                                    {
                                        child_leaf->keys_[i] = child_leaf->keys_[i - 1];
                                        child_leaf->values_[i] = child_leaf->values_[i - 1];
                                    }

                                    // steal left.k-v[nEntry-1]
                                    const uint32_t L_nEntry = other_child_leaf->nEntry_;
                                    KeyEntry kEntry_steal = other_child_leaf->read_key(L_nEntry - 1);
                                    ValueEntry vEntry_steal;
                                    other_child_leaf->read_value(L_nEntry - 1, vEntry_steal);
                                    child_leaf->insert_key(0, kEntry_steal);
                                    child_leaf->insert_value(0, vEntry_steal);
                                    child_leaf->nEntry_++;

                                    // shift left
                                    other_child_leaf->erase_key(L_nEntry - 1);
                                    other_child_leaf->erase_value(L_nEntry - 1);
                                    other_child_leaf->nEntry_--;

                                    // set root.key[0]
                                    kEntry_steal = other_child->read_key(L_nEntry - 2);
                                    root->erase_key(0);
                                    root->insert_key(0, kEntry_steal);

                                } // end steal from left.k-v[nEntry-1]

                                other_child_leaf->set_dirty();
                                child_leaf->set_dirty();

                                other_child->page_write_unlock();
                                child->page_write_unlock();
                                root_->page_write_unlock();
                                child->unref();
                                other_child->unref();

                                return ERASE_KV;

                            } // end steal 1 k-v from other-child

                        } // end other-child.nEntry > MIN_KEY


                        // 2] other-child.nEntry = MIN_KEY
                        else
                        {
                            debug::DEBUG_LOG(debug::ERASE_ROOT_INTERNAL,
                                "erase ROOT_INTERNAL, child is LEAF [root_id = %d], [child_id = %d], [other_id = %d], other-child.nEntry = MIN_KEY\n merge 2 leaf into root -> ROOT_LEAF\n",
                                root->get_page_id(), child->get_page_id(), other_child->get_page_id());

                            // *** This is the only case root -> ROOT_LEAF *** (height-1 = 1)
                            // change root to ROOT_LEAF.
                            // steal the 2 childs' k-v.
                            // delete 2 childs.
                            leaf_ptr L, R;
                            if (child_L) {
                                L = child_leaf;
                                R = other_child_leaf;
                            }
                            else {
                                L = other_child_leaf;
                                R = child_leaf;
                            }


                            debug_page(debug::MERGE_LEAF, root->get_page_id());
                            debug_page(debug::MERGE_LEAF, L->get_page_id());
                            debug_page(debug::MERGE_LEAF, R->get_page_id());


                            root->page_t_ = page_t_t::ROOT_LEAF;

                            // delete root.k[0]
                            root->erase_key(0);

                            // L.k-v[0..6] to root.k-v[0..6], R.k-v[0..6] to root.k-v[7-13]
                            KeyEntry kEntry_steal;
                            ValueEntry vEntry_steal;
                            for (uint32_t index = 0; index < MIN_KEY_SIZE; index++)
                            {
                                kEntry_steal = L->read_key(index);
                                L->read_value(index, vEntry_steal);
                                root->insert_key(index, kEntry_steal);
                                root->insert_value(index, vEntry_steal);
                                L->erase_key(index);    // maybe ununsed
                                L->erase_value(index);  // maybe ununsed

                                kEntry_steal = R->read_key(index);
                                R->read_value(index, vEntry_steal);
                                root->insert_key(index + MIN_KEY_SIZE, kEntry_steal);
                                root->insert_value(index + MIN_KEY_SIZE, vEntry_steal);
                                R->erase_key(index);    // maybe ununsed
                                R->erase_value(index);  // maybe ununsed
                            }
                            root->nEntry_ = MIN_KEY_SIZE << 1;

                            other_child->page_write_unlock();
                            child->page_write_unlock();
                            child->unref();
                            other_child->unref();

                            debug_page(debug::MERGE_LEAF, root->get_page_id());

                            // directly delete
                            uint32_t K_index = 0;
                            for (; K_index < root->nEntry_; K_index++)
                                if (key_compare(kEntry, root, K_index) == 0)
                                    break;
                            if (K_index == root->nEntry_) {
                                root_->page_write_unlock();
                                return ERASE_NOTHING;
                            }
                            else
                            {
                                size_--; // BT size
                                root->erase_key(K_index);
                                root->erase_value(K_index);
                                root->nEntry_--;
                                for (uint32_t i = K_index; i < root->nEntry_; i++)
                                {
                                    root->keys_[i] = root->keys_[i + 1];
                                    root->values_[i] = root->values_[i + 1];
                                }

                                root_->page_write_unlock();

                                return ERASE_KV;
                            }

                        } // end other-child.nEntry = MIN_KEY (aka. merge to ROOT_LEAF)


                    } // end child.nEntry = MIN_KEY


                } // end child is LEAF (remember to release root write-locks)


                // 2) child is INTERNAL
                else
                {
                    debug::DEBUG_LOG(debug::ERASE_ROOT_INTERNAL,
                        "erase ROOT_INTERNAL, child is INTERNAL [root_id = %d], [child_id = %d]\n",
                        root->get_page_id(), child->get_page_id());

                    link_ptr child_link = static_cast<link_ptr>(child);
                    link_ptr other_child_link = static_cast<link_ptr>(other_child);

                    // 1. child.nEntry > MIN_KEY
                    if (child_link->nEntry_ > MIN_KEY_SIZE)
                    {
                        debug::DEBUG_LOG(debug::ERASE_ROOT_INTERNAL,
                            "erase ROOT_INTERNAL, child is INTERNAL [root_id = %d], [child_id = %d], child.nEntry > MIN_KEY\n",
                            root->get_page_id(), child->get_page_id());

                        other_child->unref();

                        // find K_index, recusively go down (from child).
                        root->page_write_unlock();
                        root->page_read_lock();

                        uint32_t K_index = 0;
                        for (; K_index < child_link->nEntry_; K_index++)
                            if (key_compare(kEntry, child_link, K_index) <= 0)
                                break;
                        base_ptr child_child = fetch_node(child_link->branch_[K_index]);
                        uint32_t erase_return = ERASE_NONMIN(child_link, K_index, child_child, kEntry);
                        child_child->unref();

                        child_link->unref(); // here special, since no caller

                        root->page_read_unlock();

                        return erase_return;

                    } // end child.nEntry > MIN_KEY


                    // 2. child.nEntry = MIN_KEY
                    else
                    {
                        debug::DEBUG_LOG(debug::ERASE_ROOT_INTERNAL,
                            "erase ROOT_INTERNAL, child is INTERNAL [root_id = %d], [child_id = %d], [other_id = %d], child.nEntry = MIN_KEY\n",
                            root->get_page_id(), child->get_page_id(), other_child->get_page_id());

                        other_child_link->page_write_lock();

                        // 1] other-child.nEntry > MIN_KEY
                        if (other_child_link->nEntry_ > MIN_KEY_SIZE)
                        {
                            debug::DEBUG_LOG(debug::ERASE_ROOT_INTERNAL,
                                "erase ROOT_INTERNAL, child is INTERNAL [root_id = %d], [child_id = %d], [other_id = %d], child.nEntry = MIN_KEY, other-child.nEntry > MIN_KEY, steal\n",
                                root->get_page_id(), child->get_page_id(), other_child->get_page_id());

                            // *steal* one key/branch (carefully set key/branch)

                            // child is left, steal right.k-br[0]
                            if (child_L)
                            {
                                debug::DEBUG_LOG(debug::ERASE_ROOT_INTERNAL,
                                    "steal from left\n");

                                link_ptr L = child_link, R = other_child_link;

                                KeyEntry kEntry_steal_R = R->read_key(0);
                                L->nEntry_++;
                                L->branch_[MIN_KEY_SIZE + 1] = R->branch_[0];

                                // shift left
                                R->erase_key(0);
                                R->nEntry_--;
                                for (uint32_t i = 0; i < R->nEntry_; i++)
                                {
                                    R->keys_[i] = R->keys_[i + 1];
                                    R->branch_[i] = R->branch_[i + 1];
                                }
                                R->branch_[R->nEntry_] = R->branch_[R->nEntry_ + 1];

                                // set root.k[0]
                                KeyEntry kEntry_steal_root = root->read_key(0);
                                root->erase_key(0);
                                root->insert_key(0, kEntry_steal_R);
                                root->set_dirty();

                                // set L.key[7]
                                L->insert_key(MIN_KEY_SIZE, kEntry_steal_root);
                                L->set_dirty();

                                // update parent
                                base_ptr adopted = fetch_node(L->branch_[L->nEntry_]);
                                adopted->page_write_lock();
                                adopted->set_parent_id(L->get_page_id());
                                adopted->page_write_unlock();
                                adopted->unref();

                            } // end child is left, steal right.k-br[0]

                            // child is right, steal left.k[nEntry-1], br[nEntry]
                            else
                            {
                                debug::DEBUG_LOG(debug::ERASE_ROOT_INTERNAL,
                                    "steal from right\n");

                                link_ptr L = other_child_link, R = child_link;

                                // shift right
                                R->nEntry_++;
                                R->branch_[R->nEntry_] = R->branch_[R->nEntry_ - 1];
                                for (uint32_t i = R->nEntry_ - 1; i > 0; i--)
                                {
                                    R->keys_[i] = R->keys_[i - 1];
                                    R->branch_[i] = R->branch_[i - 1];
                                }

                                // steal left.k[nEntry-1], br[nEntry]
                                KeyEntry kEntry_steal_L = L->read_key(L->nEntry_ - 1);
                                KeyEntry kEntry_steal_ROOT = root->read_key(0);
                                R->insert_key(0, kEntry_steal_ROOT);
                                R->branch_[0] = L->branch_[L->nEntry_];
                                L->erase_key(L->nEntry_ - 1);
                                L->nEntry_--;

                                L->set_dirty();
                                R->set_dirty();

                                // set root.k[0]
                                root->erase_key(0);
                                root->insert_key(0, kEntry_steal_L);
                                root->set_dirty();

                                // update parent
                                base_ptr adopted = fetch_node(R->branch_[0]);
                                adopted->page_write_lock();
                                adopted->set_parent_id(R->get_page_id());
                                adopted->page_write_unlock();
                                adopted->unref();

                            } // end child is right, steal left.k[nEntry-1], br[nEntry]

                            // now child is NONMIN

                            other_child->page_write_unlock();
                            other_child->unref();

                            root->page_write_unlock();
                            root->page_read_lock();

                            // find K_index, recusively go down.
                            uint32_t K_index = 0;
                            for (; K_index < child_link->nEntry_; K_index++)
                                if (key_compare(kEntry, child_link, K_index) <= 0)
                                    break;
                            // hold child write-lock
                            base_ptr child_child = fetch_node(child_link->branch_[K_index]);
                            uint32_t erase_return = ERASE_NONMIN(child_link, K_index, child_child, kEntry);
                            child_child->unref();

                            root->page_read_unlock();

                            child->unref(); // here special, since no caller

                            return erase_return;

                        } // end other-child.nEntry > MIN_KEY


                        // 2] other-child.nEntry = MIN_KEY
                        else
                        {
                            debug::DEBUG_LOG(debug::ERASE_ROOT_INTERNAL,
                                "erase ROOT_INTERNAL, child is INTERNAL [root_id = %d], [child_id = %d], [other_id = %d], child.nEntry = MIN_KEY, other-child.nEntry = MIN_KEY\n merge 2 childs into root, heigh-1\n",
                                root->get_page_id(), child->get_page_id(), other_child->get_page_id());

                            // *merge* 2 childs into root. (height-1)
                            link_ptr L, R;
                            if (child_L) {
                                L = child_link;
                                R = other_child_link;
                            }
                            else {
                                L = other_child_link;
                                R = child_link;
                            }

                            // move root.k[0] to root.k[7]
                            root_->keys_[KEY_MIDEIUM] = root->keys_[0];

                            // move L.k[0..6] into root.k[0..6], L.br[0..7] into root.br[0..7]
                            KeyEntry keyEntry_merge;
                            for (uint32_t i = 0; i < KEY_MIDEIUM; i++)
                            {
                                keyEntry_merge = L->read_key(i);
                                root->insert_key(i, keyEntry_merge);
                                L->erase_key(i);
                                root->branch_[i] = L->branch_[i];
                            }
                            root->branch_[KEY_MIDEIUM] = L->branch_[KEY_MIDEIUM];

                            // move R.k[0..6] into root.k[8..14], R.br[0..7] into root.br[8..15]
                            for (uint32_t i = 0; i < KEY_MIDEIUM; i++)
                            {
                                keyEntry_merge = R->read_key(i);
                                root->insert_key(i + KEY_MIDEIUM + 1, keyEntry_merge);
                                R->erase_key(i);
                                root->branch_[i + KEY_MIDEIUM + 1] = R->branch_[i];
                            }
                            root->branch_[MAX_KEY_SIZE] = R->branch_[KEY_MIDEIUM];

                            root->nEntry_ = MAX_KEY_SIZE;

                            // update parent
                            for (uint32_t i = 0; i <= MAX_KEY_SIZE; i++) // br[0..15]
                            {
                                base_ptr adopted = fetch_node(root->branch_[i]);
                                adopted->page_write_lock();
                                adopted->set_parent_id(root->get_page_id());
                                adopted->page_write_unlock();
                                adopted->unref();
                            }

                            child->page_write_unlock();
                            other_child->page_write_unlock();
                            child->unref();
                            other_child->unref();

                            // find K_index, recusively go down.
                            uint32_t K_index = 0;
                            for (; K_index < root->nEntry_; K_index++)
                                if (key_compare(kEntry, root, K_index) <= 0)
                                    break;
                            base_ptr root_child = fetch_node(root->branch_[K_index]);
                            // hold root write-lock
                            uint32_t erase_return = ERASE_NONMIN(root, K_index, root_child, kEntry);
                            root_child->unref();

                            return erase_return;

                        } // end other-child.nEntry = MIN_KEY


                    } // end child.nEntry = MIN_KEY


                } // end child is INTERNAL


            } // end root.nEntry = 1, call it (the founded one) as child.

            // b. root.nEntry > 1
            else
            {
                debug::DEBUG_LOG(debug::ERASE_ROOT_INTERNAL,
                    "erase ROOT_INTERNAL [root_id = %d], root.nEntry > 1\n", root->get_page_id());

                // find K_index, recusively go down.
                uint32_t K_index = 0;
                for (; K_index < root->nEntry_; K_index++)
                    if (key_compare(kEntry, root, K_index) <= 0)
                        break;
                base_ptr root_child = fetch_node(root->branch_[K_index]);
                // hold root write-lock
                uint32_t erase_return = ERASE_NONMIN(root, K_index, root_child, kEntry);
                root_child->unref();

                return erase_return;
            }

        } // end root is ROOT_INTERNAL



    } // end function `BTree::erase(k)`


    /////////////////////////////// private implementation ///////////////////////////////


    // `ROOT_LEAF` when tree is initilized at the first time
    void BTree::BT_create(OpenTableInfo openTableInfo) {
        if (openTableInfo.isInit)
        {
            PageInitInfo info;
            info.page_t = page_t_t::ROOT_LEAF;
            info.parent_id = NOT_A_PAGE;
            info.key_t = key_t_;
            info.str_len = str_len_;
            info.value_page_id = NOT_A_PAGE;
            root_ = allocate_node(info);
            root_->set_dirty();
            debug::DEBUG_LOG(debug::BT_CREATE, "init root_id = %d\n", root_->get_page_id());
        }
        else
        {
            root_ = fetch_node(openTableInfo.root_id);
        }
        // root has been `ref()`.
    }


    // do nothing if BTree is empty.
    void BTree::search(const KeyEntry& kEntry, SearchInfo& info) const
    {
        info.leaf_id = NOT_A_PAGE;
        if (size_ == 0) return;
        root_->page_read_lock();
        doSearch(root_, kEntry, info);
        root_->page_read_unlock();
    }


    // return the min leaf.key that *** KeyEntry <= leaf.key ***
    void BTree::doSearch(base_ptr node, const KeyEntry& kEntry, SearchInfo& info) const
    {
        uint32_t index = 0;
        for (; index < node->nEntry_; index++)
            if (key_compare(kEntry, node, index) <= 0)
                break;

        // now (kEntry <= key[index]) or (index == n)
        if (index == node->nEntry_ &&
            (node->get_page_t() == page_t_t::LEAF || node->get_page_t() == page_t_t::ROOT_LEAF))
            return; // all key < kEntry, so no result.

        // if at Leaf
        if (node->get_page_t() == page_t_t::LEAF
            || node->get_page_t() == page_t_t::ROOT_LEAF)
        {
            if (key_compare(kEntry, node, index) == 0) {
                info.leaf_id = node->get_page_id();
                info.key_index = index;
            }
            return;
        }

        // root is Internal and (kEntry <= key[index])
        // search recursively into branch[index]
        base_ptr child = fetch_node(node, index);
        child->page_read_lock();
        doSearch(child, kEntry, info);
        child->page_read_unlock();
        child->unref();
    }


    BTreePage* BTree::fetch_node(page_id_t page_id) const {
        return static_cast<base_ptr>(buffer_pool_->FetchPage(page_id));
    }


    BTreePage* BTree::fetch_node(base_ptr node, uint32_t index) const {
        return static_cast<base_ptr>(buffer_pool_
            ->FetchPage(static_cast<link_ptr>(node)->branch_[index]));
    }


    BTreePage* BTree::allocate_node(PageInitInfo info) const {
        BTreePage* page_ptr =
            static_cast<base_ptr>(buffer_pool_->NewPage(info));
        return page_ptr;
    }


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
    void BTree::split_root()
    {
        // ROOT is full LEAF
        if (root_->page_t_ == page_t_t::ROOT_LEAF)
        {
            // step 1: create 2 LEAF, L and R
            PageInitInfo info;
            info.page_t = page_t_t::LEAF;
            info.parent_id = root_->get_page_id();
            info.key_t = key_t_;
            info.str_len = str_len_;
            info.value_page_id = NOT_A_PAGE;
            info.previous_page_id = NOT_A_PAGE;
            info.next_page_id = NOT_A_PAGE;
            leaf_ptr L = static_cast<leaf_ptr>(allocate_node(info));
            leaf_ptr R = static_cast<leaf_ptr>(allocate_node(info));

            debug::DEBUG_LOG(debug::SPLIT_ROOT_LEAF,
                "split_root_leaf: [L = %d], [R = %d]\n", L->get_page_id(), R->get_page_id());

            // step 2: move root.k - v[0..7] into L, root.k - v[8..14] into R
            KeyEntry kEntry;
            ValueEntry vEntry;
            for (uint32_t index = 0; index <= KEY_MIDEIUM; index++) // [0..7]
            {
                kEntry = root_->read_key(index);
                static_cast<root_ptr>(root_)->read_value(index, vEntry);
                L->insert_key(index, kEntry);
                L->insert_value(index, vEntry);
                if (index != KEY_MIDEIUM) root_->erase_key(index); // Do not erase key[7]
                static_cast<root_ptr>(root_)->erase_value(index);
            }
            L->nEntry_ = KEY_MIDEIUM + 1;
            L->set_dirty();

            for (uint32_t index = 0; index < KEY_MIDEIUM; index++) // [0..6]
            {
                const uint32_t root_index = index + KEY_MIDEIUM + 1; // [8..14]
                kEntry = root_->read_key(root_index);
                static_cast<root_ptr>(root_)->read_value(root_index, vEntry);
                R->insert_key(index, kEntry);
                R->insert_value(index, vEntry);
                root_->erase_key(root_index);
                static_cast<root_ptr>(root_)->erase_value(root_index);
            }
            R->nEntry_ = KEY_MIDEIUM;
            R->set_dirty();

            L->set_right_leaf(R->get_page_id());
            R->set_left_leaf(L->get_page_id());

            // step 3: change root to INTERNAL
            root_->page_t_ = page_t_t::ROOT_INTERNAL;

            // step 4: set root.k[0] = root.k[7], set branch, adjust the relation
            root_->nEntry_ = 1;
            root_->keys_[0] = root_->keys_[KEY_MIDEIUM];
            static_cast<root_ptr>(root_)->branch_[0] = L->get_page_id();
            static_cast<root_ptr>(root_)->branch_[1] = R->get_page_id();
            root_->set_dirty();

            L->unref();
            R->unref();

        } // end root is LEAF

        // ROOT is full INTERNAL
        else
        {
            // step 1: create L and R
            PageInitInfo info;
            info.page_t = page_t_t::INTERNAL;
            info.parent_id = root_->get_page_id();
            info.key_t = key_t_;
            info.str_len = str_len_;
            link_ptr L = static_cast<link_ptr>(allocate_node(info));
            link_ptr R = static_cast<link_ptr>(allocate_node(info));

            debug::DEBUG_LOG(debug::SPLIT_ROOT_INTERNAL,
                "split_root_internal: [L = %d], [R = %d]\n", L->get_page_id(), R->get_page_id());

            //step 2: 1) move root.k[0..6] + root.br[0..7] into L
            //        2) move root.k[8..14] + root.br[8..15] into R
            //        3) update parent_id
            KeyEntry kEntry;

            for (uint32_t index = 0; index < KEY_MIDEIUM; index++) // [0..6]
            {
                kEntry = root_->read_key(index);
                root_->erase_key(index);
                L->insert_key(index, kEntry);
                L->branch_[index] = static_cast<root_ptr>(root_)->branch_[index];
            }
            L->branch_[KEY_MIDEIUM] = static_cast<root_ptr>(root_)->branch_[KEY_MIDEIUM];
            L->nEntry_ = KEY_MIDEIUM;
            L->set_dirty();

            for (uint32_t index = 0; index < KEY_MIDEIUM; index++) // [0..6]
            {
                const uint32_t root_index = index + KEY_MIDEIUM + 1; // [8..14]
                kEntry = root_->read_key(root_index);
                root_->erase_key(root_index);
                R->insert_key(index, kEntry);
                R->branch_[index] = static_cast<root_ptr>(root_)->branch_[root_index];
            }
            R->branch_[KEY_MIDEIUM] = static_cast<root_ptr>(root_)->branch_[KEY_MIDEIUM + KEY_MIDEIUM + 1];
            R->nEntry_ = KEY_MIDEIUM;
            R->set_dirty();
            // update parent_id
            for (uint32_t index = 0; index <= L->nEntry_; index++) {
                base_ptr child = fetch_node(L->branch_[index]);
                child->page_write_lock();
                child->set_parent_id(L->get_page_id());
                child->set_dirty();
                child->page_write_unlock();
                child->unref();
            }
            for (uint32_t index = 0; index <= R->nEntry_; index++) {
                base_ptr child = fetch_node(R->branch_[index]);
                child->page_write_lock();
                child->set_parent_id(R->get_page_id());
                child->set_dirty();
                child->page_write_unlock();
                child->unref();
            }


            // step 3: set root.k[0] = root.k[7], set branch, adjust the relation
            root_->nEntry_ = 1;
            root_->keys_[0] = root_->keys_[KEY_MIDEIUM]; // set the offset.
            static_cast<root_ptr>(root_)->branch_[0] = L->get_page_id();
            static_cast<root_ptr>(root_)->branch_[1] = R->get_page_id();
            root_->set_dirty();

            L->unref();
            R->unref();

        } // end root is INTERNAL

    } // end function split_root();


    // REQUIREMENT: caller should hold the write-lock of both `node` and `L`.
    // node is non-full parent, split node.branch[index].
    // invoke `split_internal()` or `split_leaf()` on top of the page_t_t of `L`.
    void BTree::split(link_ptr node, uint32_t split_index, base_ptr L) const
    {
        switch (L->get_page_t())
        {
        case page_t_t::INTERNAL:
            debug::DEBUG_LOG(debug::SPLIT_INTERNAL, "split_internal: [id = %d], [parent_id = %d], [index = %d]\n",
                L->get_page_id(), node->get_page_id(), split_index);
            split_internal(node, split_index, static_cast<link_ptr>(L));
            return;
        case page_t_t::LEAF:
            debug::DEBUG_LOG(debug::SPLIT_LEAF, "split_leaf: [id = %d], [parent_id = %d], [index = %d]\n",
                L->get_page_id(), node->get_page_id(), split_index);
            split_leaf(node, split_index, static_cast<leaf_ptr>(L));
            return;
        default:
            debug::ERROR_LOG("`BTree::split()` page_t invalid");
            return;
        }
    }


    // node is non-full parent, split node.branch[index].
    // operation:
    //              1. create 1 INTERNAL R, call node.branch[index] as L
    //              2. move L.k[8..14] into R.k[0..6], L.br[8..15] into R.br[0..7], update parent_id.
    //              3. shift node.k/br to right
    //              4. move L.k[7] upto node.k[index], set node.br[index+1] = R
    void BTree::split_internal(link_ptr node, uint32_t split_index, link_ptr L) const
    {
        // step 1: create 1 INTERNAL R, call node.branch[index] as L
        PageInitInfo info;
        info.page_t = page_t_t::INTERNAL;
        info.parent_id = node->get_page_id();
        info.key_t = key_t_;
        info.str_len = str_len_;
        link_ptr R = static_cast<link_ptr>(allocate_node(info));


        // step 2: move L.k[8..14] into R.k[0..6], L.br[8..15] into R.br[0..7], update parent_id.
        KeyEntry kEntry;
        ValueEntry vEntry;
        for (uint32_t R_index = 0; R_index < KEY_MIDEIUM; R_index++) // [0..6]
        {
            const uint32_t L_index = R_index + KEY_MIDEIUM + 1; // [8..14]
            kEntry = L->read_key(L_index);
            R->insert_key(R_index, kEntry);
            L->erase_key(L_index);
            R->branch_[R_index] = L->branch_[L_index];
        }
        R->branch_[KEY_MIDEIUM] = L->branch_[MAX_BRANCH_SIZE - 1];
        L->nEntry_ = KEY_MIDEIUM;
        R->nEntry_ = KEY_MIDEIUM;
        L->set_dirty();
        R->set_dirty();
        // update parent_id
        for (uint32_t index = 0; index <= R->nEntry_; index++) {
            base_ptr child = fetch_node(R->branch_[index]);
            child->page_write_lock();
            child->set_parent_id(R->get_page_id());
            child->set_dirty();
            child->page_write_unlock();
            child->unref();
        }


        // step 3: shift node.k/br to right
        node->nEntry_++;
        for (uint32_t index = node->nEntry_ - 1; index > split_index; index--)
        {
            node->keys_[index] = node->keys_[index - 1];
            node->branch_[index + 1] = node->branch_[index];
        }


        // step 4: move L.k[7] upto node.k[index], set node.br[index+1] = R
        kEntry = L->read_key(KEY_MIDEIUM);
        node->insert_key(split_index, kEntry);
        L->erase_key(KEY_MIDEIUM);
        node->branch_[split_index + 1] = R->get_page_id();
        node->set_dirty();

        R->unref();

    }


    // node is non-full parent, split node.branch[index].
    // operation:
    //              1. create 1 LEAF R, call node.branch[index] as L
    //              2. move L.k-v[8..14] into R.k-v[0..6], adjust relation
    //              3. shift node.k/br to right
    //              4. set node.k[index] = L.k[7], set node.br[index+1] = R
    void BTree::split_leaf(link_ptr node, uint32_t split_index, leaf_ptr L) const
    {
        // step 1: create 1 LEAF R, call node.branch[index] as L
        PageInitInfo info;
        info.page_t = page_t_t::LEAF;
        info.parent_id = node->get_page_id();
        info.key_t = key_t_;
        info.str_len = str_len_;
        info.value_page_id = NOT_A_PAGE;
        info.previous_page_id = NOT_A_PAGE;
        info.next_page_id = NOT_A_PAGE;
        leaf_ptr R = static_cast<leaf_ptr>(allocate_node(info));

        // step 2: move L.k-v[8..14] into R.k-v[0..6], adjust relation
        KeyEntry kEntry;
        ValueEntry vEntry;
        for (uint32_t R_index = 0; R_index < KEY_MIDEIUM; R_index++) // [0..6]
        {
            const uint32_t L_index = R_index + KEY_MIDEIUM + 1; // [8..14]
            kEntry = L->read_key(L_index);
            L->read_value(L_index, vEntry);
            R->insert_key(R_index, kEntry);
            R->insert_value(R_index, vEntry);
            L->erase_key(L_index);
            L->erase_value(L_index);
        }
        L->nEntry_ = KEY_MIDEIUM + 1;
        R->nEntry_ = KEY_MIDEIUM;
        const page_id_t next = L->get_right_leaf();
        L->set_right_leaf(R->get_page_id());
        R->set_left_leaf(L->get_page_id());
        if (next != NOT_A_PAGE) {
            leaf_ptr next_leaf = static_cast<leaf_ptr>(fetch_node(next));
            next_leaf->page_write_lock();
            next_leaf->set_left_leaf(R->get_page_id());
            next_leaf->page_write_unlock();
            next_leaf->unref();
        }
        R->set_right_leaf(next);
        L->set_dirty();
        R->set_dirty();

        // step 3: shift node.k/br to right
        node->nEntry_++;
        for (uint32_t index = node->nEntry_ - 1; index > split_index; index--)
        {
            node->keys_[index] = node->keys_[index - 1];
            node->branch_[index + 1] = node->branch_[index];
        }

        // step 4: set node.k[index] = L.k[7], set node.br[index+1] = R
        kEntry = L->read_key(KEY_MIDEIUM);
        node->insert_key(split_index, kEntry);
        node->branch_[split_index + 1] = R->get_page_id();
        node->set_dirty();

        R->unref();

    }


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
    uint32_t BTree::INSERT_NONFULL(base_ptr node, const KVEntry& kvEntry)
    {
        // step 1: find index such that kEntry <= node.k[index]
        uint32_t index = 0;
        for (; index < node->nEntry_; index++)
            if (key_compare(kvEntry.kEntry, node, index) <= 0)
                break;

        uint32_t insert_return;

        // node is `LEAF`, insert and return.
        if (node->get_page_t() == page_t_t::LEAF)
        {
            // step 2: if equal, return `INSERT_NOTHING`. (note to validate index)
            if (index != node->nEntry_ && // kEntry > all node.keys, insert!!!
                key_compare(kvEntry.kEntry, node, index) == 0)
                insert_return = INSERT_NOTHING;

            // step 3: move the index to right, then insert(++BT.size), release write-lock.
            else {
                size_++; // increase B+Tree size
                leaf_ptr leaf = static_cast<leaf_ptr>(node);
                leaf->nEntry_++;
                // also valid when index is out of range :), since `index == leaf->nEntry_-1`
                for (uint32_t i = leaf->nEntry_ - 1; i > index; i--) {
                    leaf->keys_[i] = leaf->keys_[i - 1];
                    leaf->values_[i] = leaf->values_[i - 1];
                }
                leaf->insert_key(index, kvEntry.kEntry);
                leaf->insert_value(index, kvEntry.vEntry);
                insert_return = INSERT_KV;
                leaf->set_dirty();
            }

            node->page_write_unlock();

        } // end insert into LEAF


        // node is `INTERNAL`, recursively go down.
        else
        {
            // step 2: hold write-lock of node.br[index]
            //         if need to split node.br[index], then split.
            base_ptr child = fetch_node(static_cast<link_ptr>(node)->branch_[index]);
            child->page_write_lock();
            if (child->nEntry_ == MAX_KEY_SIZE)
                split(static_cast<link_ptr>(node), index, child);

            // step 3: release write-lock of node, then hold the read-lock of node.
            node->page_write_unlock();
            node->page_read_lock();

            // step 4: maybe update index and child=node.br[index] after split.
            if (index != node->nEntry_ && // if equal, no split happens.
                key_compare(kvEntry.kEntry, node, index) > 0) {
                child->page_write_unlock();
                child->unref();
                index++;
                child = fetch_node(static_cast<link_ptr>(node)->branch_[index]);
                child->page_write_lock();
            }

            // step 5: recursively go down then release read-lock.
            insert_return = INSERT_NONFULL(child, kvEntry);
            node->page_read_unlock();

            child->unref();

        } // end recursively go down

        return insert_return;

    }


    // return the maximum KeyEntry in the tree from page_id.
    KeyEntry BTree::max_KeyEntry(page_id_t page_id)
    {
        KeyEntry ret;
        base_ptr node = fetch_node(page_id);
        node->page_read_lock();
        if (node->get_page_t() != page_t_t::LEAF) {
            // go right down
            ret = max_KeyEntry(static_cast<link_ptr>(node)->branch_[node->nEntry_]);
        }
        else {
            leaf_ptr leaf = static_cast<leaf_ptr>(node);
            ret = leaf->read_key(leaf->nEntry_ - 1);
        }
        node->page_read_unlock();
        node->unref();
        return ret;
    }


    // all write-lock held, L.nEntry = R.nEtnry = KEY_MIN_SIZE
    // move R into L.
    //     move R.key[0..6] -> L.key[8..14], R.br[0..7] -> L.br[8..15], update parent_id
    //     move node.key[index] -> L.key[7]
    //     adjust node.key and node.branch
    void BTree::merge_internal(link_ptr node, uint32_t merge_index, link_ptr L, link_ptr R)
    {
        debug::DEBUG_LOG(debug::MERGE_INTERNAL,
            "merge_internal [node_id = %d], [merge_index = %d], [L = %d], [R = %d]\n",
            node->get_page_id(), merge_index, L->get_page_id(), R->get_page_id());

        debug_page(debug::MERGE_INTERNAL, node->get_page_id());
        debug_page(debug::MERGE_INTERNAL, L->get_page_id());
        debug_page(debug::MERGE_INTERNAL, R->get_page_id());

        // move R.key[0..6] -> L.key[8..14], R.br[0..7] -> L.br[8..15], update parent_id
        KeyEntry kEntry;
        for (uint32_t R_index = 0; R_index < KEY_MIDEIUM; R_index++) // [0..6]
        {
            const uint32_t L_index = R_index + KEY_MIDEIUM + 1; // [8..14]
            kEntry = R->read_key(R_index);
            L->insert_key(L_index, kEntry);
            R->erase_key(R_index);
            L->branch_[L_index] = R->branch_[R_index];
        }
        L->branch_[MAX_KEY_SIZE] = R->branch_[KEY_MIDEIUM];
        R->nEntry_ = 0;
        R->set_dirty();

        for (uint32_t index = 8; index <= MAX_KEY_SIZE; index++) // [8..15]
        {
            base_ptr adopted = fetch_node(L->branch_[index]);
            adopted->page_write_lock();
            adopted->set_parent_id(L->get_page_id());
            adopted->page_write_unlock();
            adopted->unref();
        }

        // move node.key[index] -> L.key[7]
        kEntry = node->read_key(merge_index);
        L->insert_key(KEY_MIDEIUM, kEntry);
        L->nEntry_ = MAX_KEY_SIZE;
        L->set_dirty();

        // adjust node.key and node.branch
        node->erase_key(merge_index);
        node->nEntry_--;
        for (uint32_t i = merge_index; i < node->nEntry_; i++)
        {
            node->keys_[i] = node->keys_[i + 1];
            node->branch_[i + 1] = node->branch_[i + 2];
        }
        node->set_dirty();

        debug_page(debug::MERGE_INTERNAL, node->get_page_id());
        debug_page(debug::MERGE_INTERNAL, L->get_page_id());
        debug::DEBUG_LOG(debug::MERGE_INTERNAL, "merge_internal end\n");
    }


    // all write-lock held, L.nEntry = R.nEtnry = KEY_MIN_SIZE
    // move R into L, erase node.k[index], node.br[index+1]
    //     move R.k-v[0..6] -> L.k-v[7.13], adjust relation
    //     erase node.k[index], node.br[index+1]
    //     adjust node.key and node.branch
    void BTree::merge_leaf(link_ptr node, uint32_t merge_index, leaf_ptr L, leaf_ptr R)
    {
        debug::DEBUG_LOG(debug::MERGE_LEAF,
            "merge_leaf [node_id = %d], [merge_index = %d], [L = %d], [R = %d]\n",
            node->get_page_id(), merge_index, L->get_page_id(), R->get_page_id());

        debug_page(debug::MERGE_LEAF, node->get_page_id());
        debug_page(debug::MERGE_LEAF, L->get_page_id());
        debug_page(debug::MERGE_LEAF, R->get_page_id());

        // move R.k-v[0..6] -> L.k-v[7.13], adjust relation
        KeyEntry kEntry;
        ValueEntry vEntry;
        for (uint32_t R_index = 0; R_index < KEY_MIDEIUM; R_index++) // [0..6]
        {
            const uint32_t L_index = R_index + KEY_MIDEIUM; // [7..13]
            kEntry = R->read_key(R_index);
            R->read_value(R_index, vEntry);
            L->insert_key(L_index, kEntry);
            L->insert_value(L_index, vEntry);
            R->erase_key(R_index);
            R->erase_value(R_index);
        }
        L->nEntry_ = KEY_MIDEIUM << 1;
        R->nEntry_ = 0;

        // adjust leaf-chain
        const page_id_t next_leaf = R->get_right_leaf();
        L->set_right_leaf(next_leaf);
        if (next_leaf != NOT_A_PAGE) {
            leaf_ptr right_leaf = static_cast<leaf_ptr>(fetch_node(next_leaf));
            right_leaf->page_write_lock();
            right_leaf->set_left_leaf(L->get_page_id());
            right_leaf->set_dirty();
            right_leaf->page_write_unlock();
            right_leaf->unref();
        }

        L->set_dirty();
        R->set_dirty();

        // erase node.k[index], node.br[index+1]
        node->erase_key(merge_index);

        // adjust node.key and node.branch
        node->nEntry_--;
        for (uint32_t i = merge_index; i < node->nEntry_; i++)
        {
            node->keys_[i] = node->keys_[i + 1];
            node->branch_[i + 1] = node->branch_[i + 2];
        }
        node->set_dirty();

        debug_page(debug::MERGE_LEAF, node->get_page_id());
        debug_page(debug::MERGE_LEAF, L->get_page_id());
        debug::DEBUG_LOG(debug::MERGE_LEAF, "merge_leaf end\n");
    }


    // REQUIREMENT:
    //              1. caller hold the write-lock of leaf.
    uint32_t BTree::erase_from_leaf(leaf_ptr leaf, const KeyEntry& kEntry)
    {
        // directly deleted if key exists. (return)
        uint32_t K_index = 0;
        for (; K_index < leaf->nEntry_; K_index++)
            if (key_compare(kEntry, leaf, K_index) == 0)
                break;
        if (K_index == leaf->nEntry_)
            return ERASE_NOTHING;
        else
        {
            size_--; // BT size

            leaf->erase_key(K_index);
            leaf->erase_value(K_index);
            leaf->nEntry_--;

            // shift left
            for (uint32_t i = K_index; i < leaf->nEntry_; i++)
            {
                leaf->keys_[i] = leaf->keys_[i + 1];
                leaf->values_[i] = leaf->values_[i + 1];
            }

            leaf->set_dirty();
            return ERASE_KV;
        }
    } // end function `erase_from_leaf(leaf, k)`


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
    uint32_t BTree::ERASE_NONMIN(link_ptr node, uint32_t index, base_ptr child, const KeyEntry& kEntry)
    {
        child->page_write_lock();

        // I. child is LEAF
        if (child->get_page_t() == page_t_t::LEAF)
        {
            debug::DEBUG_LOG(debug::ERASE_NONMIN_LEAF,
                "ERASE_NONMIN LEAF, [node_id = %d], [child_id = %d]\n",
                node->get_page_id(), child->get_page_id());

            leaf_ptr child_leaf = static_cast<leaf_ptr>(child);

            // a. child.nEntry > MIN_KEY
            if (child_leaf->nEntry_ > MIN_KEY_SIZE)
            {
                debug::DEBUG_LOG(debug::ERASE_NONMIN_LEAF,
                    "ERASE_NONMIN LEAF, child.nEntry > MIN_KEY, [node_id = %d], [child_id = %d]\n",
                    node->get_page_id(), child->get_page_id());

                node->page_write_unlock();
                node->page_read_lock();

                // directly deleted if key exists. (return)
                uint32_t erase_return = erase_from_leaf(child_leaf, kEntry);

                child->page_write_unlock();
                node->page_read_unlock();
                return erase_return;

            } // end a. child.nEntry > MIN_KEY


            // b. child.nEntry = MIN_KEY
            else
            {
                debug::DEBUG_LOG(debug::ERASE_NONMIN_LEAF,
                    "ERASE_NONMIN LEAF, child.nEntry = MIN_KEY, [node_id = %d], [child_id = %d]\n",
                    node->get_page_id(), child->get_page_id());

                // 1) left(right)-leaf.nEntry > MIN_KEY
                //      *steal* 1 k-v from that leaf.
                //      adjust node.key, child, leaf.
                //      directly deleted if key exists. (return)
                if (index != 0 // HACK: we suppose the steal and merge only take place in the same parent.
                    && child_leaf->get_left_leaf() != NOT_A_PAGE)
                {
                    leaf_ptr left_leaf = static_cast<leaf_ptr>(fetch_node(child_leaf->get_left_leaf()));
                    left_leaf->page_write_lock();

                    if (left_leaf->nEntry_ > MIN_KEY_SIZE) // must return inside
                    {
                        // steal left.k-v[nEntry-1]

                        // shift right child_leaf
                        for (uint32_t i = child_leaf->nEntry_; i > 0; i--)
                        {
                            child_leaf->keys_[i] = child_leaf->keys_[i - 1];
                            child_leaf->values_[i] = child_leaf->values_[i - 1];
                        }
                        child_leaf->nEntry_++;
                        child_leaf->set_dirty();

                        // steal left_leaf.k-v[nEntry-1]
                        const uint32_t steal_index = left_leaf->nEntry_ - 1;
                        KeyEntry kEntry_steal;
                        ValueEntry vEntry_steal;
                        kEntry_steal = left_leaf->read_key(steal_index);
                        left_leaf->read_value(steal_index, vEntry_steal);
                        child_leaf->insert_key(0, kEntry_steal);
                        child_leaf->insert_value(0, vEntry_steal);
                        left_leaf->erase_key(steal_index);
                        left_leaf->erase_value(steal_index);
                        left_leaf->nEntry_--;
                        left_leaf->set_dirty();

                        // set node.k[index-1]
                        node->erase_key(index - 1);
                        kEntry_steal = left_leaf->read_key(left_leaf->nEntry_ - 1);
                        node->insert_key(index - 1, kEntry_steal);
                        node->set_dirty();

                        // directly deleted if key exists. (return)
                        uint32_t erase_return = erase_from_leaf(child_leaf, kEntry);

                        left_leaf->page_write_unlock();
                        left_leaf->unref();
                        child->page_write_unlock();
                        node->page_write_unlock();

                        return erase_return;
                    }

                    // if left.nEntry =  MIN
                    left_leaf->page_write_unlock();
                    left_leaf->unref();

                }
                // try to steal from right
                if (index != node->nEntry_
                    && child_leaf->get_right_leaf() != NOT_A_PAGE)
                {
                    leaf_ptr right_leaf = static_cast<leaf_ptr>(fetch_node(child_leaf->get_right_leaf()));
                    right_leaf->page_write_lock();

                    if (right_leaf->nEntry_ > MIN_KEY_SIZE) // must return inside
                    {
                        // steal right.k-v[0]
                        KeyEntry kEntry_steal;
                        ValueEntry vEntry_steal;
                        kEntry_steal = right_leaf->read_key(0);
                        right_leaf->read_value(0, vEntry_steal);
                        child_leaf->insert_key(child_leaf->nEntry_, kEntry_steal);
                        child_leaf->insert_value(child_leaf->nEntry_, vEntry_steal);
                        child_leaf->nEntry_++;
                        child_leaf->set_dirty();
                        right_leaf->erase_key(0);
                        right_leaf->erase_value(0);

                        // right_leaf shift left
                        right_leaf->nEntry_--;
                        for (uint32_t i = 0; i < right_leaf->nEntry_; i++)
                        {
                            right_leaf->keys_[i] = right_leaf->keys_[i + 1];
                            right_leaf->values_[i] = right_leaf->values_[i + 1];
                        }
                        right_leaf->set_dirty();

                        // set node.k[index]
                        node->erase_key(index);
                        node->insert_key(index, kEntry_steal);
                        node->set_dirty();

                        // directly deleted if key exists. (return)
                        uint32_t erase_return = erase_from_leaf(child_leaf, kEntry);

                        right_leaf->page_write_unlock();
                        right_leaf->unref();
                        child->page_write_unlock();
                        node->page_write_unlock();

                        return erase_return;
                    }

                    // if right.nEntry = MIN
                    right_leaf->page_write_unlock();
                    right_leaf->unref();
                }

                // 2) left(right)-leaf.nEntry = MIN_KEY
                //      *merge* 2 leaf. (NB: manage ValuePage lifetime, left-right)
                //      delete noed.key[index], adjust node.branch, node.nEntry--
                //      directly deleted if key exists. (return)
                if (index != 0 // HACK: we suppose the steal and merge only take place in the same parent.
                    && child_leaf->get_left_leaf() != NOT_A_PAGE)
                {
                    // must return inside, since you ever enter in this block, nEntry = MIN
                    leaf_ptr left_leaf = static_cast<leaf_ptr>(fetch_node(child_leaf->get_left_leaf()));
                    left_leaf->page_write_lock();

                    // merge into left_leaf
                    merge_leaf(node, index - 1, left_leaf, child_leaf);

                    uint32_t erase_return = erase_from_leaf(left_leaf, kEntry);

                    left_leaf->page_write_unlock();
                    left_leaf->unref();
                    child->page_write_unlock();
                    node->page_write_unlock();

                    return erase_return;
                }
                // try to merge from right
                if (index != node->nEntry_
                    && child_leaf->get_right_leaf() != NOT_A_PAGE)
                {
                    // must return inside, since you ever enter in this block, nEntry = MIN
                    leaf_ptr right_leaf = static_cast<leaf_ptr>(fetch_node(child_leaf->get_right_leaf()));
                    right_leaf->page_write_lock();

                    // merge into child_leaf
                    merge_leaf(node, index, child_leaf, right_leaf);

                    uint32_t erase_return = erase_from_leaf(child_leaf, kEntry);

                    right_leaf->page_write_unlock();
                    right_leaf->unref();
                    child->page_write_unlock();
                    node->page_write_unlock();

                    return erase_return;
                }

                debug::ERROR_LOG("`BTree::ERASE_NONMIN()` no steal/merge from left/right");

            } // end b. child.nEntry = MIN_KEY


        } // end I. child is LEAF


        // II. child is INTERNAL
        else
        {
            debug::DEBUG_LOG(debug::ERASE_NONMIN_INTERNAL,
                "ERASE_NONMIN INTERNAL, [node_id = %d], [child_id = %d]\n",
                node->get_page_id(), child->get_page_id());

            link_ptr child_link = static_cast<link_ptr>(child);

            // a. child.nEntry > MIN_KEY
            if (child_link->nEntry_ > MIN_KEY_SIZE)
            {
                debug::DEBUG_LOG(debug::ERASE_NONMIN_INTERNAL,
                    "ERASE_NONMIN INTERNAL, child.nEntry > MIN_KEY, [node_id = %d], [child_id = %d]\n",
                    node->get_page_id(), child->get_page_id());

                // find K_index, such that child.key[K_index] <= kEntry
                // recusively go down.
                node->page_write_unlock();
                node->page_read_lock();

                uint32_t K_index = 0;
                for (; K_index < child_link->nEntry_; K_index++)
                    if (key_compare(kEntry, child_link, K_index) <= 0)
                        break;
                base_ptr child_child = fetch_node(child_link->branch_[K_index]);
                // hold child write-lock
                uint32_t erase_return = ERASE_NONMIN(child_link, K_index, child_child, kEntry);
                child_child->unref();

                node->page_read_unlock();

                return erase_return;

            } // end a. child.nEntry > MIN_KEY


            // b. child.nEntry = MIN_KEY, call node.branch[index+1] (if has) as R.
            else if (index != node->nEntry_)
            {
                link_ptr R = static_cast<link_ptr>(fetch_node(node->branch_[index + 1]));
                R->page_write_lock();


                debug::DEBUG_LOG(debug::ERASE_NONMIN_INTERNAL,
                    "ERASE_NONMIN INTERNAL, child.nEntry = MIN_KEY, [node_id = %d], [child_id = %d], [R = %d]\n",
                    node->get_page_id(), child->get_page_id(), R->get_page_id());


                // 1) R.nEntry > MIN_KEY
                if (R->nEntry_ > MIN_KEY_SIZE)
                {
                    debug::DEBUG_LOG(debug::ERASE_NONMIN_INTERNAL,
                        "ERASE_NONMIN INTERNAL, child.nEntry = MIN_KEY, R.nEntry > MIN_KEY, steal, [node_id = %d], [child_id = %d], [R = %d]\n",
                        node->get_page_id(), child->get_page_id(), R->get_page_id());

                    // *steal* R.branch[0] to child.branch[8]
                    // set node.key[index] = old-R.key[0], update parent.
                    // set child.key[7] = old-node.key[index]
                    // find K_index, recusively go down.

                    // *steal* R.branch[0] to child.branch[8]
                    KeyEntry kEntry_steal_R = R->read_key(0);
                    child_link->branch_[MIN_KEY_SIZE + 1] = R->branch_[0];
                    R->erase_key(0);
                    child_link->nEntry_++;
                    child_link->set_dirty();

                    // R shift left
                    R->nEntry_--;
                    for (uint32_t i = 0; i < R->nEntry_; i++)
                    {
                        R->keys_[i] = R->keys_[i + 1];
                        R->branch_[i] = R->branch_[i + 1];
                    }
                    R->branch_[R->nEntry_] = R->branch_[R->nEntry_ + 1];
                    R->set_dirty();

                    R->page_write_unlock();
                    R->unref();

                    // set node.k[index] = old-R.key[0], update parent.
                    // set L.key[MIN_KEY] = old-node.key[index]
                    // now kEntry_steal = old-R.key[0]
                    KeyEntry kEntry_steal_node = node->read_key(index);
                    node->erase_key(index);
                    node->insert_key(index, kEntry_steal_R);
                    node->set_dirty();
                    // set L.key[MIN_KEY] = old-node.key[index]
                    child_link->insert_key(MIN_KEY_SIZE, kEntry_steal_node);
                    // update parent
                    base_ptr adopted = fetch_node(child_link->branch_[MIN_KEY_SIZE + 1]);
                    adopted->page_write_lock();
                    adopted->set_parent_id(child_link->get_page_id());
                    adopted->page_write_unlock();
                    adopted->unref();

                    // find K_index, recusively go down.

                    node->page_write_unlock();
                    node->page_read_lock();

                    uint32_t K_index = 0;
                    for (; K_index < child_link->nEntry_; K_index++)
                        if (key_compare(kEntry, child_link, K_index) <= 0)
                            break;
                    base_ptr child_child = fetch_node(child_link->branch_[K_index]);
                    // hold child write-lock
                    uint32_t erase_return = ERASE_NONMIN(child_link, K_index, child_child, kEntry);
                    child_child->unref();

                    node->page_read_unlock();

                    return erase_return;

                } // end 1) R.nEntry > MIN_KEY


                // 2) R.nEntry = MIN_KEY
                else
                {
                    debug::DEBUG_LOG(debug::ERASE_NONMIN_INTERNAL,
                        "ERASE_NONMIN INTERNAL, child.nEntry = MIN_KEY, R.nEntry = MIN_KEY, merge, [node_id = %d], [child_id = %d], [R = %d]\n",
                        node->get_page_id(), child->get_page_id(), R->get_page_id());

                    // *merge* child and R into child.
                    //     move R.key[0..6] -> child.key[8..14], R.br[0..7] -> L.br[8..15], update parent_id
                    //     move noed.key[index] -> child.key[7]
                    //     adjust node.key and node.branch
                    // find K_index, recusively go down.

                    merge_internal(node, index, child_link, R);

                    R->page_write_unlock();
                    R->unref();

                    node->page_write_unlock();
                    node->page_read_lock();

                    uint32_t K_index = 0;
                    for (; K_index < child_link->nEntry_; K_index++)
                        if (key_compare(kEntry, child_link, K_index) <= 0)
                            break;
                    base_ptr child_child = fetch_node(child_link->branch_[K_index]);
                    // hold child write-lock
                    uint32_t erase_return = ERASE_NONMIN(child_link, K_index, child_child, kEntry);
                    child_child->unref();

                    node->page_read_unlock();

                    return erase_return;

                } // end 2) R.nEntry = MIN_KEY


            } // end b. child.nEntry = MIN_KEY, call node.branch[index+1] as R.


            // c. child.nEntry = MIN_KEY, call node.branch[index-1] as L.
            else
            {
                link_ptr L = static_cast<link_ptr>(fetch_node(node->branch_[index - 1]));
                L->page_write_lock();


                debug::DEBUG_LOG(debug::ERASE_NONMIN_INTERNAL,
                    "ERASE_NONMIN INTERNAL, child.nEntry = MIN_KEY, [node_id = %d], [child_id = %d], [L = %d]\n",
                    node->get_page_id(), child->get_page_id(), L->get_page_id());


                // 1) L.nEntry > MIN_KEY
                if (L->nEntry_ > MIN_KEY_SIZE)
                {
                    debug::DEBUG_LOG(debug::ERASE_NONMIN_INTERNAL,
                        "ERASE_NONMIN INTERNAL, child.nEntry = MIN_KEY, L.nEntry > MIN_KEY, steal, [node_id = %d], [child_id = %d], [L = %d]\n",
                        node->get_page_id(), child->get_page_id(), L->get_page_id());

                    // *steal* L.branch[L.nEntry] to child.branch[0]
                    // set child.key[0] = node.key[index-1], update parent.
                    // set node.key[index-1] = old-L.key[L.nEntry-1]
                    // find K_index, recusively go down.

                    // child shift right
                    child_link->nEntry_++;
                    child_link->branch_[MIN_KEY_SIZE + 1] = child_link->branch_[MIN_KEY_SIZE];
                    for (uint32_t i = MIN_KEY_SIZE; i > 0; i--)
                    {
                        child_link->keys_[i] = child_link->keys_[i - 1];
                        child_link->branch_[i] = child_link->branch_[i - 1];
                    }

                    // *steal* L.branch[L.nEntry] to child.branch[0]
                    child_link->branch_[0] = L->branch_[L->nEntry_];
                    L->nEntry_--;
                    KeyEntry kEntry_steal_L = L->read_key(L->nEntry_);
                    L->erase_key(L->nEntry_);
                    child_link->set_dirty();
                    L->set_dirty();

                    // set child.key[0] = node.key[index-1], update parent.
                    KeyEntry kEntry_steal_node = node->read_key(index - 1);
                    child->insert_key(0, kEntry_steal_node);
                    // update parent
                    base_ptr adopted = fetch_node(child_link->branch_[0]);
                    adopted->page_write_lock();
                    adopted->set_parent_id(child_link->get_page_id());
                    adopted->page_write_unlock();
                    adopted->unref();

                    // set node.key[index-1] = old-L.key[L.nEntry-1]
                    node->erase_key(index - 1);
                    node->insert_key(index - 1, kEntry_steal_L);
                    node->set_dirty();

                    L->page_write_unlock();
                    L->unref();

                    // find K_index, recusively go down.

                    node->page_write_unlock();
                    node->page_read_lock();

                    uint32_t K_index = 0;
                    for (; K_index < child_link->nEntry_; K_index++)
                        if (key_compare(kEntry, child_link, K_index) <= 0)
                            break;
                    base_ptr child_child = fetch_node(child_link->branch_[K_index]);
                    // hold child write-lock
                    uint32_t erase_return = ERASE_NONMIN(child_link, K_index, child_child, kEntry);
                    child_child->unref();

                    node->page_read_unlock();

                    return erase_return;

                } // end L.nEntry > MIN_KEY


                // 2) L.nEntry = MIN_KEY
                else
                {
                    debug::DEBUG_LOG(debug::ERASE_NONMIN_INTERNAL,
                        "ERASE_NONMIN INTERNAL, child.nEntry = MIN_KEY, L.nEntry = MIN_KEY, merge, [node_id = %d], [child_id = %d], [L = %d]\n",
                        node->get_page_id(), child->get_page_id(), L->get_page_id());

                    // *merge* L and child into child.
                    //     move child.key[0..6] -> L.key[8..14], child.br[0..7] -> L.br[8..15], update parent_id
                    //     move noed.key[index-1] -> L.key[7]
                    //     adjust node.key and node.branch
                    // find K_index, recusively go down.

                    merge_internal(node, index - 1, L, child_link);
                    child_link->page_write_unlock();

                    // find K_index, recusively go down.

                    node->page_write_unlock();
                    node->page_read_lock();

                    uint32_t K_index = 0;
                    for (; K_index < L->nEntry_; K_index++)
                        if (key_compare(kEntry, L, K_index) <= 0)
                            break;
                    base_ptr child_child = fetch_node(L->branch_[K_index]);
                    // hold L write-lock
                    uint32_t erase_return = ERASE_NONMIN(L, K_index, child_child, kEntry);
                    child_child->unref();

                    node->page_read_unlock();
                    L->unref(); // here special, since no caller

                    return erase_return;

                } // end L.nEntry = MIN_KEY

            } // end c. child.nEntry = MIN_KEY, call node.branch[index-1] as L.


        } // end II. child is INTERNAL


    } // end function `BTree::ERASE_NONMIN()`


    void BTree::debug() const {
        const page_id_t cur_page_id = buffer_pool_->disk_manager_->get_cut_page_id();
        for (page_id_t i = 1; i <= cur_page_id; i++)
            debug_page(true, i);
    }
    void BTree::debug_page(bool config, page_id_t page_id) const {
        debug::debug_page(config, page_id, this->buffer_pool_);
    }


    // return:
    // < 0, if KeyEntry < keys[index]
    // = 0, if KeyEntry = keys[index]
    // > 0, if KeyEntry > keys[index]
    int32_t key_compare(const KeyEntry& kEntry, const BTreePage* node, uint32_t key_index) {
        if (node->get_key_t() == key_t_t::INTEGER) {
            return kEntry.key_int - node->keys_[key_index];
        }
        // TODO: compound key
        //          design a protocol that forms a bijection between compound keys and key_str.
        else {
            const KeyEntry key = node->read_key(key_index);
            return kEntry.key_str.compare(key.key_str);
        }
    }


} // end namespace DB::tree