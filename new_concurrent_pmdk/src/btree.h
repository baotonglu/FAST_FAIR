/*
   Copyright (c) 2018, UNIST. All rights reserved.  The license is a free
   non-exclusive, non-transferable license to reproduce, use, modify and display
   the source code version of the Software, with or without modifications solely
   for non-commercial research, educational or evaluation purposes. The license
   does not entitle Licensee to technical support, telephone assistance,
   enhancements or updates to the Software. All rights, title to and ownership
   interest in the Software, including all intellectual property rights therein
   shall remain in UNIST.

   Please use at your own risk.
*/

#include <cassert>
#include <climits>
#include <fstream>
#include <future>
#include <iostream>
#include <math.h>
#include <mutex>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <vector>
#include "allocator.h"
#include "tree.h"

#define PAGESIZE 512

#define CPU_FREQ_MHZ (1994)
#define DELAY_IN_NS (1000)
#define CACHE_LINE_SIZE 64
#define QUERY_NUM 25

#define IS_FORWARD(c) (c % 2 == 0)

using entry_key_t = int64_t;
pthread_mutex_t print_mtx;

static inline void cpu_pause() { __asm__ volatile("pause" ::: "memory"); }
static inline unsigned long read_tsc(void) {
  unsigned long var;
  unsigned int hi, lo;

  asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
  var = ((unsigned long long int)hi << 32) | lo;

  return var;
}

unsigned long write_latency_in_ns = 0;
unsigned long long search_time_in_insert = 0;
unsigned int gettime_cnt = 0;
unsigned long long clflush_time_in_insert = 0;
unsigned long long update_time_in_insert = 0;
int clflush_cnt = 0;
int node_cnt = 0;

using namespace std;

inline void mfence() { asm volatile("mfence" ::: "memory"); }

inline void clflush(char *data, int len) {
  volatile char *ptr = (char *)((unsigned long)data & ~(CACHE_LINE_SIZE - 1));
  mfence();
  for (; ptr < data + len; ptr += CACHE_LINE_SIZE) {
    unsigned long etsc =
        read_tsc() + (unsigned long)(write_latency_in_ns * CPU_FREQ_MHZ / 1000);
    asm volatile("clflush %0" : "+m"(*(volatile char *)ptr));
    while (read_tsc() < etsc)
      cpu_pause();
  }
  mfence();
}

template <class T, class P>
class page;

template <class T, class P>
class btree : public Tree<T, P>{
private:
  int height;
  char *root;
  typedef std::pair<T, P> V;
public:
  btree();
  void setNewRoot(char *); // parameter is pointer to new root
  void getNumberOfNodes();
  bool insert(const T&, const P&);
  void btree_insert_internal(char *, T, P, uint32_t);
  void btree_delete(T);
  void btree_delete_internal(T, P, uint32_t, T *,
                             bool *, page<T, P> **);
  P search(const T&) const;
  void bulk_load(const V[], int);
  void btree_search_range(T, T, unsigned long *);
  void printAll();

  friend class page<T, P>;
};

template<class T, class P>
class header {
private:
  page<T, P> *leftmost_ptr;     // 8 bytes
  page<T, P> *sibling_ptr;      // 8 bytes
  uint32_t level;         // 4 bytes
  uint8_t switch_counter; // 1 bytes
  uint8_t is_deleted;     // 1 bytes
  int16_t last_index;     // 2 bytes
  std::mutex *mtx;        // 8 bytes

  friend class page<T, P>;
  friend class btree<T, P>;

public:
  header() {
    mtx = new std::mutex();

    leftmost_ptr = NULL;
    sibling_ptr = NULL;
    switch_counter = 0;
    last_index = -1;
    is_deleted = false;
  }

  ~header() { delete mtx; }
};

template <class T, class P>
class entry {
private:
  T key; // 8 bytes
  P ptr; // 8 bytes

public:
  entry() {
    key = LONG_MAX;
    ptr = NULL;
  }

  friend class page<T, P>;
  friend class btree<T, P>;
};

const int cardinality = (PAGESIZE - sizeof(header<entry_key_t, char*>)) / sizeof(entry<entry_key_t, char*>);

template<class T, class P>
class page {
private:
  header<T, P> hdr;                 // header in persistent memory, 16 bytes
  entry<T, P> records[cardinality]; // slots in persistent memory, 16 bytes * n

public:
  friend class btree<T, P>;

  page(uint32_t level = 0) {
    hdr.level = level;
    records[0].ptr = NULL;
  }

  // this is called when tree grows
  page(page<T, P> *left, T key, page<T, P> *right, uint32_t level = 0) {
    hdr.leftmost_ptr = left;
    hdr.level = level;
    records[0].key = key;
    records[0].ptr = (P)right;
    records[1].ptr = NULL;

    hdr.last_index = 0;

    clflush((char *)this, sizeof(page));
  }

  // BT: DRAM allocation
  /*
  void *operator new(size_t size) {
    void *ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }*/

  inline int count() {
    uint8_t previous_switch_counter;
    int count = 0;
    do {
      previous_switch_counter = hdr.switch_counter;
      count = hdr.last_index + 1;

      while (count >= 0 && records[count].ptr != NULL) {
        if (IS_FORWARD(previous_switch_counter))
          ++count;
        else
          --count;
      }

      if (count < 0) {
        count = 0;
        while (records[count].ptr != NULL) {
          ++count;
        }
      }

    } while (previous_switch_counter != hdr.switch_counter);

    return count;
  }

  inline bool remove_key(T key) {
    // Set the switch_counter
    if (IS_FORWARD(hdr.switch_counter))
      ++hdr.switch_counter;

    bool shift = false;
    int i;
    for (i = 0; records[i].ptr != NULL; ++i) {
      if (!shift && records[i].key == key) {
        records[i].ptr =
            (i == 0) ? (P)hdr.leftmost_ptr : records[i - 1].ptr;
        shift = true;
      }

      if (shift) {
        records[i].key = records[i + 1].key;
        records[i].ptr = records[i + 1].ptr;

        // flush
        uint64_t records_ptr = (uint64_t)(&records[i]);
        int remainder = records_ptr % CACHE_LINE_SIZE;
        bool do_flush =
            (remainder == 0) ||
            ((((int)(remainder + sizeof(entry<T, P>)) / CACHE_LINE_SIZE) == 1) &&
             ((remainder + sizeof(entry<T, P>)) % CACHE_LINE_SIZE) != 0);
        if (do_flush) {
          clflush((char *)records_ptr, CACHE_LINE_SIZE);
        }
      }
    }

    if (shift) {
      --hdr.last_index;
    }
    return shift;
  }

  bool remove(btree<T, P> *bt, T key, bool only_rebalance = false,
              bool with_lock = true) {
    hdr.mtx->lock();

    bool ret = remove_key(key);

    hdr.mtx->unlock();

    return ret;
  }

  /*
   * Although we implemented the rebalancing of B+-Tree, it is currently blocked
   * for the performance. Please refer to the follow. Chi, P., Lee, W. C., &
   * Xie, Y. (2014, August). Making B+-tree efficient in PCM-based main memory.
   * In Proceedings of the 2014 international symposium on Low power electronics
   * and design (pp. 69-74). ACM.
   */
  bool remove_rebalancing(btree<T, P> *bt, T key,
                          bool only_rebalance = false, bool with_lock = true) {
    if (with_lock) {
      hdr.mtx->lock();
    }
    if (hdr.is_deleted) {
      if (with_lock) {
        hdr.mtx->unlock();
      }
      return false;
    }

    if (!only_rebalance) {
      register int num_entries_before = count();

      // This node is root
      if (this == (page<T, P> *)bt->root) {
        if (hdr.level > 0) {
          if (num_entries_before == 1 && !hdr.sibling_ptr) {
            bt->root = (char *)hdr.leftmost_ptr;
            clflush((char *)&(bt->root), sizeof(char *));

            hdr.is_deleted = 1;
          }
        }

        // Remove the key from this node
        bool ret = remove_key(key);

        if (with_lock) {
          hdr.mtx->unlock();
        }
        return true;
      }

      bool should_rebalance = true;
      // check the node utilization
      if (num_entries_before - 1 >= (int)((cardinality - 1) * 0.5)) {
        should_rebalance = false;
      }

      // Remove the key from this node
      bool ret = remove_key(key);

      if (!should_rebalance) {
        if (with_lock) {
          hdr.mtx->unlock();
        }
        return (hdr.leftmost_ptr == NULL) ? ret : true;
      }
    }

    // Remove a key from the parent node
    T deleted_key_from_parent = 0;
    bool is_leftmost_node = false;
    page<T, P> *left_sibling;
    bt->btree_delete_internal(key, (P)this, hdr.level + 1,
                              &deleted_key_from_parent, &is_leftmost_node,
                              &left_sibling);

    if (is_leftmost_node) {
      if (with_lock) {
        hdr.mtx->unlock();
      }

      if (!with_lock) {
        hdr.sibling_ptr->hdr.mtx->lock();
      }
      hdr.sibling_ptr->remove(bt, hdr.sibling_ptr->records[0].key, true,
                              with_lock);
      if (!with_lock) {
        hdr.sibling_ptr->hdr.mtx->unlock();
      }
      return true;
    }

    if (with_lock) {
      left_sibling->hdr.mtx->lock();
    }

    while (left_sibling->hdr.sibling_ptr != this) {
      if (with_lock) {
        page<T, P> *t = left_sibling->hdr.sibling_ptr;
        left_sibling->hdr.mtx->unlock();
        left_sibling = t;
        left_sibling->hdr.mtx->lock();
      } else
        left_sibling = left_sibling->hdr.sibling_ptr;
    }

    register int num_entries = count();
    register int left_num_entries = left_sibling->count();

    // Merge or Redistribution
    int total_num_entries = num_entries + left_num_entries;
    if (hdr.leftmost_ptr)
      ++total_num_entries;

    T parent_key;

    if (total_num_entries > cardinality - 1) { // Redistribution
      register int m = (int)ceil(total_num_entries / 2);

      if (num_entries < left_num_entries) { // left -> right
        if (hdr.leftmost_ptr == nullptr) {
          for (int i = left_num_entries - 1; i >= m; i--) {
            insert_key(left_sibling->records[i].key,
                       left_sibling->records[i].ptr, &num_entries);
          }

          left_sibling->records[m].ptr = nullptr;
          clflush((char *)&(left_sibling->records[m].ptr), sizeof(char *));

          left_sibling->hdr.last_index = m - 1;
          clflush((char *)&(left_sibling->hdr.last_index), sizeof(int16_t));

          parent_key = records[0].key;
        } else {
          insert_key(deleted_key_from_parent, (P)hdr.leftmost_ptr,
                     &num_entries);

          for (int i = left_num_entries - 1; i > m; i--) {
            insert_key(left_sibling->records[i].key,
                       left_sibling->records[i].ptr, &num_entries);
          }

          parent_key = left_sibling->records[m].key;

          hdr.leftmost_ptr = (page<T, P> *)left_sibling->records[m].ptr;
          clflush((char *)&(hdr.leftmost_ptr), sizeof(page<T, P> *));

          left_sibling->records[m].ptr = nullptr;
          clflush((char *)&(left_sibling->records[m].ptr), sizeof(char *));

          left_sibling->hdr.last_index = m - 1;
          clflush((char *)&(left_sibling->hdr.last_index), sizeof(int16_t));
        }

        if (left_sibling == ((page<T, P> *)bt->root)) {
          //page *new_root =
          //    new page(left_sibling, parent_key, this, hdr.level + 1);
          //BT
          page<T, P> *new_root;
          my_alloc::BasePMPool::ZAllocate((void**)&new_root, sizeof(page));
          new (new_root) page(left_sibling, parent_key, this, hdr.level + 1);

          bt->setNewRoot((char *)new_root);
        } else {
          bt->btree_insert_internal((char *)left_sibling, parent_key,
                                    (P)this, hdr.level + 1);
        }
      } else { // from leftmost case
        hdr.is_deleted = 1;
        clflush((char *)&(hdr.is_deleted), sizeof(uint8_t));
        //BT
        page<T, P> *new_sibling;
        my_alloc::BasePMPool::ZAllocate((void**)&new_sibling, sizeof(page));
        new (new_sibling) page(hdr.level);
        // = new page(hdr.level);

        new_sibling->hdr.mtx->lock();
        new_sibling->hdr.sibling_ptr = hdr.sibling_ptr;

        int num_dist_entries = num_entries - m;
        int new_sibling_cnt = 0;

        if (hdr.leftmost_ptr == nullptr) {
          for (int i = 0; i < num_dist_entries; i++) {
            left_sibling->insert_key(records[i].key, records[i].ptr,
                                     &left_num_entries);
          }

          for (int i = num_dist_entries; records[i].ptr != NULL; i++) {
            new_sibling->insert_key(records[i].key, records[i].ptr,
                                    &new_sibling_cnt, false);
          }

          clflush((char *)(new_sibling), sizeof(page));

          left_sibling->hdr.sibling_ptr = new_sibling;
          clflush((char *)&(left_sibling->hdr.sibling_ptr), sizeof(page<T, P> *));

          parent_key = new_sibling->records[0].key;
        } else {
          left_sibling->insert_key(deleted_key_from_parent,
                                   (P)hdr.leftmost_ptr, &left_num_entries);

          for (int i = 0; i < num_dist_entries - 1; i++) {
            left_sibling->insert_key(records[i].key, records[i].ptr,
                                     &left_num_entries);
          }

          parent_key = records[num_dist_entries - 1].key;

          new_sibling->hdr.leftmost_ptr =
              (page<T, P> *)records[num_dist_entries - 1].ptr;
          for (int i = num_dist_entries; records[i].ptr != NULL; i++) {
            new_sibling->insert_key(records[i].key, records[i].ptr,
                                    &new_sibling_cnt, false);
          }
          clflush((char *)(new_sibling), sizeof(page));

          left_sibling->hdr.sibling_ptr = new_sibling;
          clflush((char *)&(left_sibling->hdr.sibling_ptr), sizeof(page<T, P> *));
        }

        if (left_sibling == ((page<T, P> *)bt->root)) {
          //page *new_root =
          //    new page(left_sibling, parent_key, new_sibling, hdr.level + 1);
          //BT
          page<T, P> *new_root;
          my_alloc::BasePMPool::ZAllocate((void**)&new_root, sizeof(page));
          new (new_root) page(left_sibling, parent_key, new_sibling, hdr.level + 1);
          bt->setNewRoot((char *)new_root);
        } else {
          bt->btree_insert_internal((char *)left_sibling, parent_key,
                                    (P)new_sibling, hdr.level + 1);
        }

        new_sibling->hdr.mtx->unlock();
      }
    } else {
      hdr.is_deleted = 1;
      clflush((char *)&(hdr.is_deleted), sizeof(uint8_t));

      if (hdr.leftmost_ptr)
        left_sibling->insert_key(deleted_key_from_parent,
                                 (P)hdr.leftmost_ptr, &left_num_entries);

      for (int i = 0; records[i].ptr != NULL; ++i) {
        left_sibling->insert_key(records[i].key, records[i].ptr,
                                 &left_num_entries);
      }

      left_sibling->hdr.sibling_ptr = hdr.sibling_ptr;
      clflush((char *)&(left_sibling->hdr.sibling_ptr), sizeof(page<T, P> *));
    }

    if (with_lock) {
      left_sibling->hdr.mtx->unlock();
      hdr.mtx->unlock();
    }

    return true;
  }

  inline void insert_key(T key, P ptr, int *num_entries,
                         bool flush = true, bool update_last_index = true) {
    // update switch_counter
    if (!IS_FORWARD(hdr.switch_counter))
      ++hdr.switch_counter;

    // FAST
    if (*num_entries == 0) { // this page is empty
      entry<T, P> *new_entry = (entry<T, P> *)&records[0];
      entry<T, P> *array_end = (entry<T, P> *)&records[1];
      new_entry->key = (T)key;
      new_entry->ptr = ptr;

      array_end->ptr = (P)NULL;

      if (flush) {
        clflush((char *)this, CACHE_LINE_SIZE);
      }
    } else {
      int i = *num_entries - 1, inserted = 0, to_flush_cnt = 0;
      records[*num_entries + 1].ptr = records[*num_entries].ptr;
      if (flush) {
        if ((uint64_t) & (records[*num_entries + 1].ptr) % CACHE_LINE_SIZE == 0)
          clflush((char *)&(records[*num_entries + 1].ptr), sizeof(char *));
      }

      // FAST
      for (i = *num_entries - 1; i >= 0; i--) {
        if (key < records[i].key) {
          records[i + 1].ptr = records[i].ptr;
          records[i + 1].key = records[i].key;

          if (flush) {
            uint64_t records_ptr = (uint64_t)(&records[i + 1]);

            int remainder = records_ptr % CACHE_LINE_SIZE;
            bool do_flush =
                (remainder == 0) ||
                ((((int)(remainder + sizeof(entry<T, P>)) / CACHE_LINE_SIZE) == 1) &&
                 ((remainder + sizeof(entry<T, P>)) % CACHE_LINE_SIZE) != 0);
            if (do_flush) {
              clflush((char *)records_ptr, CACHE_LINE_SIZE);
              to_flush_cnt = 0;
            } else
              ++to_flush_cnt;
          }
        } else {
          records[i + 1].ptr = records[i].ptr;
          records[i + 1].key = key;
          records[i + 1].ptr = ptr;

          if (flush)
            clflush((char *)&records[i + 1], sizeof(entry<T, P>));
          inserted = 1;
          break;
        }
      }
      if (inserted == 0) {
        records[0].ptr = (P)hdr.leftmost_ptr;
        records[0].key = key;
        records[0].ptr = ptr;
        if (flush)
          clflush((char *)&records[0], sizeof(entry<T, P>));
      }
    }

    if (update_last_index) {
      hdr.last_index = *num_entries;
    }
    ++(*num_entries);
  }

  // Insert a new key - FAST and FAIR
  page<T, P> *store(btree<T, P> *bt, char *left, T key, P right, bool flush,
              bool with_lock, page<T, P> *invalid_sibling = NULL) {
    if (with_lock) {
      hdr.mtx->lock(); // Lock the write lock
    }
    if (hdr.is_deleted) {
      if (with_lock) {
        hdr.mtx->unlock();
      }

      return NULL;
    }

    // If this node has a sibling node,
    if (hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling)) {
      // Compare this key with the first key of the sibling
      if (key > hdr.sibling_ptr->records[0].key) {
        if (with_lock) {
          hdr.mtx->unlock(); // Unlock the write lock
        }
        return hdr.sibling_ptr->store(bt, NULL, key, right, true, with_lock,
                                      invalid_sibling);
      }
    }

    register int num_entries = count();

    // FAST
    if (num_entries < cardinality - 1) {
      insert_key(key, right, &num_entries, flush);

      if (with_lock) {
        hdr.mtx->unlock(); // Unlock the write lock
      }

      return this;
    } else { // FAIR
      // overflow
      // create a new node
      //page *sibling = new page(hdr.level);
      //BT: use PMDK allocator
      page<T, P> *sibling;
      my_alloc::BasePMPool::ZAllocate((void**)&sibling, sizeof(page));
      new (sibling) page(hdr.level);

      register int m = (int)ceil(num_entries / 2);
      T split_key = records[m].key;

      // migrate half of keys into the sibling
      int sibling_cnt = 0;
      if (hdr.leftmost_ptr == NULL) { // leaf node
        for (int i = m; i < num_entries; ++i) {
          sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt,
                              false);
        }
      } else { // internal node
        for (int i = m + 1; i < num_entries; ++i) {
          sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt,
                              false);
        }
        sibling->hdr.leftmost_ptr = (page<T, P> *)records[m].ptr;
      }

      sibling->hdr.sibling_ptr = hdr.sibling_ptr;
      clflush((char *)sibling, sizeof(page));

      hdr.sibling_ptr = sibling;
      clflush((char *)&hdr, sizeof(hdr));

      // set to NULL
      if (IS_FORWARD(hdr.switch_counter))
        hdr.switch_counter += 2;
      else
        ++hdr.switch_counter;
      records[m].ptr = NULL;
      clflush((char *)&records[m], sizeof(entry<T, P>));

      hdr.last_index = m - 1;
      clflush((char *)&(hdr.last_index), sizeof(int16_t));

      num_entries = hdr.last_index + 1;

      page<T, P> *ret;

      // insert the key
      if (key < split_key) {
        insert_key(key, right, &num_entries);
        ret = this;
      } else {
        sibling->insert_key(key, right, &sibling_cnt);
        ret = sibling;
      }

      // Set a new root or insert the split key to the parent
      if (bt->root == (char *)this) { // only one node can update the root ptr
        //page *new_root =
        //    new page((page *)this, split_key, sibling, hdr.level + 1);
        page<T, P> *new_root;
        my_alloc::BasePMPool::ZAllocate((void**)&new_root, sizeof(page));
        new (new_root) page((page<T, P> *)this, split_key, sibling, hdr.level + 1);
        bt->setNewRoot((char *)new_root);

        if (with_lock) {
          hdr.mtx->unlock(); // Unlock the write lock
        }
      } else {
        if (with_lock) {
          hdr.mtx->unlock(); // Unlock the write lock
        }
        bt->btree_insert_internal(NULL, split_key, (P)sibling,
                                  hdr.level + 1);
      }

      return ret;
    }
  }

  // Search keys with linear search
  void linear_search_range(T min, T max,
                           unsigned long *buf) {
    int i, off = 0;
    uint8_t previous_switch_counter;
    page<T, P> *current = this;

    while (current) {
      int old_off = off;
      do {
        previous_switch_counter = current->hdr.switch_counter;
        off = old_off;

        T tmp_key;
        P tmp_ptr;

        if (IS_FORWARD(previous_switch_counter)) {
          if ((tmp_key = current->records[0].key) > min) {
            if (tmp_key < max) {
              if ((tmp_ptr = current->records[0].ptr) != NULL) {
                if (tmp_key == current->records[0].key) {
                  if (tmp_ptr) {
                    buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
            } else
              return;
          }

          for (i = 1; current->records[i].ptr != NULL; ++i) {
            if ((tmp_key = current->records[i].key) > min) {
              if (tmp_key < max) {
                if ((tmp_ptr = current->records[i].ptr) !=
                    current->records[i - 1].ptr) {
                  if (tmp_key == current->records[i].key) {
                    if (tmp_ptr)
                      buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              } else
                return;
            }
          }
        } else {
          for (i = count() - 1; i > 0; --i) {
            if ((tmp_key = current->records[i].key) > min) {
              if (tmp_key < max) {
                if ((tmp_ptr = current->records[i].ptr) !=
                    current->records[i - 1].ptr) {
                  if (tmp_key == current->records[i].key) {
                    if (tmp_ptr)
                      buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              } else
                return;
            }
          }

          if ((tmp_key = current->records[0].key) > min) {
            if (tmp_key < max) {
              if ((tmp_ptr = current->records[0].ptr) != NULL) {
                if (tmp_key == current->records[0].key) {
                  if (tmp_ptr) {
                    buf[off++] = (unsigned long)tmp_ptr;
                  }
                }
              }
            } else
              return;
          }
        }
      } while (previous_switch_counter != current->hdr.switch_counter);

      current = current->hdr.sibling_ptr;
    }
  }

  P linear_search(T key) {
    int i = 1;
    uint8_t previous_switch_counter;
    P ret = NULL;
    P t;
    T k;

    if (hdr.leftmost_ptr == NULL) { // Search a leaf node
      do {
        previous_switch_counter = hdr.switch_counter;
        ret = NULL;

        // search from left ro right
        if (IS_FORWARD(previous_switch_counter)) {
          if ((k = records[0].key) == key) {
            if ((t = records[0].ptr) != NULL) {
              if (k == records[0].key) {
                ret = t;
                continue;
              }
            }
          }

          for (i = 1; records[i].ptr != NULL; ++i) {
            if ((k = records[i].key) == key) {
              if (records[i - 1].ptr != (t = records[i].ptr)) {
                if (k == records[i].key) {
                  ret = t;
                  break;
                }
              }
            }
          }
        } else { // search from right to left
          for (i = count() - 1; i > 0; --i) {
            if ((k = records[i].key) == key) {
              if (records[i - 1].ptr != (t = records[i].ptr) && t) {
                if (k == records[i].key) {
                  ret = t;
                  break;
                }
              }
            }
          }

          if (!ret) {
            if ((k = records[0].key) == key) {
              if (NULL != (t = records[0].ptr) && t) {
                if (k == records[0].key) {
                  ret = t;
                  continue;
                }
              }
            }
          }
        }
      } while (hdr.switch_counter != previous_switch_counter);

      if (ret) {
        return ret;
      }

      if ((t = (P)hdr.sibling_ptr) && key >= ((page<T, P> *)t)->records[0].key)
        return t;

      return NULL;
    } else { // internal node
      do {
        previous_switch_counter = hdr.switch_counter;
        ret = NULL;

        if (IS_FORWARD(previous_switch_counter)) {
          if (key < (k = records[0].key)) {
            if ((t = (P)hdr.leftmost_ptr) != records[0].ptr) {
              ret = t;
              continue;
            }
          }

          for (i = 1; records[i].ptr != NULL; ++i) {
            if (key < (k = records[i].key)) {
              if ((t = records[i - 1].ptr) != records[i].ptr) {
                ret = t;
                break;
              }
            }
          }

          if (!ret) {
            ret = records[i - 1].ptr;
            continue;
          }
        } else { // search from right to left
          for (i = count() - 1; i >= 0; --i) {
            if (key >= (k = records[i].key)) {
              if (i == 0) {
                if ((P)hdr.leftmost_ptr != (t = records[i].ptr)) {
                  ret = t;
                  break;
                }
              } else {
                if (records[i - 1].ptr != (t = records[i].ptr)) {
                  ret = t;
                  break;
                }
              }
            }
          }
        }
      } while (hdr.switch_counter != previous_switch_counter);

      if ((t = (P)hdr.sibling_ptr) != NULL) {
        if (key >= ((page<T, P> *)t)->records[0].key)
          return t;
      }

      if (ret) {
        return ret;
      } else
        return (P)hdr.leftmost_ptr;
    }

    return NULL;
  }

  // print a node
  void print() {
    if (hdr.leftmost_ptr == NULL)
      printf("[%d] leaf %x \n", this->hdr.level, this);
    else
      printf("[%d] internal %x \n", this->hdr.level, this);
    printf("last_index: %d\n", hdr.last_index);
    printf("switch_counter: %d\n", hdr.switch_counter);
    printf("search direction: ");
    if (IS_FORWARD(hdr.switch_counter))
      printf("->\n");
    else
      printf("<-\n");

    if (hdr.leftmost_ptr != NULL)
      printf("%x ", hdr.leftmost_ptr);

    for (int i = 0; records[i].ptr != NULL; ++i)
      printf("%ld,%x ", records[i].key, records[i].ptr);

    printf("%x ", hdr.sibling_ptr);

    printf("\n");
  }

  void printAll() {
    if (hdr.leftmost_ptr == NULL) {
      printf("printing leaf node: ");
      print();
    } else {
      printf("printing internal node: ");
      print();
      ((page<T, P> *)hdr.leftmost_ptr)->printAll();
      for (int i = 0; records[i].ptr != NULL; ++i) {
        ((page<T, P> *)records[i].ptr)->printAll();
      }
    }
  }
};

// class page

/*
 * class btree
 */
template<class T, class P>
btree<T, P>::btree() {
  //root = (char *)new page();
  page<T, P> *my_root;
  my_alloc::BasePMPool::ZAllocate((void**)&my_root, sizeof(page<T, P>));
  new (my_root) page<T, P>();
  root = (char*)my_root;
  height = 1;
}

template<class T, class P>
void btree<T, P>::setNewRoot(char *new_root) {
  this->root = (char *)new_root;
  clflush((char *)&(this->root), sizeof(char *));
  ++height;
}

template<class T, class P>
P btree<T, P>::search(const T& key) const {
  page<T, P> *p = (page<T, P> *)root;

  while (p->hdr.leftmost_ptr != NULL) {
    p = (page<T, P> *)p->linear_search(key);
  }

  page<T, P> *t;
  while ((t = (page<T, P> *)p->linear_search(key)) == p->hdr.sibling_ptr) {
    p = t;
    if (!p) {
      break;
    }
  }

  if (!t) {
    printf("NOT FOUND %lu, t = %x\n", key, t);
    return NULL;
  }

  return (P)t;
}

// insert the key in the leaf node
template<class T, class P>
bool btree<T, P>::insert(const T& key, const P& right) { // need to be string
  page<T, P> *p = (page<T, P> *)root;

  while (p->hdr.leftmost_ptr != NULL) {
    p = (page<T, P> *)p->linear_search(key);
  }

  if (!p->store(this, NULL, key, right, true, true)) { // store
    insert(key, right);
  }
  return true;
}

// store the key into the node at the given level
template<class T, class P>
void btree<T, P>::btree_insert_internal(char *left, T key, P right,
                                  uint32_t level) {
  if (level > ((page<T, P> *)root)->hdr.level)
    return;

  page<T, P> *p = (page<T, P> *)this->root;

  while (p->hdr.level > level)
    p = (page<T, P> *)p->linear_search(key);

  if (!p->store(this, NULL, key, right, true, true)) {
    btree_insert_internal(left, key, right, level);
  }
}

template<class T, class P>
void btree<T, P>::btree_delete(T key) {
  page<T, P> *p = (page<T, P> *)root;

  while (p->hdr.leftmost_ptr != NULL) {
    p = (page<T, P> *)p->linear_search(key);
  }

  page<T, P> *t;
  while ((t = (page<T, P> *)p->linear_search(key)) == p->hdr.sibling_ptr) {
    p = t;
    if (!p)
      break;
  }

  if (p) {
    if (!p->remove(this, key)) {
      btree_delete(key);
    }
  } else {
    printf("not found the key to delete %lu\n", key);
  }
}

template<class T, class P>
void btree<T, P>::btree_delete_internal(T key, P ptr, uint32_t level,
                                  T *deleted_key,
                                  bool *is_leftmost_node, page<T, P> **left_sibling) {
  if (level > ((page<T, P> *)this->root)->hdr.level)
    return;

  page<T, P> *p = (page<T, P> *)this->root;

  while (p->hdr.level > level) {
    p = (page<T, P> *)p->linear_search(key);
  }

  p->hdr.mtx->lock();

  if ((P)p->hdr.leftmost_ptr == ptr) {
    *is_leftmost_node = true;
    p->hdr.mtx->unlock();
    return;
  }

  *is_leftmost_node = false;

  for (int i = 0; p->records[i].ptr != NULL; ++i) {
    if (p->records[i].ptr == ptr) {
      if (i == 0) {
        if ((P)p->hdr.leftmost_ptr != p->records[i].ptr) {
          *deleted_key = p->records[i].key;
          *left_sibling = p->hdr.leftmost_ptr;
          p->remove(this, *deleted_key, false, false);
          break;
        }
      } else {
        if (p->records[i - 1].ptr != p->records[i].ptr) {
          *deleted_key = p->records[i].key;
          *left_sibling = (page<T, P> *)p->records[i - 1].ptr;
          p->remove(this, *deleted_key, false, false);
          break;
        }
      }
    }
  }

  p->hdr.mtx->unlock();
}

// Function to search keys from "min" to "max"
template<class T, class P>
void btree<T, P>::btree_search_range(T min, T max,
                               unsigned long *buf) {
  page<T, P> *p = (page<T, P> *)root;

  while (p) {
    if (p->hdr.leftmost_ptr != NULL) {
      // The current page is internal
      p = (page<T, P> *)p->linear_search(min);
    } else {
      // Found a leaf
      p->linear_search_range(min, max, buf);

      break;
    }
  }
}

template<class T, class P>
void btree<T, P>::bulk_load(const V arr[], int num) {
  for (int i = 0; i < num; i++)
  {
    /* code */
    insert(arr[i].first, arr[i].second);
  }
}

template<class T, class P>
void btree<T, P>::printAll() {
  pthread_mutex_lock(&print_mtx);
  int total_keys = 0;
  page<T, P> *leftmost = (page<T, P> *)root;
  printf("root: %x\n", root);
  do {
    page<T, P> *sibling = leftmost;
    while (sibling) {
      if (sibling->hdr.level == 0) {
        total_keys += sibling->hdr.last_index + 1;
      }
      sibling->print();
      sibling = sibling->hdr.sibling_ptr;
    }
    printf("-----------------------------------------\n");
    leftmost = leftmost->hdr.leftmost_ptr;
  } while (leftmost);

  printf("total number of keys: %d\n", total_keys);
  pthread_mutex_unlock(&print_mtx);
}
