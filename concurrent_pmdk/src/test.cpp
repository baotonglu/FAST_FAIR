#include "btree.h"
#include "random.h"

/*
 *  *file_exists -- checks if file exists
 *   */
static inline int file_exists(char const *file) { return access(file, F_OK); }

void clear_cache() {
  // Remove cache
  int size = 256 * 1024 * 1024;
  char *garbage = new char[size];
  for (int i = 0; i < size; ++i)
    garbage[i] = i;
  for (int i = 100; i < size; ++i)
    garbage[i] += garbage[i - 100];
  delete[] garbage;
}

size_t pool_size = 10UL*1024*1024*1024;

// MAIN
int main(int argc, char **argv) {
  // Parsing arguments
  int numData = 0;
  int n_threads = 1;
  char *persistent_path = "/mnt/pmem0/baotong/fast-fair.data";

  int c;
  while ((c = getopt(argc, argv, "n:w:t:i:p:")) != -1) {
    switch (c) {
    case 'n':
      numData = atoi(optarg);
      break;
    case 't':
      n_threads = atoi(optarg);
      break;
    }
  }

  // Make or Read persistent pool
  TOID(btree) bt = TOID_NULL(btree);
  PMEMobjpool *pop;

  if (file_exists(persistent_path) != 0) {
    pop = pmemobj_create(persistent_path, "btree", pool_size,
                         0666); // make memory pool
    bt = POBJ_ROOT(pop, btree);
    D_RW(bt)->constructor(pop);
  } else {
    pop = pmemobj_open(persistent_path, "btree");
    bt = POBJ_ROOT(pop, btree);
  }

  struct timespec start, end, tmp;

  // Reading data
  entry_key_t *keys = new entry_key_t[numData];

  /*
  ifstream ifs;
  ifs.open(input_path);

  if (!ifs) {
    cout << "input loading error!" << endl;
  }

  for (int i = 0; i < numData; ++i) {
    ifs >> keys[i];
  }
  ifs.close();
  */

  // generate random keys
  unsigned long long init[4]={0x12345ULL, 0x23456ULL, 0x34567ULL, 0x45678ULL}, length=4;
  init_by_array64(init, length);

  for(int i = 0; i < numData; ++i){
    keys[i] = genrand64_int64();
  }

  clock_gettime(CLOCK_MONOTONIC, &start);

  long half_num_data = numData / 2;

  // Warm-up! Insert half of input size
  for (int i = 0; i < half_num_data; ++i) {
    D_RW(bt)->btree_insert(keys[i], (char *)keys[i]);
  }
  cout << "Warm-up!" << endl;

  clock_gettime(CLOCK_MONOTONIC, &end);
  long long elapsedTime =
      (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
  clear_cache();

  // Multithreading
  vector<future<void>> futures(n_threads);

  long data_per_thread = half_num_data / n_threads;

#ifndef MIXED
  // Search
  clock_gettime(CLOCK_MONOTONIC, &start);

  for (int tid = 0; tid < n_threads; tid++) {
    int from = data_per_thread * tid;
    int to = (tid == n_threads - 1) ? half_num_data : from + data_per_thread;

    auto f = async(launch::async,
                   [&bt, &keys](int from, int to) {
                     for (int i = from; i < to; ++i)
                       D_RW(bt)->btree_search(keys[i]);
                   },
                   from, to);
    futures.push_back(move(f));
  }
  for (auto &&f : futures)
    if (f.valid())
      f.get();

  clock_gettime(CLOCK_MONOTONIC, &end);
  elapsedTime =
      (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
  cout << "Concurrent searching with " << n_threads
       << " threads (usec) : " << elapsedTime / 1000 << endl;

  clear_cache();
  futures.clear();

  // Insert
  clock_gettime(CLOCK_MONOTONIC, &start);

  for (int tid = 0; tid < n_threads; tid++) {
    int from = half_num_data + data_per_thread * tid;
    int to = (tid == n_threads - 1) ? numData : from + data_per_thread;

    auto f = async(launch::async,
                   [&bt, &keys](int from, int to) {
                     for (int i = from; i < to; ++i)
                       D_RW(bt)->btree_insert(keys[i], (char *)keys[i]);
                   },
                   from, to);
    futures.push_back(move(f));
  }
  for (auto &&f : futures)
    if (f.valid())
      f.get();

  clock_gettime(CLOCK_MONOTONIC, &end);
  elapsedTime =
      (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
  cout << "Concurrent inserting with " << n_threads
       << " threads (usec) : " << elapsedTime / 1000 << endl;
#else
  clock_gettime(CLOCK_MONOTONIC, &start);

  for (int tid = 0; tid < n_threads; tid++) {
    int from = half_num_data + data_per_thread * tid;
    int to = (tid == n_threads - 1) ? numData : from + data_per_thread;

    auto f = async(launch::async,
                   [&bt, &keys, &half_num_data](int from, int to) {
                     for (int i = from; i < to; ++i) {
                       int sidx = i - half_num_data;

                       int jid = i % 4;
                       switch (jid) {
                       case 0:
                         D_RW(bt)->btree_insert(keys[i], (char *)keys[i]);
                         for (int j = 0; j < 4; j++)
                           D_RW(bt)->btree_search(
                               keys[(sidx + j + jid * 8) % half_num_data]);
                         D_RW(bt)->btree_delete(keys[i]);
                         break;

                       case 1:
                         for (int j = 0; j < 3; j++)
                           D_RW(bt)->btree_search(
                               keys[(sidx + j + jid * 8) % half_num_data]);
                         D_RW(bt)->btree_insert(keys[i], (char *)keys[i]);
                         D_RW(bt)->btree_search(
                             keys[(sidx + 3 + jid * 8) % half_num_data]);
                         break;
                       case 2:
                         for (int j = 0; j < 2; j++)
                           D_RW(bt)->btree_search(
                               keys[(sidx + j + jid * 8) % half_num_data]);
                         D_RW(bt)->btree_insert(keys[i], (char *)keys[i]);
                         for (int j = 2; j < 4; j++)
                           D_RW(bt)->btree_search(
                               keys[(sidx + j + jid * 8) % half_num_data]);
                         break;
                       case 3:
                         for (int j = 0; j < 4; j++)
                           D_RW(bt)->btree_search(
                               keys[(sidx + j + jid * 8) % half_num_data]);
                         D_RW(bt)->btree_insert(keys[i], (char *)keys[i]);
                         break;
                       default:
                         break;
                       }
                     }
                   },
                   from, to);
    futures.push_back(move(f));
  }

  for (auto &&f : futures)
    if (f.valid())
      f.get();

  clock_gettime(CLOCK_MONOTONIC, &end);
  elapsedTime =
      (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
  cout << "Concurrent inserting and searching with " << n_threads
       << " threads (usec) : " << elapsedTime / 1000 << endl;
#endif

  delete[] keys;

  pmemobj_close(pop);
  return 0;
}
