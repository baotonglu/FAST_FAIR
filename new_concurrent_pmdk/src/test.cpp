#include "btree.h"
#include "random.h"

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

// MAIN
int main(int argc, char **argv) {
  // Parsing arguments
  int numData = 0;
  int n_threads = 1;
  //char *input_path = (char *)std::string("../sample_input.txt").data();
  //intialize the memory pool
  my_alloc::BasePMPool::Initialize(pool_name, pool_size);
  int c;
  while ((c = getopt(argc, argv, "n:w:t:i:")) != -1) {
    switch (c) {
    case 'n':
      numData = atoi(optarg);
      break;
    case 't':
      n_threads = atoi(optarg);
    default:
      break;
    }
  }

  btree<int64_t> *bt;
  bt = reinterpret_cast<btree<int64_t>*>(my_alloc::BasePMPool::GetRoot(sizeof(btree<int64_t>)));
  new (bt) btree<int64_t>();

  struct timespec start, end, tmp;

  // Reading data
  int64_t *keys = new int64_t[numData];

  unsigned long long init[4]={0x12345ULL, 0x23456ULL, 0x34567ULL, 0x45678ULL}, length=4;
  init_by_array64(init, length);

  for(int i = 0; i < numData; ++i){
    keys[i] = genrand64_int64();
  }

  // Initializing stats
  clflush_cnt = 0;
  search_time_in_insert = 0;
  clflush_time_in_insert = 0;
  gettime_cnt = 0;

  clock_gettime(CLOCK_MONOTONIC, &start);

  long half_num_data = numData / 2;

  // Warm-up! Insert half of input size
  for (int i = 0; i < half_num_data; ++i) {
    bt->btree_insert(keys[i], (char *)keys[i]);
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
                       bt->btree_search(keys[i]);
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
  cout << "Throughput = " << (double)half_num_data / ((double)elapsedTime / (1000UL*1000*1000)) << "Mops/s" << std::endl;

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
                       bt->btree_insert(keys[i], (char *)keys[i]);
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
  cout << "Throughput = " << (double)half_num_data / ((double)elapsedTime / (1000UL*1000*1000)) << "Mops/s" << std::endl;

#else
  clock_gettime(CLOCK_MONOTONIC, &start);

  for (int tid = 0; tid < n_threads; tid++) {
    int from = half_num_data + data_per_thread * tid;
    int to = (tid == n_threads - 1) ? numData : from + data_per_thread;

    auto f = async(
        launch::async,
        [&bt, &keys, &half_num_data](int from, int to) {
          for (int i = from; i < to; ++i) {
            int sidx = i - half_num_data;

            int jid = i % 4;
            switch (jid) {
            case 0:
              bt->btree_insert(keys[i], (char *)keys[i]);
              for (int j = 0; j < 4; j++)
                bt->btree_search(keys[(sidx + j + jid * 8) % half_num_data]);
              bt->btree_delete(keys[i]);
              break;
            case 1:
              for (int j = 0; j < 3; j++)
                bt->btree_search(keys[(sidx + j + jid * 8) % half_num_data]);
              bt->btree_insert(keys[i], (char *)keys[i]);
              bt->btree_search(keys[(sidx + 3 + jid * 8) % half_num_data]);
              break;
            case 2:
              for (int j = 0; j < 2; j++)
                bt->btree_search(keys[(sidx + j + jid * 8) % half_num_data]);
              bt->btree_insert(keys[i], (char *)keys[i]);
              for (int j = 2; j < 4; j++)
                bt->btree_search(keys[(sidx + j + jid * 8) % half_num_data]);
              break;
            case 3:
              for (int j = 0; j < 4; j++)
                bt->btree_search(keys[(sidx + j + jid * 8) % half_num_data]);
              bt->btree_insert(keys[i], (char *)keys[i]);
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
  cout << "Throughput = " << (double)half_num_data / ((double)elapsedTime / (1000UL*1000*1000)) << "Mops/s" << std::endl;
#endif

  //delete bt;
  delete[] keys;

  return 0;
}
