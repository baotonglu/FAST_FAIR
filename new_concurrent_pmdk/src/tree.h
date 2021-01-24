#pragma once

#include <map>
//used to define the interface of all benchmarking trees
template <class T, class P>
class Tree {
 public:
  typedef std::pair<T, P> V;
  virtual void bulk_load(const V[], int) = 0;
  virtual bool insert(const T&, const P&) = 0;
  virtual P* search(const T&) const = 0;
  void print_min_max(){

  }

  void get_depth_info(){ 

  }
};