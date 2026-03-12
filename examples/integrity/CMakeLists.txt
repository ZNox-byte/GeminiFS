cmake_minimum_required (VERSION 3.1)
project (libnvm-samples)

set (THREADS_PREFER_PTHREAD_FLAG ON)
find_package (Threads REQUIRED)

make_sample (integrity-smartio integrity-util "integrity.cpp;transfer.cpp;util.cpp;read.cpp")
set_multithread (integrity-smartio)


make_sample (integrity-module integrity-util "integrity.cpp;transfer.cpp;util.cpp;read.cpp")
set_multithread (integrity-module)


make_samples_choice (integrity-util integrity-smartio integrity-module)
add_custom_target (integrity DEPENDS integrity-util)


make_sample (test-smartio test-bench "test.cpp;transfer.cpp;util.cpp;read.cpp")
set_multithread (test-smartio)

make_sample (test-module test-bench "test.cpp;transfer.cpp;util.cpp;read.cpp")
set_multithread (test-module)

make_samples_choice (test-bench test-smartio test-module)
add_custom_target (test_bench DEPENDS test-bench)
