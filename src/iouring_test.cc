#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <liburing.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <vector>
#include <fstream>
#include <omp.h>
#include <array>

#define BUFFER_SIZE 4096
#define NUM_RING 4
#define RING_LEN 1024

uint64_t time_diff(struct timespec *start, struct timespec *end) {
  return (end->tv_sec - start->tv_sec) * 1000000000 +
         (end->tv_nsec - start->tv_nsec);
}

struct IOUringState {
  std::array<io_uring, NUM_RING> ring_arr;

  static IOUringState *Global() {
    static IOUringState state;
    return &state;
  }
};

void Init_iouring() {
  auto* uring_state = IOUringState::Global();
  auto& ring_arr = uring_state->ring_arr;

  for (int i = 0; i < NUM_RING; i++) {
    int ret = io_uring_queue_init(RING_LEN, &ring_arr[i], 0);
    if (ret) {
      std::cout << "Unable to setup io_uring: " << strerror(-ret);
    }
  }
}

void Exit_iouring() {
  auto* uring_state = IOUringState::Global();

  for (int i = 0; i < NUM_RING; i++) {
    io_uring_queue_exit(&uring_state->ring_arr[i]);
  }
}

struct IoContext {
  int fd;
  char *buffer;
  struct io_uring ring;
};

void io_uring(const std::string &file_path, int thread_num) {
  int fd = open(file_path.c_str(), O_RDONLY);
  if (fd == -1) {
    perror("open");
    return;
  }

  struct IoContext ioContext;
  ioContext.fd = fd;
  char *buffer = (char *)malloc(BUFFER_SIZE);

  if (io_uring_queue_init(thread_num, &ioContext.ring, 0) < 0) {
    perror("io_uring_queue_init");
    close(fd);
    free(buffer);
    return;
  }

  off_t file_size = lseek(fd, 0, SEEK_END);
  off_t offset = 0;
  char *buf = (char *)aligned_alloc(BUFFER_SIZE, file_size);

  struct timespec start, end;
  clock_gettime(CLOCK_MONOTONIC_RAW, &start);

  while (offset < file_size) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ioContext.ring);
    if (!sqe) {
      perror("io_uring_get_sqe");
      break;
    }

    // Set up read operation
    io_uring_prep_read(sqe, fd, buf + offset, BUFFER_SIZE, offset);
    io_uring_sqe_set_data(sqe, (void *)(buf + offset));

    // Set up callback
    io_uring_sqe_set_flags(sqe, IOSQE_IO_LINK);
    io_uring_sqe_set_data(sqe, (void *)(buf + offset));

    // Submit the read operation
    io_uring_submit(&ioContext.ring);

    offset += BUFFER_SIZE;
  }

  // Wait for completions
  for (off_t i = 0; i < file_size / BUFFER_SIZE; ++i) {
    struct io_uring_cqe *cqe;
    if (io_uring_wait_cqe(&ioContext.ring, &cqe) < 0) {
      perror("io_uring_wait_cqe");
      break;
    }

    // Handle completion
    io_uring_cqe_seen(&ioContext.ring, cqe);
  }
  clock_gettime(CLOCK_MONOTONIC_RAW, &end);
  auto duration = time_diff(&start, &end) / 1000000.0;
  auto bandwidth = (file_size / (1024.0 * 1024.0 * 1024.0)) / (duration / 1000);

  uint64_t *array = (uint64_t *)buf;
  uint64_t sum = 0;
  for (int i = 0; i < file_size / sizeof(uint64_t); ++i) {
    sum ^= array[i];
  }
  printf("%lu, %.3fms, %.3fGB/s\n", sum, duration, bandwidth);
  // Clean up resources
  close(fd);
  io_uring_queue_exit(&ioContext.ring);
  free(buffer);
}

void test_iouring_randread(const std::string &file_path, int thread_num,
                         uint32_t block_size) {
  std::ifstream is(file_path, std::ifstream::binary | std::ifstream::ate);
  std::size_t file_size = is.tellg();
  is.close();

  int fd = open(file_path.c_str(), O_RDONLY | O_DIRECT);

  int num_read = 1000000;
  size_t buf_size = block_size * num_read;
  char *buf = (char *)aligned_alloc(block_size, buf_size);
  // std::cout << block_size / 1024 << "KB " << std::endl;
  auto num_blocks = file_size / block_size;
  int offset[num_read];
  for (int i = 0; i < num_read; i++) {
    offset[i] = rand() % num_blocks;
  }

  Init_iouring();
  auto* uring_state = IOUringState::Global();
  auto& ring_arr = uring_state->ring_arr;

  struct timespec start, end;
  clock_gettime(CLOCK_MONOTONIC_RAW, &start);

#pragma omp parallel for num_threads(NUM_RING)
  for (int i = 0; i < num_read; i += RING_LEN) {
    int thd_id = omp_get_thread_num();
    auto& ring = ring_arr[thd_id];

    int r = std::min(num_read, i + RING_LEN);
    struct io_uring_sqe* sqe;
    for (int j = i; j < r; j++) {
      sqe = io_uring_get_sqe(&ring);
      if (!sqe) {
        std::cout << "Could not get SQE."
                   << " i: " << i << " j: " << j;
      }
      io_uring_prep_read(sqe, fd, buf + j * block_size,
                         block_size, offset[j] * block_size);
      if ((j + 1) % 64 == 0 || j == r - 1) {
        io_uring_submit(&ring);
      }
    }

    int finish_num = 0;
    while (finish_num < r - i) {
      struct io_uring_cqe* cqe;
      int ret = io_uring_wait_cqe(&ring, &cqe);
      if (ret < 0) {
        std::cout << "Error waiting for completion: " << strerror(-ret);
      }
      struct io_uring_cqe* cqes[RING_LEN];
      int cqecount = io_uring_peek_batch_cqe(&ring, cqes, RING_LEN);
      if (cqecount == -1) {
        std::cout << "Error peeking batch data";
      }
      io_uring_cq_advance(&ring, cqecount);
      finish_num += cqecount;
    }
  }

  clock_gettime(CLOCK_MONOTONIC_RAW, &end);
  Exit_iouring();
  auto duration = time_diff(&start, &end) / 1000000.0;
  uint64_t bandwidth = (buf_size / (1024 * 1024)) / (duration / 1000);
  uint64_t *array = (uint64_t *)buf;
  uint64_t sum = 0;
  for (int i = 0; i < buf_size / sizeof(uint64_t); ++i) {
    sum ^= array[i];
  }
  printf("%lu, %.3fms, %luMB/s\n", sum, duration, bandwidth);
  free(buf);
  close(fd);
}

int main(int argc, char *argv[]) {
  if (argc != 5) {
    std::cerr << "Usage: " << argv[0]
              << "<filename> <is_random>  <thread_num> <block_size KB>"
              << std::endl;
  }
  const std::string filename = argv[1];
  int rand = atoi(argv[2]);
  int t = atoi(argv[3]);
  int block = atoi(argv[4]) * 1024;
  if (rand) {
    test_iouring_randread(filename, t, block);
  } else {
    std::cout << "Not implemented\n";
  }

  return 0;
}