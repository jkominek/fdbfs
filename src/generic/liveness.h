#ifndef _LIVENESS_H_
#define _LIVENESS_H_

#include <vector>

enum class PidLiveness { Alive, Dead, Unknown };

extern std::vector<uint8_t> pid; // our unique id for use records

extern bool start_liveness(struct fuse_session *);
extern void terminate_liveness();
extern PidLiveness classify_pid(const std::vector<uint8_t> &);

#endif
