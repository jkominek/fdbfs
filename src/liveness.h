#ifndef _LIVENESS_H_
#define _LIVENESS_H_

extern std::vector<uint8_t> pid; // our unique id for use records

extern void start_liveness(struct fuse_session *);
extern void terminate_liveness();

#endif
