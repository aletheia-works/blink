#ifndef BLINK_EMUFD_H_
#define BLINK_EMUFD_H_
// Emulated file descriptors: eventfd, AF_UNIX stream socketpair, pipes and
// epoll implemented inside blink, without relying on the host kernel.
//
// Hosts such as Emscripten have no eventfd or epoll, and their pipes never
// block. These objects are plain memory guarded by one lock, so guest
// threads (which are host threads) can block on them with condition
// variables. Each emulated descriptor still owns a host descriptor (opened
// on /dev/null) so its number is reserved in the host's table and the usual
// dup(), fcntl(F_SETFL) and close() paths keep working.
//
// Enabled by default on Emscripten. Define HAVE_EMUFD to use it on other
// hosts (for testing), or DISABLE_EMUFD to turn it off.
#include <stdbool.h>
#include <sys/socket.h>
#include <sys/uio.h>

#include "blink/fds.h"
#include "blink/linux.h"
#include "blink/machine.h"
#include "blink/types.h"

#if defined(__EMSCRIPTEN__) && !defined(DISABLE_EMUFD) && !defined(HAVE_EMUFD)
#define HAVE_EMUFD
#endif

#ifdef HAVE_EMUFD

extern const struct FdCb kFdCbEmu;

bool EmuIsFd(int);
bool EmuIsSocket(int);
void EmuDupFd(int, int);

int EmuEventfd(struct Machine *, u32, i32);
int EmuPipe(struct Machine *, int[2], i32);
int EmuSocketpair(struct Machine *, i32, i32, int[2]);

ssize_t EmuSendmsg(int, const struct msghdr *, int);
ssize_t EmuRecvmsg(int, struct msghdr *, int);
int EmuShutdown(int, int);
int EmuGetsockopt(int, int, int, void *, socklen_t *);
int EmuSetsockopt(int, int, int, const void *, socklen_t);
int EmuGetsockname(int, struct sockaddr *, socklen_t *);

int EmuEpollCreate(struct Machine *, i32);
int EmuEpollCtl(struct Machine *, i32, i32, i32, u32, u64);
int EmuEpollWait(struct Machine *, i32, struct epoll_event_linux *, int,
                 struct timespec);

#endif /* HAVE_EMUFD */
#endif /* BLINK_EMUFD_H_ */
