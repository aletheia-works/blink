/*-*- mode:c;indent-tabs-mode:nil;c-basic-offset:2;tab-width:8;coding:utf-8 -*-│
│ vi: set et ft=c ts=2 sts=2 sw=2 fenc=utf-8                               :vi │
╞══════════════════════════════════════════════════════════════════════════════╡
│ Copyright 2026 aletheia-works                                                │
│                                                                              │
│ Permission to use, copy, modify, and/or distribute this software for         │
│ any purpose with or without fee is hereby granted, provided that the         │
│ above copyright notice and this permission notice appear in all copies.      │
│                                                                              │
│ THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL                │
│ WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED                │
│ WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE             │
│ AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL         │
│ DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR        │
│ PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER               │
│ TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR             │
│ PERFORMANCE OF THIS SOFTWARE.                                                │
╚─────────────────────────────────────────────────────────────────────────────*/
#include "blink/emufd.h"

#ifdef HAVE_EMUFD
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include "blink/assert.h"
#include "blink/atomic.h"
#include "blink/endian.h"
#include "blink/errno.h"
#include "blink/log.h"
#include "blink/macros.h"
#include "blink/ndelay.h"
#include "blink/syscall.h"
#include "blink/thread.h"
#include "blink/timespec.h"
#include "blink/tunables.h"
#include "blink/vfs.h"

#define kEmuMaxFds         4096
#define kEmuSocketCapacity (256 * 1024)  // close to Linux's default rmem
#define kEmuPipeCapacity   (64 * 1024)   // Linux's default pipe size
#define kEmuEventfdMax     UINT64_C(0xfffffffffffffffe)
#define kEmuHostPollMs     10  // how often epoll rechecks host descriptors

#define EPOLL_LEVEL_BITS_LINUX                                       \
  (EPOLLIN_LINUX | EPOLLPRI_LINUX | EPOLLOUT_LINUX | EPOLLRDNORM_LINUX | \
   EPOLLRDBAND_LINUX | EPOLLWRNORM_LINUX | EPOLLWRBAND_LINUX |           \
   EPOLLMSG_LINUX | EPOLLRDHUP_LINUX)

enum EmuKind {
  kEmuEventfd = 1,
  kEmuStream,
  kEmuEpoll,
};

struct EmuObj;

// One direction of a pipe or socket pair: a ring buffer shared by the
// endpoint that writes it and the endpoint that reads it.
struct EmuBuf {
  u8 *data;
  size_t cap;
  size_t head;
  size_t len;
  bool eof;     // nothing more will be written (writer gone or shut down)
  bool broken;  // nobody will read (reader gone or shut down)
  int users;    // endpoints still referring to this buffer
  struct EmuObj *reader;
  struct EmuObj *writer;
};

struct EmuWatch {
  int fd;                 // guest descriptor as registered
  struct EmuObj *target;  // emulated object, or null for a host descriptor
  u32 events;             // EPOLL*_LINUX requested
  u64 data;               // returned verbatim
  u64 seen;               // target generation last reported (EPOLLET)
  u32 lastready;          // host readiness last reported (EPOLLET)
  bool disabled;          // EPOLLONESHOT already fired
};

struct EmuObj {
  int kind;
  int refs;  // host descriptors mapped to this object
  u64 gen;   // bumped whenever readiness may have changed
  // eventfd
  u64 count;
  bool semaphore;
  // stream (pipe end or socket end)
  bool socket;
  struct EmuBuf *in;
  struct EmuBuf *out;
  // epoll
  struct EmuWatch *watches;
  int nwatches;
  int capwatches;
};

static struct Emu {
  pthread_once_t_ once;
  pthread_mutex_t_ lock;
  pthread_cond_t_ cond;
  struct EmuObj *fds[kEmuMaxFds];
} g_emu = {PTHREAD_ONCE_INIT_};

static long enotsock(void) {
  errno = ENOTSOCK;
  return -1;
}

static long enoprotoopt(void) {
  errno = ENOPROTOOPT;
  return -1;
}

static void EmuInit(void) {
  unassert(!pthread_mutex_init(&g_emu.lock, 0));
  unassert(!pthread_cond_init(&g_emu.cond, 0));
}

static void EmuLock(void) {
  pthread_once_(&g_emu.once, EmuInit);
  LOCK(&g_emu.lock);
}

static void EmuUnlock(void) {
  UNLOCK(&g_emu.lock);
}

static void EmuNotify(void) {
  unassert(!pthread_cond_broadcast(&g_emu.cond));
}

static struct EmuObj *EmuGet(int fildes) {
  if (0 <= fildes && fildes < kEmuMaxFds) return g_emu.fds[fildes];
  return 0;
}

static void EmuTouch(struct EmuObj *o) {
  if (o) ++o->gen;
}

static void EmuTouchBuf(struct EmuBuf *b) {
  EmuTouch(b->reader);
  EmuTouch(b->writer);
}

// Blocks the calling thread until another thread changes some emulated
// object, a polling tick passes, or `deadline` passes. The lock must be
// held. Returns 1 when the deadline has passed, 0 to recheck, and -1 with
// EINTR when a guest signal is pending. A thread that's being killed by
// exit_group() exits here, since retrying would never end.
static int EmuWait(struct timespec deadline, int tickms) {
  struct timespec now, tick;
  struct Machine *m = g_machine;
  if (m && atomic_load_explicit(&m->killed, memory_order_acquire)) {
    EmuUnlock();
    SysExit(m, 0);
  }
  if (m && (m->signals & ~m->sigmask)) {
    errno = EINTR;
    return -1;
  }
  now = GetTime();
  if (CompareTime(now, deadline) >= 0) return 1;
  tick = AddTime(now, FromMilliseconds(tickms));
  if (CompareTime(tick, deadline) > 0) tick = deadline;
  pthread_cond_timedwait(&g_emu.cond, &g_emu.lock, &tick);
  return 0;
}

static bool EmuIsNonblocking(int fildes) {
  int fl;
  fl = VfsFcntl(fildes, F_GETFL, 0);
  return fl != -1 && (fl & O_NONBLOCK);
}

////////////////////////////////////////////////////////////////////////////////
// object lifetime

static struct EmuBuf *EmuNewBuf(size_t cap) {
  struct EmuBuf *b;
  if (!(b = (struct EmuBuf *)calloc(1, sizeof(*b)))) return 0;
  if (!(b->data = (u8 *)malloc(cap))) {
    free(b);
    return 0;
  }
  b->cap = cap;
  return b;
}

static void EmuDropBuf(struct EmuBuf *b) {
  if (b && !--b->users) {
    free(b->data);
    free(b);
  }
}

static struct EmuObj *EmuNewObj(int kind) {
  struct EmuObj *o;
  if ((o = (struct EmuObj *)calloc(1, sizeof(*o)))) {
    o->kind = kind;
  }
  return o;
}

// Forgets every epoll registration that refers to `o`, so no epoll
// instance keeps a pointer to an object that's about to be freed.
static void EmuUnwatchEverywhere(struct EmuObj *o) {
  int i, j, k;
  struct EmuObj *ep;
  for (i = 0; i < kEmuMaxFds; ++i) {
    if ((ep = g_emu.fds[i]) && ep->kind == kEmuEpoll) {
      for (k = j = 0; j < ep->nwatches; ++j) {
        if (ep->watches[j].target != o) {
          ep->watches[k++] = ep->watches[j];
        }
      }
      ep->nwatches = k;
    }
  }
}

static void EmuDestroy(struct EmuObj *o) {
  if (o->in) {
    o->in->broken = true;
    o->in->reader = 0;
    EmuTouchBuf(o->in);
    EmuDropBuf(o->in);
  }
  if (o->out) {
    o->out->eof = true;
    o->out->writer = 0;
    EmuTouchBuf(o->out);
    EmuDropBuf(o->out);
  }
  EmuUnwatchEverywhere(o);
  free(o->watches);
  free(o);
  EmuNotify();
}

static void EmuRelease(struct EmuObj *o) {
  if (!--o->refs) EmuDestroy(o);
}

static int EmuOpenHostFd(struct Machine *m, bool nonblock) {
  int lim, fildes;
  if (!(lim = GetFileDescriptorLimit(m->system))) return emfile();
  if ((fildes = VfsOpen(AT_FDCWD, "/dev/null",
                        O_RDWR | (nonblock ? O_NONBLOCK : 0), 0)) == -1) {
    return -1;
  }
  if (fildes >= lim || fildes >= kEmuMaxFds) {
    VfsClose(fildes);
    return emfile();
  }
  return fildes;
}

// Reserves a host descriptor for `o`, maps it, and adds the guest Fd. On
// failure `o` is left exactly as it was (unmapped, same reference count),
// so the caller decides whether to discard it.
static int EmuInstall(struct Machine *m, struct EmuObj *o, bool cloexec,
                      bool nonblock, bool socket) {
  int fildes, oflags;
  struct Fd *fd;
  if ((fildes = EmuOpenHostFd(m, nonblock)) == -1) return -1;
  EmuLock();
  g_emu.fds[fildes] = o;
  ++o->refs;
  EmuUnlock();
  oflags = O_RDWR | (cloexec ? O_CLOEXEC : 0) | (nonblock ? O_NDELAY : 0);
  LOCK(&m->system->fds.lock);
  if ((fd = AddFd(&m->system->fds, fildes, oflags))) {
    fd->cb = &kFdCbEmu;
    if (socket) fd->socktype = SOCK_STREAM;
  }
  UNLOCK(&m->system->fds.lock);
  if (!fd) {
    EmuLock();
    g_emu.fds[fildes] = 0;
    --o->refs;
    EmuUnlock();
    VfsClose(fildes);
    return enomem();
  }
  return fildes;
}

// Frees an object that no descriptor refers to (after a failed install).
static void EmuDiscard(struct EmuObj *o) {
  if (o && !o->refs) {
    EmuLock();
    EmuDestroy(o);
    EmuUnlock();
  }
}

bool EmuIsFd(int fildes) {
  bool res;
  EmuLock();
  res = !!EmuGet(fildes);
  EmuUnlock();
  return res;
}

bool EmuIsSocket(int fildes) {
  bool res;
  struct EmuObj *o;
  EmuLock();
  res = (o = EmuGet(fildes)) && o->kind == kEmuStream && o->socket;
  EmuUnlock();
  return res;
}

void EmuDupFd(int oldfildes, int newfildes) {
  struct EmuObj *o;
  EmuLock();
  if ((o = EmuGet(oldfildes)) && 0 <= newfildes && newfildes < kEmuMaxFds) {
    if (g_emu.fds[newfildes]) EmuRelease(g_emu.fds[newfildes]);
    g_emu.fds[newfildes] = o;
    ++o->refs;
  }
  EmuUnlock();
}

static int EmuClose(int fildes) {
  struct EmuObj *o;
  EmuLock();
  if ((o = EmuGet(fildes))) {
    g_emu.fds[fildes] = 0;
    EmuRelease(o);
  }
  EmuUnlock();
  return VfsClose(fildes);
}

////////////////////////////////////////////////////////////////////////////////
// readiness

// Returns EPOLL*_LINUX bits describing what `o` is ready for. Never looks
// into the objects an epoll instance watches, so it can't recurse.
static u32 EmuReadiness(struct EmuObj *o);

static u32 EmuStreamReadiness(struct EmuObj *o) {
  u32 r = 0;
  bool rdone, wdone;
  if (o->in) {
    if (o->in->len) r |= EPOLLIN_LINUX | EPOLLRDNORM_LINUX;
    if (o->in->eof) {
      if (o->socket) {
        r |= EPOLLIN_LINUX | EPOLLRDNORM_LINUX | EPOLLRDHUP_LINUX;
      } else {
        r |= EPOLLHUP_LINUX;
      }
    }
  }
  if (o->out) {
    if (o->out->broken) {
      r |= EPOLLERR_LINUX;
      if (o->socket) r |= EPOLLOUT_LINUX | EPOLLWRNORM_LINUX;
    } else if (o->out->len < o->out->cap && !o->out->eof) {
      r |= EPOLLOUT_LINUX | EPOLLWRNORM_LINUX;
    }
  }
  if (o->socket) {
    rdone = !o->in || o->in->eof;
    wdone = !o->out || o->out->broken || o->out->eof;
    if (rdone && wdone) r |= EPOLLHUP_LINUX;
  }
  return r;
}

static u32 EmuWatchMask(const struct EmuWatch *w) {
  return (w->events & EPOLL_LEVEL_BITS_LINUX) | EPOLLERR_LINUX | EPOLLHUP_LINUX;
}

static u32 EmuHostReadiness(int fildes) {
  u32 r = 0;
  struct pollfd pfd;
  pfd.fd = fildes;
  pfd.events = POLLIN | POLLOUT | POLLPRI;
  pfd.revents = 0;
  if (VfsPoll(&pfd, 1, 0) == -1) return EPOLLNVAL_LINUX;
  if (pfd.revents & POLLNVAL) return EPOLLNVAL_LINUX;
  if (pfd.revents & POLLIN) r |= EPOLLIN_LINUX | EPOLLRDNORM_LINUX;
  if (pfd.revents & POLLPRI) r |= EPOLLPRI_LINUX;
  if (pfd.revents & POLLOUT) r |= EPOLLOUT_LINUX | EPOLLWRNORM_LINUX;
  if (pfd.revents & POLLERR) r |= EPOLLERR_LINUX;
  if (pfd.revents & POLLHUP) r |= EPOLLHUP_LINUX;
  return r;
}

// Returns the events `w` would report now, without changing its state.
static u32 EmuWatchReady(struct EmuWatch *w) {
  u32 r;
  if (w->disabled) return 0;
  if (w->target) {
    r = EmuReadiness(w->target) & EmuWatchMask(w);
    if (r && (w->events & EPOLLET_LINUX) && w->target->gen == w->seen) r = 0;
  } else {
    r = EmuHostReadiness(w->fd);
    if (r & EPOLLNVAL_LINUX) return 0;
    r &= EmuWatchMask(w);
    if (r && (w->events & EPOLLET_LINUX) && r == w->lastready) r = 0;
  }
  return r;
}

static u32 EmuReadiness(struct EmuObj *o) {
  int i;
  switch (o->kind) {
    case kEmuEventfd:
      return (o->count ? EPOLLIN_LINUX | EPOLLRDNORM_LINUX : 0) |
             (o->count < kEmuEventfdMax ? EPOLLOUT_LINUX | EPOLLWRNORM_LINUX
                                        : 0);
    case kEmuStream:
      return EmuStreamReadiness(o);
    case kEmuEpoll:
      for (i = 0; i < o->nwatches; ++i) {
        if (o->watches[i].target && o->watches[i].target->kind == kEmuEpoll) {
          continue;  // nested epoll readiness isn't propagated
        }
        if (EmuWatchReady(o->watches + i)) {
          return EPOLLIN_LINUX | EPOLLRDNORM_LINUX;
        }
      }
      return 0;
    default:
      __builtin_unreachable();
  }
}

static short EmuToPollEvents(u32 r) {
  short ev = 0;
  if (r & EPOLLIN_LINUX) ev |= POLLIN;
  if (r & EPOLLPRI_LINUX) ev |= POLLPRI;
  if (r & EPOLLOUT_LINUX) ev |= POLLOUT;
  if (r & EPOLLERR_LINUX) ev |= POLLERR;
  if (r & EPOLLHUP_LINUX) ev |= POLLHUP;
  return ev;
}

static int EmuPoll(struct pollfd *fds, nfds_t nfds, int timeout) {
  int n, rc;
  nfds_t i;
  struct EmuObj *o;
  struct timespec deadline;
  deadline = timeout < 0 ? GetMaxTime()
                         : AddTime(GetTime(), FromMilliseconds(timeout));
  EmuLock();
  for (;;) {
    for (n = 0, i = 0; i < nfds; ++i) {
      if ((o = EmuGet(fds[i].fd))) {
        fds[i].revents = EmuToPollEvents(EmuReadiness(o)) &
                         (fds[i].events | POLLERR | POLLHUP);
      } else {
        fds[i].revents = POLLNVAL;
      }
      if (fds[i].revents) ++n;
    }
    if (n) break;
    if ((rc = EmuWait(deadline, kPollingMs))) {
      if (rc == -1) n = -1;
      break;
    }
  }
  EmuUnlock();
  return n;
}

////////////////////////////////////////////////////////////////////////////////
// eventfd

static size_t EmuIovLen(const struct iovec *iov, int iovcnt) {
  int i;
  size_t n;
  for (n = i = 0; i < iovcnt; ++i) n += iov[i].iov_len;
  return n;
}

static void EmuScatter(const struct iovec *iov, int iovcnt, const u8 *p,
                       size_t n) {
  int i;
  size_t k;
  for (i = 0; n && i < iovcnt; ++i) {
    k = MIN(n, iov[i].iov_len);
    memcpy(iov[i].iov_base, p, k);
    p += k;
    n -= k;
  }
}

static void EmuGather(const struct iovec *iov, int iovcnt, u8 *p, size_t n) {
  int i;
  size_t k;
  for (i = 0; n && i < iovcnt; ++i) {
    k = MIN(n, iov[i].iov_len);
    memcpy(p, iov[i].iov_base, k);
    p += k;
    n -= k;
  }
}

static ssize_t EmuEventfdRead(int fildes, struct EmuObj *o,
                              const struct iovec *iov, int iovcnt) {
  int rc;
  u64 value;
  u8 word[8];
  if (EmuIovLen(iov, iovcnt) < 8) return einval();
  while (!o->count) {
    if (EmuIsNonblocking(fildes)) return eagain();
    if ((rc = EmuWait(GetMaxTime(), kPollingMs)) == -1) return -1;
  }
  if (o->semaphore) {
    value = 1;
  } else {
    value = o->count;
  }
  o->count -= value;
  Write64(word, value);
  EmuScatter(iov, iovcnt, word, 8);
  EmuTouch(o);
  EmuNotify();
  return 8;
}

static ssize_t EmuEventfdWrite(int fildes, struct EmuObj *o,
                               const struct iovec *iov, int iovcnt) {
  u64 value;
  u8 word[8];
  if (EmuIovLen(iov, iovcnt) < 8) return einval();
  EmuGather(iov, iovcnt, word, 8);
  value = Read64(word);
  if (value == UINT64_C(0xffffffffffffffff)) return einval();
  while (kEmuEventfdMax - o->count < value) {
    if (EmuIsNonblocking(fildes)) return eagain();
    if (EmuWait(GetMaxTime(), kPollingMs) == -1) return -1;
  }
  o->count += value;
  EmuTouch(o);
  EmuNotify();
  return 8;
}

int EmuEventfd(struct Machine *m, u32 initval, i32 flags) {
  int fildes;
  struct EmuObj *o;
  const i32 kSemaphore = 1;  // EFD_SEMAPHORE
  if (flags & ~(O_CLOEXEC_LINUX | O_NDELAY_LINUX | kSemaphore)) {
    return einval();
  }
  if (!(o = EmuNewObj(kEmuEventfd))) return enomem();
  o->count = initval;
  o->semaphore = !!(flags & kSemaphore);
  if ((fildes = EmuInstall(m, o, !!(flags & O_CLOEXEC_LINUX),
                           !!(flags & O_NDELAY_LINUX), false)) == -1) {
    EmuDiscard(o);
  }
  return fildes;
}

////////////////////////////////////////////////////////////////////////////////
// streams (pipes and AF_UNIX stream socket pairs)

static size_t EmuBufRead(struct EmuBuf *b, const struct iovec *iov,
                         int iovcnt, bool peek) {
  int i;
  size_t k, n, got, pos;
  for (got = 0, pos = b->head, i = 0; i < iovcnt && got < b->len; ++i) {
    for (n = 0; n < iov[i].iov_len && got < b->len;) {
      k = MIN(iov[i].iov_len - n, b->len - got);
      k = MIN(k, b->cap - pos);
      memcpy((u8 *)iov[i].iov_base + n, b->data + pos, k);
      n += k;
      got += k;
      pos = (pos + k) % b->cap;
    }
  }
  if (!peek && got) {
    b->head = pos;
    b->len -= got;
    if (!b->len) b->head = 0;
    EmuTouchBuf(b);
  }
  return got;
}

static size_t EmuBufWrite(struct EmuBuf *b, const struct iovec *iov,
                          int iovcnt, size_t skip) {
  int i;
  size_t k, n, put, tail, room;
  room = b->cap - b->len;
  for (put = 0, i = 0; i < iovcnt && put < room; ++i) {
    if (skip >= iov[i].iov_len) {
      skip -= iov[i].iov_len;
      continue;
    }
    for (n = skip, skip = 0; n < iov[i].iov_len && put < room;) {
      tail = (b->head + b->len) % b->cap;
      k = MIN(iov[i].iov_len - n, room - put);
      k = MIN(k, b->cap - tail);
      memcpy(b->data + tail, (const u8 *)iov[i].iov_base + n, k);
      n += k;
      put += k;
      b->len += k;
    }
  }
  if (put) EmuTouchBuf(b);
  return put;
}

static ssize_t EmuStreamRecv(int fildes, struct EmuObj *o,
                             const struct iovec *iov, int iovcnt, int flags) {
  int rc;
  size_t want, got;
  bool nonblock, peek, waitall;
  if (!o->in) return ebadf();
  want = EmuIovLen(iov, iovcnt);
  if (!want) return 0;
  nonblock = (flags & MSG_DONTWAIT) || EmuIsNonblocking(fildes);
  peek = !!(flags & MSG_PEEK);
  waitall = (flags & MSG_WAITALL) && !peek && !nonblock;
  for (got = 0;;) {
    if (o->in->len) {
      if (got) {
        struct iovec rest[IOV_MAX_LINUX];
        int i, j;
        size_t skip = got;
        for (j = i = 0; i < iovcnt && j < IOV_MAX_LINUX; ++i) {
          if (skip >= iov[i].iov_len) {
            skip -= iov[i].iov_len;
            continue;
          }
          rest[j].iov_base = (u8 *)iov[i].iov_base + skip;
          rest[j].iov_len = iov[i].iov_len - skip;
          skip = 0;
          ++j;
        }
        got += EmuBufRead(o->in, rest, j, peek);
      } else {
        got += EmuBufRead(o->in, iov, iovcnt, peek);
      }
      if (!peek) EmuNotify();
      if (!waitall || got == want) return got;
      continue;
    }
    if (o->in->eof) return got;
    if (got) {
      if (!waitall) return got;
    } else if (nonblock) {
      return eagain();
    }
    if ((rc = EmuWait(GetMaxTime(), kPollingMs)) == -1) {
      return got ? (ssize_t)got : -1;
    }
  }
}

static ssize_t EmuStreamSend(int fildes, struct EmuObj *o,
                             const struct iovec *iov, int iovcnt, int flags) {
  int rc;
  size_t want, put;
  bool nonblock;
  if (!o->out) return ebadf();
  want = EmuIovLen(iov, iovcnt);
  nonblock = (flags & MSG_DONTWAIT) || EmuIsNonblocking(fildes);
  for (put = 0;;) {
    if (o->out->broken || o->out->eof) {
      if (put) return put;
      errno = EPIPE;
      return -1;
    }
    if (put == want) return put;
    if (o->out->len < o->out->cap) {
      put += EmuBufWrite(o->out, iov, iovcnt, put);
      EmuNotify();
      if (put == want || nonblock) return put;
      continue;
    }
    if (nonblock) return put ? (ssize_t)put : eagain();
    if ((rc = EmuWait(GetMaxTime(), kPollingMs)) == -1) {
      return put ? (ssize_t)put : -1;
    }
  }
}

static int EmuNewStreamPair(struct EmuObj *a, struct EmuObj *b, bool socket) {
  struct EmuBuf *ab, *ba = 0;
  if (!(ab = EmuNewBuf(socket ? kEmuSocketCapacity : kEmuPipeCapacity))) {
    return enomem();
  }
  if (socket && !(ba = EmuNewBuf(kEmuSocketCapacity))) {
    EmuDropBuf(ab);
    return enomem();
  }
  a->socket = b->socket = socket;
  // a writes ab and b reads it; for sockets b writes ba and a reads it.
  a->out = ab;
  b->in = ab;
  ab->writer = a;
  ab->reader = b;
  ab->users = 2;
  if (ba) {
    b->out = ba;
    a->in = ba;
    ba->writer = b;
    ba->reader = a;
    ba->users = 2;
  }
  return 0;
}

// Creates the two endpoints and installs them as guest descriptors. For a
// pipe, fds[0] is the read end and fds[1] the write end.
static int EmuNewStreams(struct Machine *m, int fds[2], bool socket,
                         bool cloexec, bool nonblock) {
  struct EmuObj *a, *b;
  a = EmuNewObj(kEmuStream);
  b = EmuNewObj(kEmuStream);
  if (!a || !b || EmuNewStreamPair(a, b, socket) == -1) {
    free(a);
    free(b);
    return enomem();
  }
  // the writer end goes in fds[1] so pipe semantics come out right
  if ((fds[1] = EmuInstall(m, a, cloexec, nonblock, socket)) == -1) {
    EmuDiscard(a);
    EmuDiscard(b);
    return -1;
  }
  if ((fds[0] = EmuInstall(m, b, cloexec, nonblock, socket)) == -1) {
    SysClose(m, fds[1]);
    EmuDiscard(b);
    return -1;
  }
  return 0;
}

int EmuPipe(struct Machine *m, int fds[2], i32 flags) {
  if (flags & ~(O_CLOEXEC_LINUX | O_NDELAY_LINUX)) return einval();
  return EmuNewStreams(m, fds, false, !!(flags & O_CLOEXEC_LINUX),
                       !!(flags & O_NDELAY_LINUX));
}

int EmuSocketpair(struct Machine *m, i32 type, i32 flags, int fds[2]) {
  if (type != SOCK_STREAM_LINUX) {
    errno = EPROTONOSUPPORT;
    return -1;
  }
  return EmuNewStreams(m, fds, true, !!(flags & SOCK_CLOEXEC_LINUX),
                       !!(flags & SOCK_NONBLOCK_LINUX));
}

ssize_t EmuRecvmsg(int fildes, struct msghdr *msg, int flags) {
  ssize_t rc;
  struct EmuObj *o;
  EmuLock();
  if (!(o = EmuGet(fildes))) {
    rc = ebadf();
  } else if (o->kind != kEmuStream || !o->socket) {
    rc = enotsock();
  } else {
    rc = EmuStreamRecv(fildes, o, msg->msg_iov, msg->msg_iovlen, flags);
  }
  EmuUnlock();
  if (rc != -1) {
    msg->msg_namelen = 0;
    msg->msg_controllen = 0;
    msg->msg_flags = 0;
  }
  return rc;
}

ssize_t EmuSendmsg(int fildes, const struct msghdr *msg, int flags) {
  ssize_t rc;
  struct EmuObj *o;
  EmuLock();
  if (!(o = EmuGet(fildes))) {
    rc = ebadf();
  } else if (o->kind != kEmuStream || !o->socket) {
    rc = enotsock();
  } else if (msg->msg_controllen) {
    rc = einval();  // SCM_RIGHTS and friends aren't emulated
  } else {
    rc = EmuStreamSend(fildes, o, msg->msg_iov, msg->msg_iovlen, flags);
  }
  EmuUnlock();
  return rc;
}

int EmuShutdown(int fildes, int how) {
  int rc = 0;
  struct EmuObj *o;
  EmuLock();
  if (!(o = EmuGet(fildes))) {
    rc = ebadf();
  } else if (o->kind != kEmuStream || !o->socket) {
    rc = enotsock();
  } else if (how != SHUT_RD && how != SHUT_WR && how != SHUT_RDWR) {
    rc = einval();
  } else {
    if ((how == SHUT_RD || how == SHUT_RDWR) && o->in) {
      o->in->eof = true;
      o->in->broken = true;
      EmuTouchBuf(o->in);
    }
    if ((how == SHUT_WR || how == SHUT_RDWR) && o->out) {
      o->out->eof = true;
      EmuTouchBuf(o->out);
    }
    EmuNotify();
  }
  EmuUnlock();
  return rc;
}

int EmuGetsockopt(int fildes, int level, int optname, void *optval,
                  socklen_t *optlen) {
  int value;
  if (!EmuIsSocket(fildes)) return EmuIsFd(fildes) ? enotsock() : ebadf();
  if (level != SOL_SOCKET) return enoprotoopt();
  switch (optname) {
    case SO_TYPE:
      value = SOCK_STREAM;
      break;
    case SO_ERROR:
    case SO_RCVTIMEO:
    case SO_SNDTIMEO:
      value = 0;
      break;
    case SO_RCVBUF:
    case SO_SNDBUF:
      value = kEmuSocketCapacity;
      break;
    default:
      return enoprotoopt();
  }
  if (optname == SO_RCVTIMEO || optname == SO_SNDTIMEO) {
    memset(optval, 0, MIN(*optlen, (socklen_t)sizeof(struct timeval)));
    *optlen = MIN(*optlen, (socklen_t)sizeof(struct timeval));
    return 0;
  }
  if (*optlen < (socklen_t)sizeof(value)) return einval();
  memcpy(optval, &value, sizeof(value));
  *optlen = sizeof(value);
  return 0;
}

int EmuSetsockopt(int fildes, int level, int optname, const void *optval,
                  socklen_t optlen) {
  if (!EmuIsSocket(fildes)) return EmuIsFd(fildes) ? enotsock() : ebadf();
  // Buffer sizes and timeouts are accepted and ignored; the emulated pair
  // has fixed buffers and blocking calls are interruptible by signals.
  (void)level;
  (void)optname;
  (void)optval;
  (void)optlen;
  return 0;
}

int EmuGetsockname(int fildes, struct sockaddr *addr, socklen_t *addrlen) {
  if (!EmuIsSocket(fildes)) return EmuIsFd(fildes) ? enotsock() : ebadf();
  // an unnamed AF_UNIX socket: just the family
  if (*addrlen >= (socklen_t)sizeof(addr->sa_family)) {
    addr->sa_family = AF_UNIX;
  }
  *addrlen = sizeof(addr->sa_family);
  return 0;
}

////////////////////////////////////////////////////////////////////////////////
// epoll

int EmuEpollCreate(struct Machine *m, i32 flags) {
  int fildes;
  struct EmuObj *o;
  if (flags & ~EPOLL_CLOEXEC_LINUX) return einval();
  if (!(o = EmuNewObj(kEmuEpoll))) return enomem();
  if ((fildes = EmuInstall(m, o, !!(flags & EPOLL_CLOEXEC_LINUX), false,
                           false)) == -1) {
    EmuDiscard(o);
  }
  return fildes;
}

static struct EmuWatch *EmuFindWatch(struct EmuObj *ep, int fd) {
  int i;
  for (i = 0; i < ep->nwatches; ++i) {
    if (ep->watches[i].fd == fd) return ep->watches + i;
  }
  return 0;
}

static bool EmuIsPollableHostFd(int fd) {
  struct stat st;
  if (VfsFstat(fd, &st) == -1) return false;
  // Linux refuses to watch regular files and directories with EPERM.
  return !S_ISREG(st.st_mode) && !S_ISDIR(st.st_mode);
}

int EmuEpollCtl(struct Machine *m, i32 epfd, i32 op, i32 fd, u32 events,
                u64 data) {
  int rc;
  struct Fd *gfd;
  struct EmuWatch *w, *p;
  struct EmuObj *ep, *target;
  bool hostok = true, hostfd;
  // EBADF for descriptors the guest doesn't have
  if (!(gfd = GetAndLockFd(m, epfd))) return -1;
  UnlockFd(gfd);
  if (!(gfd = GetAndLockFd(m, fd))) return -1;
  UnlockFd(gfd);
  EmuLock();
  hostfd = !EmuGet(fd);
  EmuUnlock();
  if (hostfd && op != EPOLL_CTL_DEL_LINUX) hostok = EmuIsPollableHostFd(fd);
  EmuLock();
  if (!(ep = EmuGet(epfd)) || ep->kind != kEmuEpoll || epfd == fd) {
    rc = einval();
  } else if ((target = EmuGet(fd)) == ep) {
    rc = einval();
  } else if (!hostok) {
    rc = eperm();
  } else {
    w = EmuFindWatch(ep, fd);
    rc = 0;
    switch (op) {
      case EPOLL_CTL_ADD_LINUX:
        if (w) {
          rc = eexist();
          break;
        }
        if (ep->nwatches == ep->capwatches) {
          int cap = ep->capwatches ? ep->capwatches * 2 : 8;
          if (!(p = (struct EmuWatch *)realloc(ep->watches,
                                              cap * sizeof(*p)))) {
            rc = enomem();
            break;
          }
          ep->watches = p;
          ep->capwatches = cap;
        }
        w = ep->watches + ep->nwatches++;
        memset(w, 0, sizeof(*w));
        w->fd = fd;
        w->target = target;
        w->events = events;
        w->data = data;
        w->seen = target ? target->gen - 1 : 0;  // report current state once
        break;
      case EPOLL_CTL_MOD_LINUX:
        if (!w) {
          rc = enoent();
          break;
        }
        w->events = events;
        w->data = data;
        w->disabled = false;
        w->seen = w->target ? w->target->gen - 1 : 0;
        w->lastready = 0;
        break;
      case EPOLL_CTL_DEL_LINUX:
        if (!w) {
          rc = enoent();
          break;
        }
        *w = ep->watches[--ep->nwatches];
        break;
      default:
        rc = einval();
        break;
    }
    if (!rc) {
      EmuTouch(ep);
      EmuNotify();
    }
  }
  EmuUnlock();
  return rc;
}

static int EmuEpollScan(struct EmuObj *ep, struct epoll_event_linux *out,
                        int maxevents, bool *hostwatch) {
  int i, n;
  u32 r;
  struct EmuWatch *w;
  *hostwatch = false;
  for (n = i = 0; i < ep->nwatches && n < maxevents;) {
    w = ep->watches + i;
    if (!w->target) {
      *hostwatch = true;
      if (EmuHostReadiness(w->fd) & EPOLLNVAL_LINUX) {
        // closed behind our back: Linux drops it with the file
        *w = ep->watches[--ep->nwatches];
        continue;
      }
    }
    if ((r = EmuWatchReady(w))) {
      if (w->target) {
        w->seen = w->target->gen;
      } else {
        w->lastready = r;
      }
      if (w->events & EPOLLONESHOT_LINUX) w->disabled = true;
      Write32(out[n].events, r);
      Write64(out[n].data, w->data);
      ++n;
    } else if (!w->target && (w->events & EPOLLET_LINUX)) {
      // remember when a host descriptor stops being ready, so the next
      // readiness counts as a new edge
      u32 now = EmuHostReadiness(w->fd) & EmuWatchMask(w);
      if (!now) w->lastready = 0;
    }
    ++i;
  }
  return n;
}

int EmuEpollWait(struct Machine *m, i32 epfd, struct epoll_event_linux *out,
                 int maxevents, struct timespec deadline) {
  int n, rc;
  bool hostwatch;
  struct EmuObj *ep;
  if (maxevents <= 0) return einval();
  EmuLock();
  if (!(ep = EmuGet(epfd)) || ep->kind != kEmuEpoll) {
    EmuUnlock();
    return einval();
  }
  ++ep->refs;  // keep it alive if another thread closes it meanwhile
  for (;;) {
    n = EmuEpollScan(ep, out, maxevents, &hostwatch);
    if (n) break;
    if ((rc = EmuWait(deadline, hostwatch ? kEmuHostPollMs : kPollingMs))) {
      if (rc == -1) n = -1;
      break;
    }
  }
  EmuRelease(ep);
  EmuUnlock();
  return n;
}

////////////////////////////////////////////////////////////////////////////////
// descriptor callbacks

static ssize_t EmuReadv(int fildes, const struct iovec *iov, int iovcnt) {
  ssize_t rc;
  struct EmuObj *o;
  EmuLock();
  if (!(o = EmuGet(fildes))) {
    rc = ebadf();
  } else if (o->kind == kEmuEventfd) {
    rc = EmuEventfdRead(fildes, o, iov, iovcnt);
  } else if (o->kind == kEmuStream) {
    rc = EmuStreamRecv(fildes, o, iov, iovcnt, 0);
  } else {
    rc = einval();
  }
  EmuUnlock();
  return rc;
}

static ssize_t EmuWritev(int fildes, const struct iovec *iov, int iovcnt) {
  ssize_t rc;
  struct EmuObj *o;
  EmuLock();
  if (!(o = EmuGet(fildes))) {
    rc = ebadf();
  } else if (o->kind == kEmuEventfd) {
    rc = EmuEventfdWrite(fildes, o, iov, iovcnt);
  } else if (o->kind == kEmuStream) {
    rc = EmuStreamSend(fildes, o, iov, iovcnt, 0);
  } else {
    rc = einval();
  }
  EmuUnlock();
  return rc;
}

static int EmuNotATerminal(void) {
  errno = ENOTTY;
  return -1;
}

static int EmuTcgetattr(int fd, struct termios *tio) {
  return EmuNotATerminal();
}

static int EmuTcsetattr(int fd, int act, const struct termios *tio) {
  return EmuNotATerminal();
}

static int EmuTcgetwinsize(int fd, struct winsize *ws) {
  return EmuNotATerminal();
}

static int EmuTcsetwinsize(int fd, const struct winsize *ws) {
  return EmuNotATerminal();
}

const struct FdCb kFdCbEmu = {
    .close = EmuClose,
    .readv = EmuReadv,
    .writev = EmuWritev,
    .poll = EmuPoll,
    .tcgetattr = EmuTcgetattr,
    .tcsetattr = EmuTcsetattr,
    .tcgetwinsize = EmuTcgetwinsize,
    .tcsetwinsize = EmuTcsetwinsize,
};

#endif /* HAVE_EMUFD */
