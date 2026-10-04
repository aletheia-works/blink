// Additions to Emscripten's in-memory filesystem (MEMFS) that Linux
// programs expect: hard links and flock(2). Linked into the Emscripten
// build with --js-library; see emscriptenfs.c for the C side.
//
// Everything here runs on the main JS thread: blink's file system calls
// from guest threads (Web Workers) are proxied there, so this state needs
// no locking.

addToLibrary({
  // Hard links for MEMFS, which has none: FS.link() fails with EMLINK
  // unless the parent directory has a `link` node op. MEMFS finds nodes
  // through FS's name table, keyed by each node's single parent and name,
  // so a second name for the same node is kept in the directory's
  // `contents` and found through a `lookup` fallback. The link count is
  // kept on the node and reported by getattr.
  // Known gap: renaming the second name moves the node's primary name.
  $BLINKFS_HARDLINKS__deps: ['$FS', '$MEMFS', '$addOnPreRun'],
  $BLINKFS_HARDLINKS__postset: () => 'addOnPreRun(BLINKFS_HARDLINKS);',
  $BLINKFS_HARDLINKS: () => {
    if (!MEMFS.ops_table) {
      MEMFS.createNode(null, '/', {{{ cDefs.S_IFDIR }}} | 0o777, 0);
    }
    var AT_SYMLINK_FOLLOW = 0x400;
    var dir = MEMFS.ops_table.dir.node;
    var file = MEMFS.ops_table.file.node;
    var link = MEMFS.ops_table.link.node;

    var lookup = dir.lookup;
    dir.lookup = (parent, name) => parent.contents[name] ?? lookup(parent, name);

    dir.link = (parent, newname, oldpath, flags) => {
      var target = FS.lookupPath(oldpath, { follow: !!(flags & AT_SYMLINK_FOLLOW) }).node;
      if (FS.isDir(target.mode)) {
        throw new FS.ErrnoError({{{ cDefs.EPERM }}});
      }
      if (parent.contents[newname]) {
        throw new FS.ErrnoError({{{ cDefs.EEXIST }}});
      }
      parent.contents[newname] = target;
      target.blinkNlink = (target.blinkNlink ?? 1) + 1;
      target.ctime = Date.now();
      parent.ctime = parent.mtime = Date.now();
    };

    var unlink = dir.unlink;
    dir.unlink = (parent, name) => {
      var node = parent.contents[name];
      if (node?.blinkNlink > 1) {
        node.blinkNlink--;
      }
      unlink(parent, name);
    };

    for (var ops of [file, link]) {
      let getattr = ops.getattr;
      ops.getattr = (node) => {
        var attr = getattr(node);
        attr.nlink = node.blinkNlink ?? attr.nlink;
        return attr;
      };
    }
  },

  // flock(2) with BSD semantics: locks belong to an open file description
  // (shared by dup()ed descriptors, which share `stream.shared`), and are
  // released when its last descriptor closes. Never blocks: a conflicting
  // request fails with EWOULDBLOCK, and blink retries blocking requests on
  // the guest's own thread.
  $BLINKFS_FLOCKS__deps: ['$FS', '$addOnPreRun'],
  $BLINKFS_FLOCKS__postset: () => 'addOnPreRun(BLINKFS_FLOCKS.install);',
  $BLINKFS_FLOCKS: {
    locks: new Map(),  // node -> { ex: owner or null, sh: Set of owners }
    release(owner) {
      for (var [node, entry] of BLINKFS_FLOCKS.locks) {
        if (entry.ex === owner) entry.ex = null;
        entry.sh.delete(owner);
        if (!entry.ex && !entry.sh.size) BLINKFS_FLOCKS.locks.delete(node);
      }
    },
    install() {
      var close = FS.close;
      FS.close = (stream) => {
        var owner = stream.shared;
        close(stream);
        if (owner && !FS.streams.some((s) => s && s.shared === owner)) {
          BLINKFS_FLOCKS.release(owner);
        }
      };
    },
  },

  // also pulls in the hard link installer, which no C code names
  blinkfs_flock__deps: ['$FS', '$BLINKFS_FLOCKS', '$BLINKFS_HARDLINKS'],
  blinkfs_flock__proxy: 'sync',
  blinkfs_flock: (fd, op) => {
    var LOCK_SH = 1, LOCK_EX = 2, LOCK_NB = 4, LOCK_UN = 8;
    var stream = FS.getStream(fd);
    if (!stream) return -{{{ cDefs.EBADF }}};
    if (!stream.node) return -{{{ cDefs.EINVAL }}};
    var owner = stream.shared;
    var locks = BLINKFS_FLOCKS.locks;
    var entry = locks.get(stream.node);
    if (!entry) {
      entry = { ex: null, sh: new Set() };
      locks.set(stream.node, entry);
    }
    var others = [...entry.sh].filter((o) => o !== owner).length;
    switch (op & ~LOCK_NB) {
      case LOCK_UN:
        if (entry.ex === owner) entry.ex = null;
        entry.sh.delete(owner);
        break;
      case LOCK_SH:
        if (entry.ex && entry.ex !== owner) return -{{{ cDefs.EWOULDBLOCK }}};
        if (entry.ex === owner) entry.ex = null;  // convert, as Linux does
        entry.sh.add(owner);
        break;
      case LOCK_EX:
        if ((entry.ex && entry.ex !== owner) || others) return -{{{ cDefs.EWOULDBLOCK }}};
        entry.sh.delete(owner);
        entry.ex = owner;
        break;
      default:
        return -{{{ cDefs.EINVAL }}};
    }
    if (!entry.ex && !entry.sh.size) locks.delete(stream.node);
    return 0;
  },
});
