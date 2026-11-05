#!/bin/sh
# Bookworm publishes no sqlite3 >= 3.54.0, and sid's 3.54.1 needs libc6 >= 2.38.
set -eu

apt-get update
apt-get install -y --no-install-recommends build-essential

python - <<'PY'
import hashlib, tarfile, urllib.request
url = "https://www.sqlite.org/2026/sqlite-autoconf-3540100.tar.gz"
path = "/tmp/sqlite.tar.gz"
urllib.request.urlretrieve(url, path)
digest = hashlib.sha256(open(path, "rb").read()).hexdigest()
expect = "926ef8d3eab93fbf6e06011901273eec076b25594a4e1cdadd50c987f4f6d64e"
if digest != expect:
    raise SystemExit(f"sqlite tarball sha256 {digest}")
tarfile.open(path).extractall("/tmp")
PY

cd /tmp/sqlite-autoconf-3540100
export CFLAGS="-O2 -fno-strict-aliasing \
-DSQLITE_SECURE_DELETE -DSQLITE_ENABLE_COLUMN_METADATA \
-DSQLITE_ENABLE_FTS3 -DSQLITE_ENABLE_FTS3_PARENTHESIS \
-DSQLITE_ENABLE_RTREE -DSQLITE_SOUNDEX=1 \
-DSQLITE_ENABLE_UNLOCK_NOTIFY \
-DSQLITE_ENABLE_DBSTAT_VTAB -DSQLITE_ENABLE_DBPAGE_VTAB \
-DSQLITE_ALLOW_ROWID_IN_VIEW -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT=1 \
-DSQLITE_ENABLE_LOAD_EXTENSION -DSQLITE_ENABLE_JSON1 \
-DSQLITE_LIKE_DOESNT_MATCH_BLOBS -DSQLITE_THREADSAFE=1 \
-DSQLITE_USE_URI=1 -DSQLITE_MAX_SCHEMA_RETRY=25 \
-DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_SESSION \
-DSQLITE_ENABLE_STMTVTAB -DSQLITE_STRICT_SUBTYPE=1 \
-DSQLITE_MAX_VARIABLE_NUMBER=250000"
libdir=/usr/lib/$(gcc -dumpmachine)
./configure --prefix=/usr --libdir="$libdir" --disable-static --enable-threadsafe
make -j"$(nproc)"
make install DESTDIR=/tmp/sqlite-root

rm -rf /tmp/debpkg
mkdir -p "/tmp/debpkg/DEBIAN" "/tmp/debpkg${libdir}"
cp -a /tmp/sqlite-root${libdir}/libsqlite3.so.* "/tmp/debpkg${libdir}/"
# Bookworm's tracker marks source sqlite3 unfixed at every version, so a
# package that claims that source stays flagged past the upstream fix.
printf '%s\n' \
    'Package: libsqlite3-0' \
    'Version: 3.54.1-1' \
    "Architecture: $(dpkg --print-architecture)" \
    'Multi-Arch: same' \
    'Maintainer: Codecov <security@codecov.io>' \
    'Depends: libc6 (>= 2.36)' \
    'Description: SQLite 3 shared library' \
    > /tmp/debpkg/DEBIAN/control
dpkg-deb --build /tmp/debpkg /tmp/libsqlite3.deb
dpkg -i /tmp/libsqlite3.deb
ldconfig
python -c 'import sqlite3; assert sqlite3.sqlite_version=="3.54.1", sqlite3.sqlite_version'

apt-get purge -y build-essential
apt-get autoremove -y
rm -rf /var/lib/apt/lists/* /tmp/sqlite.tar.gz /tmp/sqlite-autoconf-3540100 /tmp/sqlite-root /tmp/debpkg /tmp/libsqlite3.deb
