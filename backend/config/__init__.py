"""Configuration package for stock event replay system."""

import selectors

# Patch selectors._BaseSelectorImpl._fileobj_lookup to prevent Windows Python 3.12+ ValueError: Invalid file descriptor: -1
# when kafka-python-ng unregisters sockets closed during broker coordinator discovery.
try:
    _orig_fileobj_lookup = selectors._BaseSelectorImpl._fileobj_lookup

    def _safe_fileobj_lookup(self, fileobj):
        try:
            return _orig_fileobj_lookup(self, fileobj)
        except ValueError:
            for fd, key in list(self._fd_to_key.items()):
                if key.fileobj is fileobj:
                    return fd
            raise

    selectors._BaseSelectorImpl._fileobj_lookup = _safe_fileobj_lookup
except Exception:
    pass
