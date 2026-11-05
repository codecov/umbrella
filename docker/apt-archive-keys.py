"""Add the trixie archive keys to the keyring apt-key actually verifies with."""

import base64
import urllib.request


def dearmor(url):
    urllib.request.urlretrieve(url, "/tmp/k.asc")
    body, started = [], False
    for line in open("/tmp/k.asc"):
        line = line.strip()
        if line.startswith("-----BEGIN"):
            continue
        if line.startswith("-----END"):
            break
        if not started:
            if not line:
                started = True
            continue
        if line.startswith("="):
            break
        body.append(line)
    return base64.b64decode("".join(body))


with open("/usr/share/keyrings/debian-archive-keyring.gpg", "ab") as f:
    for name in ("archive-key-13.asc", "archive-key-13-security.asc"):
        f.write(dearmor(f"https://ftp-master.debian.org/keys/{name}"))
