"""Download a checksum-pinned test dependency; never downloaded during pytest."""
import argparse
import hashlib
from pathlib import Path
from urllib.request import urlopen

VERSION = '3.13.2'
SHA256 = 'd097b19bd483c5038479b13a5c71e9faf8f2f5106584f0c120a7770ab0bdb367'
URL = f'https://repo.maven.apache.org/maven2/org/wiremock/wiremock-standalone/{VERSION}/wiremock-standalone-{VERSION}.jar'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', default='.tools/wiremock.jar', type=Path)
    target = parser.parse_args().output
    with urlopen(URL, timeout=90) as response:
        data = response.read()
    if hashlib.sha256(data).hexdigest() != SHA256:
        raise RuntimeError('WireMock checksum mismatch')
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    print(target)


if __name__ == '__main__':
    main()
