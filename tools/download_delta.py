"""Fetch checksum-pinned Delta 3.3.2 test JARs for Spark 3.5 / Scala 2.12."""
import argparse
import hashlib
from pathlib import Path
from urllib.request import urlopen

ARTIFACTS = {
    'delta-spark_2.12': '4e92085d5d794cf949c50bb665b91384857cad58680c2d0bfc9c7ad353e98e85',
    'delta-storage': 'c096975a2ca97ccc9b4005aa5cce16c9ac3903139d340da6665b66ace67d7c36',
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, default=Path('.tools/delta'))
    folder = parser.parse_args().output_dir
    folder.mkdir(parents=True, exist_ok=True)
    for name, sha in ARTIFACTS.items():
        filename = f'{name}-3.3.2.jar'
        url = f'https://repo.maven.apache.org/maven2/io/delta/{name}/3.3.2/{filename}'
        with urlopen(url, timeout=90) as response:
            data = response.read()
        if hashlib.sha256(data).hexdigest() != sha:
            raise RuntimeError(f'Checksum mismatch for {filename}')
        (folder / filename).write_bytes(data)
        print(folder / filename)


if __name__ == '__main__':
    main()
