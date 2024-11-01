import os
import time
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from os import fsync, path
from src.format import (
    HEADER_SIZE,
    KVData,
    KVEntry,
    KVHeader,
)


def shrink(filename: str):
    if not path.exists(filename):
        return

    tombstones = set()

    source_file = filename
    target_file = filename + "_kompact"

    print("initializing compaction...")
    try:
        with open(
            source_file,
            "a+b",
        ) as infile, open(
            target_file,
            "a+b",
        ) as outfile:
            # flushing buffers before load to persit leftover data in buffers
            # and reset position back to zero
            infile.flush()
            fsync(infile.fileno())
            infile.seek(0)

            while hdr_bytes := infile.read(HEADER_SIZE):
                chksm, tstamp, expiry, deleted, ksz, vsz = KVHeader.decode_hdr(
                    hdr_bytes,
                )

                # find deleted and expired keys
                if deleted == 1 or (expiry > 0 and expiry <= int(time.time())):
                    tombstones.add(infile.read(ksz).decode("utf-8"))
                    infile.seek(vsz, os.SEEK_CUR)
                else:
                    infile.seek((ksz + vsz), os.SEEK_CUR)

            if len(tombstones) < 1:
                return

            infile.seek(0)
            while hdr_bytes := infile.read(HEADER_SIZE):
                chksm, tstamp, expiry, deleted, ksz, vsz = KVHeader.decode_hdr(
                    hdr_bytes,
                )
                key_bytes = infile.read(ksz)
                key = key_bytes.decode("utf-8")

                # skip if deleted or expired
                if key in tombstones:
                    infile.seek(vsz, os.SEEK_CUR)
                    continue

                outfile.write(hdr_bytes + key_bytes + infile.read(vsz))
                outfile.flush()
                fsync(outfile.fileno())

            outfile.flush()
            fsync(outfile.fileno())

    except Exception as e:
        print(f"unexpected {e=}, {type(e)=} druing compaction")
        print("aborting compaction process...!")

        # delete the target file if it was created
        if path.exists(target_file):
            try:
                os.remove(target_file)
            except OSError as err:
                print(f"OSError: {err}")
        return

    # Clean up the source file
    try:
        os.remove(source_file)
    except OSError as err:
        print(f"OSError when cleaning up: {err}")
        return

    # rename the target file
    try:
        os.rename(target_file, filename)
    except OSError as err:
        print(f"OSError when renaming the compact file: {err}")
        return

    print("compaction finished...!")
