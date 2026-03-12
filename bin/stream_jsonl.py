#!/usr/bin/env python3
"""
Streaming UniProtKB JSON → JSONL converter.
 
Usage (standalone):
    pigz -dc input.json.gz | python3 stream_jsonl.py | zstd -3 -T0 -o output.jsonl.zst
 
Usage (chunked):
    pigz -dc input.json.gz \
      | python3 stream_jsonl.py \
      | split -l 500000 --filter='zstd -3 -T0 > chunks/$FILE.jsonl.zst' - chunk_
"""
 
import sys
 
try:
    import ijson.backends.yajl2_c as ijson
except ImportError:
    import ijson
    print("Warning: yajl2_c backend not found; falling back to pure Python ijson.", file=sys.stderr)
 
import orjson
 
DUMPS = orjson.dumps
OPT = orjson.OPT_APPEND_NEWLINE
WRITE = sys.stdout.buffer.write
 
 
def main():
    try:
        for item in ijson.items(sys.stdin.buffer, "results.item", use_float=True):
            WRITE(DUMPS(item, option=OPT))
    except BrokenPipeError:
        pass
    except KeyboardInterrupt:
        pass
 
 
if __name__ == "__main__":
    main()