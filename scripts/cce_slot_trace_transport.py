"""ADR0242 transfer exact stopped observer traces under ADR0240 bounds."""
import re
from pathlib import Path

from bounded_trace_transport import unpack_trace


def collect(stage, directory, owner, name, copy_out):
    if (name not in {"pipeline", "kafka", "projection-kafka"}
            or directory != "/tmp/" + stage.run + "/cce-observers"
            or not re.fullmatch(r"adr0151-[0-9a-f]{12}", stage.run)):
        raise ValueError("Exact owned stopped trace required")
    source, packed = directory + "/" + name + ".jsonl", directory + "/" + name + ".jsonl.gz"
    # Supporting helper hashes are verified before jobs start. Transform only
    # after stop_jobs succeeds; pack also rejects an input changed during reading.
    code = ("import json,sys;sys.path.insert(0," + repr(directory) + ");"
            "from bounded_trace_transport import pack_trace;"
            "print(json.dumps(pack_trace(" + repr(source) + "," + repr(packed) + ")))" )
    stage.check(90)
    receipt = stage.session.api(stage.cid, code, 45)
    raw = copy_out(stage.session, stage.cid, packed, owner + "/" + name + ".jsonl.gz")
    target = Path(stage.output) / (name + ".jsonl.gz")
    with target.open("xb") as stream:
        stream.write(raw)
    restored = Path(stage.output) / (name + ".unpacked.private.jsonl")
    verified = unpack_trace(target, restored, receipt)
    # Return exact bytes to the existing complete summary and financial gates.
    return restored.read_bytes(), {"compressed": True, "verified": verified, "receipt": receipt}
