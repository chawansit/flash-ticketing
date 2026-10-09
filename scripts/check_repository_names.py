"""ADR0169: bounded structural naming checks; no imports of cloud runners."""
import argparse
import json
import posixpath
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIMIT = 1024 * 1024
ADR_NAME = re.compile(r"docs/adr/(\d{4})-[a-z0-9]+(?:-[a-z0-9]+)*\.md$")
ADR_ID = re.compile(r"\bADR\s*(\d+)\b")
LINK = re.compile(r"\[([^\]]*)\]\(([^)]+)\)")


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT)


def wanted(name):
    return ((name.startswith("docs/adr/") and name.endswith(".md"))
            or (name.startswith("docs/capacity/") and name.endswith(".json")))


def snapshot(revision=None, staged=False):
    if staged:
        revision = git("write-tree").decode().strip()
    if revision:
        records = git("ls-tree", "-rlz", revision).split(bytes([0]))
        paths, requests = set(), []
        for record in records:
            if not record:continue
            meta, raw_name = record.split(bytes([9]), 1)
            name = raw_name.decode();paths.add(name)
            fields = meta.split()
            if wanted(name) and fields[1] == b"blob" and int(fields[3]) <= LIMIT:
                requests.append((name, fields[2].decode()))
        process = subprocess.run(["git", "cat-file", "--batch"], cwd=ROOT,
                                 input=("".join(oid+"\n" for _,oid in requests)).encode(),
                                 stdout=subprocess.PIPE, check=True)
        data, offset = {}, 0
        for name,_oid in requests:
            end = process.stdout.index(b"\n", offset)
            size = int(process.stdout[offset:end].split()[-1]);offset=end+1
            data[name]=process.stdout[offset:offset+size];offset+=size+1
        return paths, data
    paths = {p.decode() for p in git("ls-files", "--cached", "--others", "--exclude-standard", "-z").split(bytes([0])) if p}
    data = {name: (ROOT/name).read_bytes() for name in paths if wanted(name)
            and (ROOT/name).is_file() and (ROOT/name).stat().st_size <= LIMIT}
    return paths, data


def markdown_text(raw):
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("cp1252")  # Legacy Windows-authored ADRs; preserve their bytes.


def validate(paths, data):
    errors, catalog, indexed = [], {}, set()
    folded = {p.casefold():p for p in paths}

    def check_target(source, target):
        if target not in paths:
            suggestion = folded.get(target.casefold())
            errors.append(source+": "+("case mismatch: "+target+"; expected "+suggestion if suggestion else "missing reference: "+target))
        return ADR_NAME.fullmatch(target)

    for name in sorted(paths):
        if not name.startswith("docs/adr/") or not name.endswith(".md") or name == "docs/adr/README.md":continue
        match = ADR_NAME.fullmatch(name)
        if not match:
            errors.append(name+": expected NNNN-kebab-case.md");continue
        number = match[1]
        if number in catalog:errors.append(name+": duplicate ADR"+number+" also in "+catalog[number])
        catalog[number] = name
        if name not in data:
            errors.append(name+": ADR missing or exceeds bounded document size")
        text = markdown_text(data.get(name,b""))
        header = next((line for line in text.splitlines() if line.startswith("# ")), "")
        explicit = re.match(r"# (?:ADR\s*)?(\d+)\b", header)
        if explicit and explicit[1] != number:errors.append(name+": title identifier disagrees with ADR"+number)

    if "docs/adr/README.md" not in data:errors.append("docs/adr/README.md: missing ADR index")
    for name,raw in sorted(data.items()):
        if name.startswith("docs/adr/"):
            text = markdown_text(raw)
            for label,target in LINK.findall(text):
                target=target.strip().strip("<>").split("#",1)[0]
                if not target or re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:",target) or target.startswith("/"):continue
                target=posixpath.normpath(posixpath.join(posixpath.dirname(name),target))
                if not target.startswith(("docs/adr/","docs/capacity/")):continue
                match=check_target(name,target)
                if name == "docs/adr/README.md" and match:
                    indexed.add(target)
                    number=re.match(r"(?:ADR\s*)?(\d{4})\b",label)
                    if number and number[1] != match[1]:errors.append(name+": index label disagrees with "+target)
        elif name.endswith(".json"):
            try:report=json.loads(raw)
            except (ValueError,UnicodeError):
                errors.append(name+": invalid compact JSON report");continue
            if not isinstance(report,dict):continue
            for key in ("decision","decisions"):
                values=report.get(key,[])
                if isinstance(values,str):values=[values]
                if not isinstance(values,list):continue
                for value in values:
                    if not isinstance(value,str):continue
                    if value.startswith("docs/adr/"):check_target(name,value)
                    identifiers=ADR_ID.findall(value)
                    slug=re.match(r"^(\d{4})-[a-z]",value)
                    if slug:identifiers.append(slug[1])
                    for number in identifiers:
                        if len(number)!=4 or number not in catalog:errors.append(name+": unknown decision ADR"+number)
    for name in sorted(set(catalog.values())-indexed):errors.append(name+": missing entry in docs/adr/README.md")
    return errors


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    mode=parser.add_mutually_exclusive_group()
    mode.add_argument("--staged",action="store_true")
    mode.add_argument("--revision")
    mode.add_argument("--all",action="store_true")
    args=parser.parse_args()
    paths,data=snapshot(args.revision,args.staged)
    errors=validate(paths,data)
    print(json.dumps({"pass":not errors,"snapshot":"staged" if args.staged else args.revision or "working",
                      "checked_documents":len(data),"compact_report_limit_bytes":LIMIT,
                      "error_count":len(errors),"errors":errors[:30]},indent=2))
    raise SystemExit(1 if errors else 0)


if __name__ == "__main__":main()
