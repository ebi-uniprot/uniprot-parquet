"""upjson2lake.nf's bin_modules() table against the bin/ scripts' imports:
a static check, so it runs without Nextflow (test_workflow.py needs it)."""

import os

from conftest import PROJECT_ROOT

WORKFLOW = os.path.join(PROJECT_ROOT, "upjson2lake.nf")


def _sibling_imports(script: str, seen: set[str] | None = None) -> set[str]:
    """The bin/ modules ``script`` imports, directly or through each other."""
    import ast
    bin_dir = os.path.join(PROJECT_ROOT, "bin")
    seen = set() if seen is None else seen
    with open(os.path.join(bin_dir, script)) as f:
        tree = ast.parse(f.read())
    for node in ast.walk(tree):
        names = ([a.name for a in node.names] if isinstance(node, ast.Import)
                 else [node.module] if isinstance(node, ast.ImportFrom) and node.module else [])
        for name in names:
            module = name.split(".")[0] + ".py"
            if module not in seen and os.path.exists(os.path.join(bin_dir, module)):
                seen.add(module)
                _sibling_imports(module, seen)
    return seen


def test_bin_modules_list_every_imported_module() -> None:
    """bin_modules(<process>) names exactly the bin/ modules the process's
    scripts import, so a change to one re-runs the process under -resume."""
    import re
    with open(WORKFLOW) as f:
        text = f.read()
    table = text[text.index("def bin_modules("):]
    table = table[:table.index("\n}\n")]
    lists = {p: set(re.findall(r"'([^']+)'", names))
             for p, names in re.findall(r"(\w+):\s*\[([^]]*)\]", table)}
    assert set(lists) == {"STREAM_JSONL", "SORT_JSONL", "INFER_SCHEMA", "STAGE_PARQUET",
                          "PARQUET_TRANSFORM", "VALIDATE", "PROVENANCE"}
    for process, listed in lists.items():
        body = text[text.index(f"process {process} {{"):]
        body = body[:body.index("\n}\n")]
        scripts = set(re.findall(r"\b(\w+\.py)\b", body))
        imported = set().union(*(_sibling_imports(s) for s in scripts)) - scripts
        assert listed == imported, f"{process}: bin_modules lists {sorted(listed)}, scripts import {sorted(imported)}"
        if listed:
            assert f"bin_digest('{process}')" in text, process
