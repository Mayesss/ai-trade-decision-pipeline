"""Revision 2 recheck of r2_integrity.py against the revised r013_run (throwaway repos; the clone's own module)."""
import json, os, py_compile, shutil, subprocess, sys, tempfile
from pathlib import Path
HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE)); sys.path.insert(0, str(Path(__file__).resolve().parent))
import r013_run as R
from r2_integrity import git, repo, ROW

def p1p2():
    for attr, cfg in (('results.jsonl -diff\n', None), ('results.jsonl diff=hide\n', ('diff.hide.textconv', 'sh -c "true"'))):
        d = repo()
        (d / 'results.jsonl').write_text(ROW + '\n'); git(d, 'commit', '-qam', 'row'); git(d, 'reset', '-q', '--hard', 'HEAD~1')
        (d / '.git' / 'info').mkdir(exist_ok=True); (d / '.git' / 'info' / 'attributes').write_text(attr)
        if cfg: git(d, 'config', *cfg)
        print(f'P1/P2 {attr.strip()!r}: erased rows still seen = {len(R.result_rows(d, "results.jsonl"))} (want 1)')
        shutil.rmtree(d)

def p3():
    d = repo(); (d / 'ct').mkdir()
    for f in ('__init__.py', 'listings013.py'):
        shutil.copy(HERE / 'ct' / f, d / 'ct' / f)
    git(d, 'add', '.'); git(d, 'commit', '-qm', 'code')
    (d / 'email').mkdir(); (d / 'email' / '__init__.py').write_text('x=1\n')
    s = d / 'ipaddress_src.py'; s.write_text('x=1\n'); py_compile.compile(str(s), cfile=str(d / 'ipaddress.pyc')); s.unlink()
    (d / 'quopri.py').write_text('x=1\n'); (d / '.git' / 'info').mkdir(exist_ok=True); (d / '.git' / 'info' / 'exclude').write_text('quopri.py\n')
    import importlib.machinery as m
    (d / 'ct' / f'listings013{m.EXTENSION_SUFFIXES[0]}').write_bytes(b'x')
    print('P3 (v) shadow failures:', [x for x in R.environment_failures(d) if 'shadow' in x])
    print('P3 loaded_from_source in this process:', R.loaded_from_source(), '| pycache_prefix:', sys.pycache_prefix)
    shutil.rmtree(d)

def p4():
    d = repo()
    saved = (dict(R.HOOKS), R.RESULTS)
    try:
        R.RESULTS = d / 'results.jsonl'
        R.HOOKS['commit'] = lambda msg: None
        try:
            R.record({'registration': R.REG.name, 'mode': 'discovery_started'}, 'm'); print('P4a record() with a no-op hook: NOT refused')
        except R.Refused as e:
            print('P4a record() with a no-op hook: refused:', str(e)[:90])
        R.RESULTS = saved[1]                     # the (clone's) real results.jsonl and data tree, mock hook
        print('P4b real tree + mock hook:', R.real_tree_failures())
        R.HOOKS.clear(); R.HOOKS.update(saved[0])
        print('P4c real tree + default hook:', R.real_tree_failures())
        R.RESULTS = d / 'results.jsonl'
        print('P4d synthetic results + real data tree:', R.real_tree_failures())
    finally:
        R.HOOKS.clear(); R.HOOKS.update(saved[0]); R.RESULTS = saved[1]
    shutil.rmtree(d)

p1p2(); p3(); p4()
