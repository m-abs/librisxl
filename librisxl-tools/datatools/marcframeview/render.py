import sys, os
import json
from jinja2 import Environment, PackageLoader

MARC_CATEGORIES = 'bib', 'auth', 'hold'

args = sys.argv[1:]
marcframe_path = args.pop(0)
with open(marcframe_path) as fp:
    marcframe = json.load(fp)

def marc_categories():
    for cat in MARC_CATEGORIES:
        yield cat, marcframe[cat]

def tags(catdfn):
    for tag, dfn in sorted(catdfn.items()):
        if tag.isdigit() and dfn:
            yield tag, dfn

def codes(dfn):
    for code, subdfn in sorted(dfn.items()):
        if code.startswith('$') and subdfn:
            yield code, subdfn

env = Environment(loader=PackageLoader(__name__, '.'),
        variable_start_string='${', variable_end_string='}',
        line_statement_prefix='%')
tplt = env.get_template('template.html')

html = tplt.render(dict(vars(__builtins__), **vars())).encode('utf-8')
sys.stdout.write(html)
