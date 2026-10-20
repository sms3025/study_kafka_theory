"""Check local HTML resources, curriculum pairing, source snippets, SVG, JSON and lesson content blocks."""
from pathlib import Path
from html.parser import HTMLParser
from urllib.parse import unquote, urlsplit
import json
import re
import sys
import xml.etree.ElementTree as ET

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'))
from course_content import LESSONS, APPENDICES, SOURCES

class Page(HTMLParser):
    def __init__(self):
        super().__init__();self.links=[];self.ids=set();self.images=[];self.script=False;self.data=''
    def handle_starttag(self,tag,attrs):
        a=dict(attrs)
        if 'id' in a:self.ids.add(a['id'])
        if 'href' in a:self.links.append(a['href'])
        if 'src' in a:self.links.append(a['src'])
        if tag=='img':self.images.append(a)
        if tag=='script' and a.get('id')=='lesson-data':self.script=True
    def handle_endtag(self,tag):
        if tag=='script':self.script=False
    def handle_data(self,data):
        if self.script:self.data+=data

LESSON_SECTIONS={'concept','flow','code','lab','apply','check','sources'}
failures=[]; warnings=[]; count=0
pages=list((ROOT/'이론').rglob('*.html'))+[ROOT/'index.html']
for path in pages:
    p=Page();p.feed(path.read_text(encoding='utf-8'));count+=1
    for raw in p.links:
        u=urlsplit(raw)
        if u.scheme or u.netloc:continue
        target=(path.parent/unquote(u.path)).resolve() if u.path else path
        if not target.exists():failures.append(f'{path.relative_to(ROOT)}: missing {raw}')
        if u.fragment and not u.path and unquote(u.fragment) not in p.ids:failures.append(f'{path}: missing #{u.fragment}')
    for image in p.images:
        if not image.get('alt'):failures.append(f'{path}: image without alt')
    if path.stem.isdigit():
        if not LESSON_SECTIONS.issubset(p.ids):failures.append(f'{path}: incomplete lesson sections {sorted(LESSON_SECTIONS-p.ids)}')
        data=json.loads(p.data)
        if len(data['steps'])<4 or not 0<=data['correct']<3:failures.append(f'{path}: invalid interaction data')

def texts(obj):
    if isinstance(obj,str):yield obj
    elif isinstance(obj,dict):
        for v in obj.values():yield from texts(v)
    elif isinstance(obj,(list,tuple)):
        for v in obj:yield from texts(v)

def prose(text):
    return re.sub(r'`[^`]*`','',re.sub(r'```[\s\S]*?```','',text))

for item in LESSONS:
    n=item['id']
    path=ROOT/'실습'/item['snippet'][0]
    source=path.read_text(encoding='utf-8')
    if f'// region {item["snippet"][1]}' not in source:failures.append(f'Missing snippet: {path}')
    if not (ROOT/'이론'/item['level']/(n+'.html')).exists():failures.append(f'Missing theory {n}')
    guides=list((ROOT/'실습'/item['level']).glob(n+'-*.md'))
    if len(guides)!=1:failures.append(f'Expected one lab guide: {n}')
    # Study edition 02: every lesson carries the richer explanation blocks.
    for key in ('why','example','logcase'):
        if not item.get(key):failures.append(f'Lesson {n}: missing {key}')
    if not (item.get('analogy') or ('',''))[0]:failures.append(f'Lesson {n}: missing analogy')
    if len(item['concept'])<4:failures.append(f'Lesson {n}: expected at least 4 concept blocks')
    if len(item.get('deep',[]))<2:failures.append(f'Lesson {n}: expected at least 2 deep blocks')
    if len(item.get('summary',[]))<4:failures.append(f'Lesson {n}: expected at least 4 summary items')
    if len(item.get('faq',[]))<3:failures.append(f'Lesson {n}: expected at least 3 FAQ items')
    if not item.get('scenario') and not item.get('tables'):failures.append(f'Lesson {n}: expected a scenario or table')
    for t in ([item['scenario']] if item.get('scenario') else [])+list(item.get('tables',[])):
        if any(len(row)!=len(t['columns']) for row in t['rows']):failures.append(f'Lesson {n}: table row width mismatch in {t["title"]}')
    for command in item['commands']:
        if len(command)<4 or not command[3].strip():failures.append(f'Lesson {n}: command without result interpretation: {command[0]}')
    for key in item['sources']:
        if key not in SOURCES:failures.append(f'Lesson {n}: unknown source {key}')
    for text in texts({k:v for k,v in item.items() if k not in ('nodes','edges','snippet','sources')}):
        if text.count('```')%2:failures.append(f'Lesson {n}: unbalanced code fence')
        if '—' in prose(text):warnings.append(f'Lesson {n}: em dash in prose')

slugs=[a['slug'] for a in APPENDICES]
for slug in slugs:
    if not (ROOT/'이론'/'부록'/f'{slug}.html').exists():failures.append(f'Missing appendix page {slug}')
for svg in (ROOT/'이론'/'assets').glob('*.svg'):ET.parse(svg)
if len(LESSONS)!=12:failures.append('Expected 12 lessons')
if failures:
    print('\n'.join(failures));sys.exit(1)
for w in warnings:print('WARN',w)
print(f'PASS {count} HTML files: all local links, images, anchors and lesson JSON')
print('PASS 12 theory/practice pairs, Java source regions, SVG XML')
print(f'PASS 12 lessons with why/analogy/concept/deep/summary/FAQ/log-pipeline blocks and result interpretation; {len(APPENDICES)} appendix pages')
