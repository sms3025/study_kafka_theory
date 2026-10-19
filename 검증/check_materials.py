"""Check local HTML resources, curriculum pairing, source snippets, SVG and JSON."""
from pathlib import Path
from html.parser import HTMLParser
from urllib.parse import unquote, urlsplit
import json
import sys
import xml.etree.ElementTree as ET

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'))
from course_content import LESSONS

class Page(HTMLParser):
    def __init__(self):
        super().__init__();self.links=[];self.ids=set();self.images=[];self.script=False;self.data='';self.sections=set()
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

failures=[]; count=0
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
        if not {'concept','flow','code','lab','check','sources'}.issubset(p.ids):failures.append(f'{path}: incomplete lesson sections')
        data=json.loads(p.data)
        if len(data['steps'])<4 or not 0<=data['correct']<3:failures.append(f'{path}: invalid interaction data')
for item in LESSONS:
    path=ROOT/'실습'/item['snippet'][0]
    source=path.read_text(encoding='utf-8')
    if f'// region {item["snippet"][1]}' not in source:failures.append(f'Missing snippet: {path}')
    if not (ROOT/'이론'/item['level']/(item['id']+'.html')).exists():failures.append(f'Missing theory {item["id"]}')
    guides=list((ROOT/'실습'/item['level']).glob(item['id']+'-*.md'))
    if len(guides)!=1:failures.append(f'Expected one lab guide: {item["id"]}')
for svg in (ROOT/'이론'/'assets').glob('*.svg'):ET.parse(svg)
if len(LESSONS)!=12:failures.append('Expected 12 lessons')
if failures:
    print('\n'.join(failures));sys.exit(1)
print(f'PASS {count} HTML files: all local links, images, anchors and lesson JSON')
print('PASS 12 theory/practice pairs, Java source regions, SVG XML')
