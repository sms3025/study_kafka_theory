"""Rebuild static offline course + lab Markdown from content and real Java snippets."""
from pathlib import Path
from html import escape as esc
from urllib.parse import quote
import json
import textwrap
import re
from course_content import LESSONS, SOURCES, ADDITIONAL_SNIPPETS

ROOT = Path(__file__).resolve().parents[1]
THEORY = ROOT / '이론'
PRACTICE = ROOT / '실습'
LESSONS = sorted(LESSONS, key=lambda x: x['id'])
LEVELS = [('초급','01','기록하고, 나누고, 읽기','메시지 한 건의 이동부터 Java 송수신까지'),('중급','02','실패해도 흐름을 지키기','커밋 · 중복 방지 · 재시도 · 복제'),('고급','03','일관성과 운영까지 설계하기','트랜잭션 · Outbox · Streams · 관측')]

def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding='utf-8')

def link(path):
    return quote(str(path).replace('\\','/'), safe='/.:#?=-')

def lesson_url(item, base=''):
    return link(f"{base}{item['level']}/{item['id']}.html")

LOGO = '''<svg width="35" height="43" viewBox="0 0 35 43" aria-hidden="true"><g fill="none" stroke="#d64d2e" stroke-width="2.2"><path d="M12 12L24 5M12 12v19m0-10l14 7m-14 3l12 7"/><circle cx="12" cy="12" r="4.5" fill="white"/><circle cx="12" cy="31" r="4.5" fill="white"/><circle cx="25" cy="5" r="3.2" fill="white"/><circle cx="27" cy="28" r="3.2" fill="white"/><circle cx="25" cy="38" r="3.2" fill="white"/></g></svg>'''

def sidebar(base, current=None):
    groups=[]
    for level, number, title, desc in LEVELS:
        entries=[]
        for item in LESSONS:
            if item['level'] != level: continue
            entries.append(f'''<a class="nav-item {'active' if current==item['id'] else ''}" {'aria-current="page"' if current==item['id'] else ''} data-lesson="{item['id']}" href="{lesson_url(item,base)}"><span class="num">{item['id']}</span><span>{esc(item['short'])}</span></a>''')
        groups.append(f'<div class="nav-group"><p><span class="dot"></span>{level} / LEVEL {number}</p>{"".join(entries)}</div>')
    return f'''<aside class="sidebar" aria-label="학습 목차"><a class="brand" href="{base}index.html">{LOGO}<span>Kafka Field Guide<small>원리에서 실행까지</small></span></a><p class="nav-label">YOUR LEARNING PATH</p><a class="overview-link {'active' if current is None else ''}" href="{base}index.html">⌘ &nbsp; 전체 학습 로드맵</a><nav>{''.join(groups)}</nav><div class="sidebar-bottom">나의 학습 진행 <b data-progress-count>0 / 12</b><div class="mini-progress"><span data-progress-bar></span></div><p data-storage-note class="storage-note">학습 기록은 이 브라우저에만 저장됩니다.</p><span>Java 21 · Kafka 4.3.1 · Docker</span></div></aside>'''

def page(title, body, base='', current=None):
    lab='../실습/README.md' if not base else '../../실습/README.md'
    result = f'''<!doctype html>
<html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="description" content="Kafka 초급·중급·고급 학습. 데이터 흐름 그림, 실무 패턴 코드와 Docker 실습을 연결한 로컬 가이드."><meta name="theme-color" content="#182c30"><title>{esc(title)} · Kafka Field Guide</title><link rel="icon" href="{base}assets/icon.svg" type="image/svg+xml"><link rel="stylesheet" href="{base}assets/style.css"><script defer src="{base}assets/app.js"></script></head>
<body><a href="#main" class="skip">본문으로 건너뛰기</a>{sidebar(base,current)}<div class="shell"><header class="topbar"><button class="menu-toggle" aria-label="학습 목차 열기" aria-expanded="false">☰</button><span class="edition">APACHE KAFKA / STUDY EDITION 01</span><div class="top-links"><a href="{base}index.html">학습 로드맵</a><a href="{link(lab)}">실습 시작 안내 ↗</a></div></header><main id="main" class="content">{body}<footer class="footer"><span>Kafka Field Guide · 개념을 읽고, 흐름을 보고, 코드로 확인합니다.</span><span>OFFLINE READY / 2026.10</span></footer></main></div></body></html>'''

    return result.replace('</aside><div class="shell">','<button class="sidebar-close" data-menu-close aria-label="학습 목차 닫기">×</button></aside><div class="shell">',1)

def codeblock(code, label='Java 21'):
    rendered=esc(code)
    if 'PowerShell' not in label:
        pattern=r'(?P<comment>//[^\n]*|/\*[\s\S]*?\*/)|(?P<string>"""[\s\S]*?"""|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\')|(?P<keyword>\b(?:public|private|static|final|void|var|int|long|boolean|if|else|for|while|try|catch|throw|throws|new|return|true|false|null|class|record)\b)'
        parts=[];last=0
        for token in re.finditer(pattern,code):
            parts.extend([esc(code[last:token.start()]),f'<span class="syntax-{token.lastgroup}">{esc(token.group())}</span>']);last=token.end()
        parts.append(esc(code[last:]));rendered=''.join(parts)
    return f'<div class="code-block"><div class="code-head"><span>{esc(label)}</span><button class="copy" data-copy aria-label="코드 복사">복사</button></div><pre><code>{rendered}</code></pre></div>'

def extra_code(item):
    result=[]
    for source,region,title,description in ADDITIONAL_SNIPPETS.get(item['id'],[]):
        lines=(PRACTICE/source).read_text(encoding='utf-8').splitlines()
        start=next(i for i,line in enumerate(lines) if '// region '+region in line)+1
        end=next(i for i in range(start,len(lines)) if '// endregion '+region in lines[i])
        snippet=textwrap.dedent('\n'.join(lines[start:end]))
        result.append(f'<div class="concept-block"><h3>{esc(title)}</h3><p>{esc(description)}</p></div>'+codeblock(snippet,Path(source).name+' · Java 21')+f'<p class="code-source"><a href="{link("../../실습/"+source)}">{esc(source)}</a> · {start+1}행부터</p>')
    return ''.join(result)

def svg(item):
    nodes=item['nodes']; edges=item['edges']; w=225; h=76
    elements=['<svg xmlns="http://www.w3.org/2000/svg" width="920" height="410" viewBox="0 0 920 410" role="img" aria-labelledby="title desc">',f'<title id="title">{esc(item["short"])} 데이터 흐름</title><desc id="desc">{esc(" → ".join(x[0] for x in nodes))}. {esc(item["example"])}</desc>', '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10" fill="none" stroke="#758c80" stroke-width="1.5"/></marker><pattern id="dots" width="20" height="20" patternUnits="userSpaceOnUse"><circle cx="1" cy="1" r="1" fill="#dfe7dc"/></pattern></defs><rect width="920" height="410" fill="#f4f7ef"/><rect width="920" height="410" fill="url(#dots)"/>']
    for a,b,label in edges:
        _,_,ax,ay=nodes[a]; _,_,bx,by=nodes[b]
        dx=bx-ax; dy=by-ay
        if abs(dx)>abs(dy):
            x1=ax+(w if dx>0 else 0); y1=ay+h/2; x2=bx+(0 if dx>0 else w); y2=by+h/2
            path=f'M{x1} {y1} C{(x1+x2)/2} {y1} {(x1+x2)/2} {y2} {x2} {y2}'
        else:
            x1=ax+w/2;y1=ay+(h if dy>0 else 0);x2=bx+w/2;y2=by+(0 if dy>0 else h)
            path=f'M{x1} {y1} C{x1} {(y1+y2)/2} {x2} {(y1+y2)/2} {x2} {y2}'
        elements.append(f'<path d="{path}" fill="none" stroke="#8b9e8c" stroke-width="1.7" marker-end="url(#arrow)"/>')
        if label:
            cx=(x1+x2)/2;cy=(y1+y2)/2-13;lw=max(48,len(label)*9)
            elements.append(f'<rect x="{cx-lw/2}" y="{cy-13}" width="{lw}" height="20" rx="4" fill="#f4f7ef"/><text x="{cx}" y="{cy+1}" text-anchor="middle" font-size="10" fill="#637c6a" font-family="Segoe UI,Malgun Gothic,sans-serif">{esc(label)}</text>')
    for i,(name,detail,x,y) in enumerate(nodes):
        bg='#182c30' if i==0 else '#fff';fg='#fff' if i==0 else '#223e3b';muted='#b2c6bb' if i==0 else '#6f8176'
        elements.append(f'<g><rect x="{x}" y="{y}" width="{w}" height="{h}" rx="11" fill="{bg}" stroke="#cbd8c6"/><circle cx="{x+21}" cy="{y+23}" r="10" fill="#e8eed8"/><text x="{x+21}" y="{y+27}" text-anchor="middle" font-size="10" fill="#587046" font-family="Consolas,monospace">{i+1}</text><text x="{x+40}" y="{y+28}" fill="{fg}" font-weight="600" font-size="14" font-family="Segoe UI,Malgun Gothic,sans-serif">{esc(name)}</text><text x="{x+20}" y="{y+55}" fill="{muted}" font-size="11" font-family="Segoe UI,Malgun Gothic,sans-serif">{esc(detail)}</text></g>')
    elements.append('<text x="885" y="396" text-anchor="end" fill="#879587" font-family="Consolas,monospace" font-size="9">KAFKA FIELD GUIDE / CONCEPTUAL FLOW</text></svg>')
    return ''.join(elements)

def tool(item):
    n=item['id']; controls=''; kind=''; title=''; note='설명용 메모리 모델입니다. 실제 Kafka 동작은 아래 실습에서 확인합니다.'
    if n=='03':
        kind='groups';title='Consumer 수를 바꿔 보세요';controls='<label>Consumer 수 <input type="number" value="2" min="1" max="5" aria-label="Consumer 수"></label>';note='파티션은 3개입니다. 예시 분배이며 실제 assignor의 배정 결과와 다를 수 있습니다. 일반 Consumer Group 기준입니다.'
    if n=='05':
        kind='offset';title='처리 후 커밋 전에 장애가 나면?';controls=''.join(f'<button data-action="{a}">{t}</button>' for a,t in [('process','다음 레코드 처리'),('commit','처리 위치 커밋'),('restart','장애 후 재시작'),('reset','초기화')])
    if n=='08':
        kind='replication';title='정상 노드 수와 쓰기 조건';controls='<label>정상 노드 <input type="number" value="3" min="1" max="3" aria-label="정상 노드 수"></label>';note='Follower가 동기화된 정상 상태를 가정한 combined 구성의 설명용 모델입니다. 실제 장애 판정에는 시간이 걸립니다.'
    if n=='09':
        kind='transaction';title='기록되었다고 바로 보이지는 않습니다';controls=''.join(f'<button data-action="{a}">{t}</button>' for a,t in [('begin','트랜잭션 시작 · 전송'),('commit','Commit'),('abort','Abort'),('reset','초기화')])
    if n=='10':
        kind='outbox';title='같은 이벤트가 재발행되는 구간';controls=''.join(f'<button data-action="{a}">{t}</button>' for a,t in [('save','① 주문 + Outbox 저장'),('send','② Relay 전송'),('crash','장애 · Relay 재시작'),('mark','③ 완료 표식'),('reset','초기화')]);note='① → ② → 장애 → ② 순서로 누르면 같은 eventId의 기록이 2건이 됩니다. 설명용 모델입니다.'
    if not kind:return ''
    return f'<div class="flow-tool" data-tool="{kind}"><h3>{title}</h3><div class="tool-controls">{controls}</div><p class="tool-output" aria-live="polite"></p><p>{note}</p></div>'

def render_lesson(item):
    n=item['id'];lvl=item['level']; idx=LESSONS.index(item)
    source,region=item['snippet']; path=PRACTICE/source
    lines=path.read_text(encoding='utf-8').splitlines()
    start=next(i for i,line in enumerate(lines) if '// region '+region in line)+1
    end=next(i for i in range(start,len(lines)) if '// endregion '+region in lines[i])
    snippet=textwrap.dedent('\n'.join(lines[start:end]))
    concepts=''.join(f'<div class="concept-block"><h3>{esc(t)}</h3><p>{esc(p)}</p></div>' for t,p in item['concept'])
    goals=''.join(f'<li>{esc(g)}</li>' for g in item['goals'])
    notes=''.join(f'<li>{esc(x)}</li>' for x in item['explain'])
    labs=''.join(f'<div class="lab-step"><h3>{i+1:02d}. {esc(t)}</h3>{codeblock(c,"PowerShell · 저장소 루트에서 실행")}<p class="expected">{esc(e)}</p></div>' for i,(t,c,e) in enumerate(item['commands']))
    experiments=''.join(f'<li>{esc(x)}</li>' for x in item['experiment'])
    question,answers,correct,explanation=item['quiz']
    options=''.join(f'<button data-answer="{i}" aria-pressed="false">{chr(65+i)}. {esc(answer)}</button>' for i,answer in enumerate(answers))
    references=''.join(f'<li><a href="{SOURCES[key][1]}" target="_blank" rel="noreferrer">{esc(SOURCES[key][0])} ↗</a></li>' for key in item['sources'])
    before=f'<a href="{lesson_url(LESSONS[idx-1],"../")}"><small>← 이전 단원</small>{esc(LESSONS[idx-1]["title"])}</a>' if idx else '<a href="../index.html"><small>← 시작 페이지</small>전체 학습 로드맵</a>'
    after=f'<a href="{lesson_url(LESSONS[idx+1],"../")}"><small>다음 단원 →</small>{esc(LESSONS[idx+1]["title"])}</a>' if idx<len(LESSONS)-1 else '<a href="../../%EC%8B%A4%EC%8A%B5/%EC%B5%9C%EC%A2%85-%ED%94%84%EB%A1%9C%EC%A0%9D%ED%8A%B8.md"><small>다음 도전 →</small>최종 프로젝트와 복구 실험</a>'
    labfile=f'{n}-{item["short"].split(" · ")[0].replace(" ","-")}.md'
    item['labfile']=labfile
    snippet_url=link('../../실습/'+source)
    step_nodes=''.join('<span class="sim-node"></span>' for _ in item['steps'])
    data=json.dumps(dict(steps=item['steps'],correct=correct,explanation=explanation),ensure_ascii=False).replace('</','<\\/')
    body=f'''<div class="breadcrumb"><a href="../index.html">로드맵</a> / {lvl} / LESSON {n}</div><section class="lesson-hero"><div class="eyebrow">LESSON {n} / {lvl}</div><h1>{esc(item['title'])}</h1><p>{esc(item['desc'])}</p><div class="lesson-meta"><span class="pill accent">{esc(item['short'])}</span><span class="pill">이론 + 실습 약 {item['minutes']}분</span><span class="pill">Java 21 · Kafka 4.3.1</span></div></section>
<div class="lesson-layout"><article class="lesson-body"><div class="goals"><p>이 단원을 마치면</p><ul>{goals}</ul></div>
<section id="concept" class="lesson-section"><h2><span class="section-no">01 / UNDERSTAND</span>먼저, 원리를 이해합니다.</h2>{concepts}<aside class="example"><strong>예시로 생각하기</strong><p>{esc(item['example'])}</p></aside></section>
<section id="flow" class="lesson-section"><h2><span class="section-no">02 / FOLLOW THE EVENT</span>데이터의 이동을 따라갑니다.</h2><figure class="diagram"><img src="../assets/flow-{n}.svg" width="920" height="410" alt="{esc(item['short'])} 데이터 흐름: {esc(' → '.join(x[0] for x in item['nodes']))}"><figcaption>설명용 구조도 · 번호는 구성 요소를 구분합니다. &nbsp;<a class="diagram-download" href="../assets/flow-{n}.svg" download>SVG 이미지 저장 ↓</a></figcaption></figure><div class="simulation"><div class="sim-head"><b>FLOW PLAYER</b><span data-step-count>1 / {len(item['steps'])}</span></div><div class="sim-track" aria-hidden="true">{step_nodes}</div><p class="sim-message" data-step-message aria-live="polite">{esc(item['steps'][0])}</p><div class="sim-controls"><button data-step="prev">이전</button><button class="primary" data-step="next">다음 단계 →</button><button data-step="play">자동 재생</button><button data-step="reset">처음부터</button></div><p class="sim-small">원리를 이해하기 위한 단계 모델입니다. 네트워크 시간이나 장애 판정을 재현하지 않습니다.</p></div>{tool(item)}</section>
<section id="code" class="lesson-section"><h2><span class="section-no">03 / READ THE CODE</span>현업 패턴을 코드로 읽습니다.</h2><p class="lab-intro">아래 코드는 함께 제공된 실행 소스에서 발췌했습니다. 전체 클래스와 설정은 소스 파일에서 확인할 수 있습니다.</p>{codeblock(snippet,Path(source).name+' · Java 21')}<p class="code-source">소스: <a href="{snippet_url}">{esc(source)}</a> · {start+1}행부터</p><ul class="code-notes">{notes}</ul><aside class="callout"><b>오해하기 쉬운 지점</b>{esc(item['pitfall'])}</aside></section>
<section id="lab" class="lesson-section"><h2><span class="section-no">04 / MAKE IT REAL</span>이제, 직접 확인합니다.</h2><p class="lab-intro">PowerShell에서 저장소 루트로 이동한 뒤 실행하세요. 01단원의 up → build → init이 선행되어야 합니다. 종료된 환경은 up으로 다시 시작합니다. 이후 단원의 max=0은 입력이 끊긴 뒤 25초까지 읽습니다. &nbsp;<a href="{link('../../실습/'+lvl+'/'+labfile)}">대응 실습 안내 파일 ↗</a></p>{labs}<div class="experiment"><h3>한 걸음 더 실험하기</h3><ul>{experiments}</ul></div></section>
<section id="check" class="lesson-section"><h2><span class="section-no">05 / CHECK YOUR UNDERSTANDING</span>설명할 수 있으면, 내 지식입니다.</h2><div class="quiz"><h3>{esc(question)}</h3><div class="quiz-options">{options}</div><p class="quiz-feedback" aria-live="polite"></p></div><div class="completion"><p>원리를 읽고 실습 결과를 확인했나요?<br><span class="storage-note" data-storage-note>학습 기록은 이 브라우저에만 저장됩니다.</span></p><button class="button complete-button" data-complete="{n}" aria-pressed="false">이 단원 학습 완료</button></div></section>
<section id="sources" class="lesson-section"><h2><span class="section-no">06 / GO TO THE SOURCE</span>공식 문서로 더 알아보기</h2><ul class="source-list">{references}</ul></section><nav class="next-prev" aria-label="이전 다음 단원">{before}{after}</nav></article><aside class="toc" aria-label="페이지 목차"><p>IN THIS LESSON</p><a href="#concept">01 개념과 원리</a><a href="#flow">02 데이터 흐름</a><a href="#code">03 코드 해설</a><a href="#lab">04 실습하기</a><a href="#check">05 이해도 확인</a><a href="#sources">06 공식 자료</a></aside></div><script type="application/json" id="lesson-data">{data}</script>'''
    body=body.replace('<aside class="callout">',extra_code(item)+'<aside class="callout">',1)
    body=body.replace('SVG 이미지 저장 ↓</a>',f'SVG 이미지 저장 ↓</a> &nbsp; <a href="../assets/flow-{n}.svg" target="_blank" rel="noreferrer">크게 보기 ↗</a>')
    put(THEORY/lvl/f'{n}.html',page(item['title'],body,'../',n))
    put(THEORY/'assets'/f'flow-{n}.svg',svg(item))
    guide=f'# {n}. {item["short"]}\n\n{item["desc"]}\n\n먼저 [대응 이론 HTML](../../이론/{lvl}/{n}.html)을 읽으세요.\n\n## 준비\n\n- 저장소 루트의 PowerShell에서 실행합니다.\n- 01단원의 up → build → init을 완료해야 합니다.\n- 기존 기록은 유지됩니다. 새 실험은 고유 group / runId를 사용하세요.\n- `max=0`은 무한 상주가 아니라 입력이 끊긴 뒤 25초까지 읽는 모드입니다.\n\n## 학습 목표\n\n'+''.join('- '+x+'\n' for x in item['goals'])
    guide+='\n## 실행과 관찰\n\n'
    for i,(t,c,e) in enumerate(item['commands']):guide+=f'### {i+1}. {t}\n\n```powershell\n{c}\n```\n\n예상 결과: {e}\n\n'
    guide+='## 더 실험하기\n\n'+''.join('- '+x+'\n' for x in item['experiment'])+'\n## 주의할 경계\n\n'+item['pitfall']+'\n\n## 읽을 소스\n\n'+f'- [발췌한 전체 소스](../{source})\n\n## 기록할 것\n\n| 예상 결과 | 실제 결과 | 차이의 이유 |\n|---|---|---|\n| 직접 작성 | 직접 작성 | 직접 작성 |\n\n## 완료 기준\n\n'+''.join('- [ ] '+x+'\n' for x in item['goals'])+'\n'+f'이해도 확인: {question}\n\n<details><summary>답 확인</summary>\n\n{answers[correct]} — {explanation}\n\n</details>\n'
    put(PRACTICE/lvl/labfile,guide)

def render_home():
    levels=[]
    for lvl,num,title,desc in LEVELS:
        rows=[]
        for item in LESSONS:
            if item['level']!=lvl:continue
            rows.append(f'<a class="lesson-row" data-lesson="{item["id"]}" href="{lesson_url(item)}"><span class="row-no">{item["id"]}</span><span><h4>{esc(item["title"])}</h4><p>{esc(item["short"])}</p></span><span class="duration">{item["minutes"]} MIN</span><span class="row-arrow" aria-hidden="true">↗</span></a>')
        levels.append(f'<section class="level-card"><div class="level-head"><span class="level-badge"><small>LEVEL</small><b>{num}</b></span><div><h3>{lvl} · {title}</h3><p>{desc}</p></div><span>4 LESSONS</span></div>{"".join(rows)}</section>')
    hero_svg='''<svg viewBox="0 0 560 330" role="img" aria-labelledby="hero-title"><title id="hero-title">이벤트가 Producer에서 Kafka 파티션을 거쳐 독립된 두 Consumer Group으로 흐르는 구조</title><defs><pattern id="grid" width="20" height="20" patternUnits="userSpaceOnUse"><circle cx="1" cy="1" r="1" fill="#c8d5c3"/></pattern><marker id="ha" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="5" markerHeight="5" orient="auto"><path d="M0 0L8 4L0 8" fill="none" stroke="#8c9e84"/></marker></defs><rect width="560" height="330" fill="url(#grid)"/><g fill="none" stroke="#91a284" stroke-width="1.5" marker-end="url(#ha)"><path d="M108 165H168"/><path d="M364 145C402 145 385 87 420 87"/><path d="M364 185C402 185 385 246 420 246"/></g><rect x="12" y="128" width="98" height="74" rx="12" fill="#fff" stroke="#c9d4c2"/><text x="61" y="158" text-anchor="middle" font-size="11" fill="#223b32" font-family="Consolas,monospace">PRODUCER</text><text x="61" y="180" text-anchor="middle" font-size="10" fill="#728367" font-family="Malgun Gothic,sans-serif">주문 생성</text><rect x="170" y="63" width="195" height="205" rx="14" fill="#182c30"/><text x="189" y="91" fill="#eef2e9" font-size="11" font-family="Consolas,monospace">KAFKA / orders</text><text x="189" y="111" fill="#99b09d" font-size="9" font-family="Consolas,monospace">APPEND-ONLY LOG</text><g fill="#2c4547"><rect x="188" y="128" width="159" height="30" rx="5"/><rect x="188" y="169" width="159" height="30" rx="5"/><rect x="188" y="210" width="159" height="30" rx="5"/></g><g fill="#b6c9b5" font-size="9" font-family="Consolas,monospace"><text x="196" y="147">P0</text><text x="196" y="188">P1</text><text x="196" y="229">P2</text></g><g fill="#b6ca84"><rect x="223" y="136" width="18" height="14" rx="3"/><rect x="246" y="136" width="18" height="14" rx="3"/><rect x="269" y="136" width="18" height="14" rx="3"/><rect x="223" y="177" width="18" height="14" rx="3"/><rect x="246" y="177" width="18" height="14" rx="3"/><rect x="223" y="218" width="18" height="14" rx="3"/></g><rect x="292" y="136" width="18" height="14" rx="3" fill="#ee8157"/><rect x="420" y="50" width="127" height="74" rx="12" fill="#fff" stroke="#c9d4c2"/><rect x="420" y="210" width="127" height="74" rx="12" fill="#fff" stroke="#c9d4c2"/><g text-anchor="middle" font-family="Consolas,Malgun Gothic,sans-serif"><text x="483" y="79" font-size="11" fill="#294637">GROUP A</text><text x="483" y="101" font-size="10" fill="#728367">주문 알림</text><text x="483" y="240" font-size="11" fill="#294637">GROUP B</text><text x="483" y="262" font-size="10" fill="#728367">매출 집계</text></g><circle cx="137" cy="165" r="5" fill="#d96c43"/><circle cx="395" cy="108" r="4" fill="#d96c43"/><circle cx="395" cy="218" r="4" fill="#d96c43"/><text x="266" y="299" text-anchor="middle" font-size="9" fill="#6a7e60" font-family="Consolas,monospace">STORE ONCE. READ INDEPENDENTLY.</text></svg>'''
    body=f'''<section class="hero"><div><div class="eyebrow">LEARN BY FOLLOWING THE EVENT</div><h1>메시지 하나로,<br><em>분산 시스템을</em><br>이해하다.</h1><p>Kafka가 처음이어도 괜찮습니다.<br>주문 이벤트 하나를 따라 원리를 읽고, 흐름을 보고,<br>직접 코드를 실행하며 단계적으로 익혀 보세요.</p><div class="hero-actions"><a class="button" data-resume href="{lesson_url(LESSONS[0])}">01. 첫 학습 시작하기 ↗</a><a class="button secondary" href="#curriculum">전체 과정 살펴보기 ↓</a></div></div><div class="hero-art"><p class="art-label">ANATOMY OF AN EVENT</p>{hero_svg}<div class="art-footer"><span>evt-001 / order-001</span><span>PRODUCE → STORE → CONSUME</span></div></div></section><div class="stats"><div class="stat"><strong>03</strong><small>초급 → 중급 → 고급</small></div><div class="stat"><strong>12</strong><small>이론과 실습을 연결한 단원</small></div><div class="stat"><strong>12</strong><small>데이터 흐름 SVG 이미지</small></div><div class="stat"><strong data-progress-count>0 / 12</strong><small>완료한 단원 · 브라우저 저장</small></div></div><section id="curriculum"><div class="section-heading"><div><span class="section-no">THE LEARNING PATH</span><h2>원리에서 실행까지, 한 단계씩.</h2><p>각 단원은 개념 → 흐름 그림 → 코드 → 실습 → 이해도 확인으로 이어집니다.</p></div><div class="search-wrap"><label class="sr-label" for="search">단원 검색</label><input class="search" id="search" data-search placeholder="예: 파티션, Outbox, 트랜잭션" type="search"><div class="search-feedback" role="status"></div></div></div><div class="curriculum-list">{''.join(levels)}</div></section><div class="bottom-grid"><section class="note-card"><span class="section-no">BEFORE YOU START</span><h3>준비는 가볍게, 관찰은 꼼꼼하게.</h3><p>JSON과 기본 프로그래밍 지식, 실행 중인 Docker Desktop이면 시작할 수 있습니다. Java 21과 Maven은 컨테이너로 제공합니다.</p><ol><li>예상 결과를 먼저 적습니다.</li><li>실행 로그의 key · partition · offset을 비교합니다.</li><li>장애를 만들고 재시작해 결과를 설명합니다.</li></ol><p>12단원 첫 학습 약 11.5시간 + 반복 실험·최종 프로젝트. 권장 속도는 주 2단원, 총 6주입니다. 설치·디버깅 시간은 별도입니다.</p></section><section class="note-card dark"><span class="section-no">ONE PROJECT, MANY LESSONS</span><h3>주문 이벤트 하나로 끝까지 연결합니다.</h3><p>OrderEvent → Producer → Kafka → Consumer.<br>여기에 중복 방지, 실패 기록, DB Outbox와 Streams를 더해, 정상 동작과 복구를 함께 익힙니다.</p><p><code>이론/초급 · 중급 · 고급</code><br><code>실습/초급 · 중급 · 고급</code></p><p><a href="{link('../실습/README.md')}">실습 환경과 명령 안내 ↗</a> &nbsp; <a href="{link('../실습/최종-프로젝트.md')}">최종 프로젝트 ↗</a></p></section></div>'''
    put(THEORY/'index.html',page('학습 로드맵',body))

def main():
    assert len(LESSONS)==12 and len({x['id'] for x in LESSONS})==12
    for item in LESSONS: render_lesson(item)
    render_home()
    for lvl,num,title,desc in LEVELS:
        items=[x for x in LESSONS if x['level']==lvl]
        put(PRACTICE/lvl/'README.md',f'# {lvl} — {title}\n\n{desc}\n\n'+''.join(f'- [{x["id"]}. {x["short"]}]({x["labfile"]})\n' for x in items)+'\n단원마다 먼저 대응 이론 HTML을 읽고, 실행 → 예상 결과 비교 → 추가 실험 순서로 진행합니다.\n')
    put(THEORY/'assets'/'icon.svg','<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 48 48"><rect width="48" height="48" rx="12" fill="#182c30"/><g stroke="#e87a50" stroke-width="3" fill="#182c30"><path d="M17 13v23m0-18l17-8m-17 15l17 10"/><circle cx="17" cy="15" r="5"/><circle cx="17" cy="34" r="5"/><circle cx="34" cy="10" r="4"/><circle cx="34" cy="35" r="4"/></g></svg>')
    put(ROOT/'index.html','<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta http-equiv="refresh" content="0;url=이론/index.html"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Kafka 학습 시작</title></head><body><p><a href="이론/index.html">Kafka Field Guide 학습 페이지 열기 →</a></p></body></html>')
    print(f'Built {len(LESSONS)} lessons, SVG diagrams, and matching practice guides.')

if __name__=='__main__': main()
