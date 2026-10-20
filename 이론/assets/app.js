(function () {
  'use strict';
  var storeKey = 'kafka-field-guide-v1';
  var done = [];
  var storageAvailable = true;
  try { var saved = JSON.parse(localStorage.getItem(storeKey) || '[]'); if (Array.isArray(saved)) done = saved.filter(function(x){return /^\d{2}$/.test(x);}); }
  catch (_) { storageAvailable = false; }
  function save() { try { localStorage.setItem(storeKey, JSON.stringify(done)); } catch (_) { storageAvailable = false; } }
  function progress() {
    document.querySelectorAll('[data-lesson]').forEach(function (link) { link.classList.toggle('done', done.includes(link.dataset.lesson)); });
    document.querySelectorAll('[data-progress-count]').forEach(function (el) { el.textContent = done.length + ' / 12'; });
    document.querySelectorAll('[data-progress-bar]').forEach(function (el) { el.style.width = (done.length / 12 * 100) + '%'; });
    var button = document.querySelector('[data-complete]');
    if (button) { var isDone = done.includes(button.dataset.complete); button.setAttribute('aria-pressed', String(isDone)); button.textContent = isDone ? '✓ 학습 완료 · 취소' : '이 단원 학습 완료'; }
    document.querySelectorAll('[data-storage-note]').forEach(function(el){el.textContent=storageAvailable?'학습 기록은 이 브라우저에만 저장됩니다.':'브라우저 저장소를 사용할 수 없어, 이 화면에서만 기록합니다.';});
    var resume = document.querySelector('[data-resume]');
    var catalogue = document.querySelectorAll('.lesson-row');
    if (resume && catalogue.length) {
      var target = Array.from(catalogue).find(function(el){return !done.includes(el.dataset.lesson);}) || catalogue[0];
      resume.href = target.href;
      resume.textContent = done.length ? '이어서 학습하기 ↗' : '01. 첫 학습 시작하기 ↗';
    }
  }
  progress();
  document.querySelectorAll('[data-complete]').forEach(function(button){button.addEventListener('click',function(){var id=button.dataset.complete;done=done.includes(id)?done.filter(function(x){return x!==id;}):done.concat(id);save();progress();});});
  var menu = document.querySelector('.menu-toggle');
  if (menu) {
    menu.addEventListener('click',function(){var open=document.querySelector('.sidebar').classList.toggle('open');menu.setAttribute('aria-expanded',String(open));});
    var closeMenu=document.querySelector('[data-menu-close]');
    if(closeMenu)closeMenu.addEventListener('click',function(){document.querySelector('.sidebar').classList.remove('open');menu.setAttribute('aria-expanded','false');menu.focus();});
    document.addEventListener('keydown',function(e){if(e.key==='Escape'){document.querySelector('.sidebar').classList.remove('open');menu.setAttribute('aria-expanded','false');}});
  }
  document.querySelectorAll('[data-search]').forEach(function(input){input.addEventListener('input',function(){var query=input.value.trim().toLocaleLowerCase();var visible=0;document.querySelectorAll('.lesson-row, .appendix-row').forEach(function(row){row.hidden=!row.textContent.toLocaleLowerCase().includes(query);if(!row.hidden)visible++;});document.querySelectorAll('.level-card').forEach(function(card){card.hidden=Array.from(card.querySelectorAll('.lesson-row, .appendix-row')).every(function(row){return row.hidden;});});document.querySelector('.search-feedback').textContent=query?(visible?visible+'개 항목을 찾았습니다.':'검색 결과가 없습니다. 다른 단어를 입력하세요.') : '';});});
  document.querySelectorAll('[data-copy]').forEach(function(button){button.addEventListener('click',async function(){var value=button.closest('.code-block').querySelector('code').textContent;try{if(navigator.clipboard && window.isSecureContext){await navigator.clipboard.writeText(value);}else{var area=document.createElement('textarea');area.value=value;area.style.position='fixed';area.style.opacity='0';document.body.appendChild(area);area.select();var copied=document.execCommand('copy');area.remove();if(!copied)throw Error('copy');}button.textContent='복사됨 ✓';}catch(_){button.textContent='직접 선택해 복사';}setTimeout(function(){button.textContent='복사';},2000);});});
  document.querySelectorAll('[data-term-search]').forEach(function(input){var feedback=document.querySelector('.term-feedback');input.addEventListener('input',function(){var query=input.value.trim().toLocaleLowerCase();var visible=0;document.querySelectorAll('[data-term]').forEach(function(term){term.hidden=query!==''&&!term.textContent.toLocaleLowerCase().includes(query);if(!term.hidden)visible++;});document.querySelectorAll('[data-term-group]').forEach(function(group){group.hidden=Array.from(group.querySelectorAll('[data-term]')).every(function(term){return term.hidden;});});if(feedback)feedback.textContent=query?(visible?visible+'개 용어를 찾았습니다.':'찾는 용어가 없습니다. 영문이나 다른 표현으로 검색해 보세요.'):'';});});
  var json=document.getElementById('lesson-data');
  if (!json) return;
  var lesson=JSON.parse(json.textContent);
  var index=0, timer=null;
  var sim=document.querySelector('.simulation');
  function renderStep(){
    sim.querySelector('[data-step-count]').textContent=(index+1)+' / '+lesson.steps.length;
    sim.querySelector('[data-step-message]').textContent=lesson.steps[index];
    sim.querySelectorAll('.sim-node').forEach(function(node,i){node.classList.toggle('current',i===index);node.classList.toggle('past',i<index);});
    sim.querySelector('[data-step=prev]').disabled=index===0;
    sim.querySelector('[data-step=next]').disabled=index===lesson.steps.length-1;
  }
  function stop(){if(timer){clearInterval(timer);timer=null;}sim.querySelector('[data-step=play]').textContent='자동 재생';}
  sim.querySelectorAll('[data-step]').forEach(function(button){button.addEventListener('click',function(){var action=button.dataset.step;if(action==='prev'){stop();index=Math.max(0,index-1);}if(action==='next'){stop();index=Math.min(lesson.steps.length-1,index+1);}if(action==='reset'){stop();index=0;}if(action==='play'){if(timer){stop();}else{if(index===lesson.steps.length-1)index=0;button.textContent='일시 정지';timer=setInterval(function(){if(index<lesson.steps.length-1){index++;renderStep();}else stop();},2400);}}renderStep();});});
  document.addEventListener('visibilitychange',function(){if(document.hidden)stop();});
  renderStep();
  var quiz=document.querySelector('.quiz');
  quiz.querySelectorAll('[data-answer]').forEach(function(button){button.addEventListener('click',function(){quiz.querySelectorAll('[data-answer]').forEach(function(b){b.classList.remove('correct','incorrect');b.setAttribute('aria-pressed','false');});var correct=Number(button.dataset.answer)===lesson.correct;button.classList.add(correct?'correct':'incorrect');button.setAttribute('aria-pressed','true');var feedback=quiz.querySelector('.quiz-feedback');feedback.classList.toggle('wrong',!correct);feedback.textContent=(correct?'정답입니다. ':'다시 생각해 보세요. ')+lesson.explanation;});});
  var tool=document.querySelector('[data-tool]');
  if(!tool)return;
  var output=tool.querySelector('.tool-output');
  var kind=tool.dataset.tool;
  if(kind==='groups'){
    var input=tool.querySelector('input');
    var groupRender=function(){var count=Math.max(1,Math.min(5,Number(input.value)||1));var lines=[];for(var i=0;i<count;i++){var partitions=[];for(var p=0;p<3;p++)if(p%count===i)partitions.push('P'+p);lines.push('Consumer '+(i+1)+' → '+(partitions.join(', ')||'배정 없음'));}output.textContent=lines.join('\n');};
    input.addEventListener('input',groupRender);groupRender();
  }
  if(kind==='offset'){
    var cursor=0, committed=0, processed=new Set(), last=null, message='offset 0부터 시작합니다.';
    var offsetRender=function(){output.textContent='다음 처리 위치: '+cursor+' | committed: '+committed+'\n'+message;};
    tool.querySelectorAll('[data-action]').forEach(function(b){b.addEventListener('click',function(){if(b.dataset.action==='process'){var duplicate=processed.has(cursor);processed.add(cursor);last=cursor;cursor++;message='offset '+last+' 업무 처리 '+(duplicate?'재실행 (중복)':'완료')+' · 아직 미커밋';}if(b.dataset.action==='commit'){if(last===null){message='먼저 처리할 레코드가 필요합니다.';}else{committed=cursor;message='다음 위치 '+committed+' 저장 완료';last=null;}}if(b.dataset.action==='restart'){cursor=committed;last=null;message='committed 위치로 복구했습니다. 미커밋 처리를 다시 실행해 보세요.';}if(b.dataset.action==='reset'){cursor=0;committed=0;processed.clear();last=null;message='초기화했습니다.';}offsetRender();});});offsetRender();
  }
  if(kind==='transaction'){
    var logs=[],state='시작 전';
    var txRender=function(){output.textContent='기록 상태: '+(logs.map(function(x,i){return 'E'+(i+1)+'['+x+']';}).join(' ')||'없음')+'\nread_committed에 보이는 이벤트: '+logs.filter(function(x){return x==='committed';}).length+'건\n'+state;};
    tool.querySelectorAll('[data-action]').forEach(function(b){b.addEventListener('click',function(){var a=b.dataset.action;if(a==='begin'){if(logs.includes('pending'))state='이미 진행 중인 트랜잭션이 있습니다.';else{logs.push('pending');state='이벤트를 썼지만 아직 보이지 않습니다.';}}if(a==='commit'||a==='abort'){var i=logs.indexOf('pending');if(i<0)state='먼저 트랜잭션을 시작하세요.';else{logs[i]=a==='commit'?'committed':'aborted';state=a==='commit'?'commit 후 읽을 수 있습니다.':'abort된 레코드는 read_committed에 보이지 않습니다.';}}if(a==='reset'){logs=[];state='초기화했습니다.';}txRender();});});txRender();
  }
  if(kind==='outbox'){
    var stored=false,published=false,deliveries=0,acked=false;
    var obRender=function(){output.textContent='DB 주문/Outbox: '+(stored?'저장됨':'없음')+' | published_at: '+(published?'있음':'NULL')+'\nKafka 기록 수: '+deliveries+' (같은 eventId)'+(acked?' · ack 수신, 아직 표식 전':'');};
    tool.querySelectorAll('[data-action]').forEach(function(b){b.addEventListener('click',function(){var a=b.dataset.action;if(a==='save')stored=true;if(a==='send'&&stored&&!published&&!acked){deliveries++;acked=true;}if(a==='mark'&&acked){published=true;acked=false;}if(a==='crash')acked=false;if(a==='reset'){stored=false;published=false;acked=false;deliveries=0;}obRender();});});obRender();
  }
  if(kind==='replication'){
    var live=tool.querySelector('input');var repRender=function(){var n=Math.max(1,Math.min(3,Number(live.value)||1));output.textContent='RF=3 · min ISR=2 · acks=all\n정상 노드 '+n+'개 → '+(n>=2?'동기화와 과반 조건을 만족하면 쓰기 가능':'최소 ISR 미달. 이 combined 구성은 controller 과반도 상실. 쓰기 진행 불가');};live.addEventListener('input',repRender);repRender();
  }
})();
