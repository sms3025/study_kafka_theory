# 검증 안내

## 자료와 UI 검사

```powershell
python .\검증\check_materials.py
node --check .\이론\assets\app.js
```

- 루트 진입 페이지와 이론 HTML 총 17개 파일(단원 12, 부록 3, 로드맵, 루트)의 로컬 링크·이미지·페이지 앵커 검사
- 12개 이론/실습 짝과 실제 Java 발췌 구간 검사
- SVG XML과 단원별 상호작용 JSON 검사
- 단원마다 왜 배우는지·비유·개념 4개 이상·추가 개념·핵심 정리·FAQ·로그 수집 적용 블록과, 모든 실습 명령의 결과 해석이 있는지 검사

브라우저 검사는 `browser_check.cjs`에 있습니다. Playwright 패키지와 Chrome이 필요하며 학습 자료 실행에는 필요하지 않습니다.
`COURSE_URL`은 기본 `http://127.0.0.1:8765/`, `CHROME_PATH`는 Chrome 실행 파일 경로입니다.
이 세션에서는 제공된 Playwright 런타임과 설치된 Chrome으로 검사했습니다.

- 12개 단원의 그림 로드, 다음/초기화, 정답/오답 해설, 학습 블록과 FAQ 펼치기
- 검색 결과/빈 결과, 학습 완료 저장과 다시 열기, 이어서 학습
- 5개 시뮬레이터: 그룹 수, offset 복구, 복제 조건, 트랜잭션, Outbox
- 부록 3개 페이지와 용어집 검색
- 390px 모바일 화면 가로 넘침, 메뉴 열기/닫기
- `file://` 직접 열기와 외부 fetch 없는 동작
- 브라우저 JS 오류 및 HTTP 리소스 오류 없음

## 실제 Kafka·PostgreSQL 통합 검사

```powershell
.\실습\study.ps1 up
.\실습\study.ps1 build
.\실습\study.ps1 init
.\실습\study.ps1 run verify

.\실습\study.ps1 up -Cluster
.\실습\study.ps1 init -Cluster
.\실습\study.ps1 run -Cluster verify
```

`verify`는 고유 이름의 전용 토픽을 사용하며 자신의 검사 토픽·그룹·DB 행을 정리합니다.

| 검사 | 확인하는 보장 |
|---|---|
| 6건 송수신 | 실제 브로커를 통과한 이벤트 전달 |
| key / partition / offset | 같은 key의 동일 파티션, 파티션 내 증가 offset |
| 커밋 후 Consumer 재생성 | 커밋한 레코드를 다시 읽지 않음 |
| commit·abort 격리 | read_committed에서는 abort된 데이터 제외 |
| read_uncommitted | abort된 레코드도 관찰 가능 |
| DB 중복 처리 | 같은 eventId 재처리 시 업무 갱신 제외 |
| DB 합계 원자성 | 표식과 집계가 함께 저장되고 1회만 누적 |
| Outbox rollback | 실패한 주문과 outbox가 모두 저장되지 않음 |
| 스키마 계약 | 선택 필드 추가 허용, 미지원 버전 거부 |

## 단원별 실행 시나리오

```powershell
python .\검증\run_scenarios.py
```

이 검사는 **학습 환경의 orders 등 토픽에 테스트 데이터를 발행**합니다. 단일과 3노드 환경을 모두 `up`/`init`한 뒤 실행하세요.
완전히 새로 마련한 학습 환경에서 검사하는 것이 권장됩니다. 학습 데이터가 누적되어도 고유 runId와 group으로 대부분 분리하지만, retry 입력 앞부분과 미발행 outbox는 이전 실험의 영향을 받을 수 있습니다.

- 단건 Producer와 Consumer의 실제 이벤트 전달
- 처리 후 미커밋 강제 종료와 재시작 재처리
- DB 성공 직후 강제 종료와 중복 건너뛰기
- 일시 오류의 두 번째 시도 성공과 영구 오류 2건의 DLT 발행
- Outbox Kafka ack 후 강제 종료와 같은 eventId 재발행
- Kafka→Kafka 트랜잭션 처리의 커밋된 출력
- Streams 실행 및 count 출력
- 3노드 중 한 Broker 중단 상태에서 쓰기, 중단 노드 복구

강제 종료 exit 17과 DB rollback 예외는 의도된 실패 재현입니다. 정상 종료와 구분해 검사합니다.
검사 중에는 `build`로 실행 중인 JAR를 덮어쓰지 마세요.

## 기록

실행 로그와 브라우저 캡처는 `검증/결과/`에 저장합니다. 생성 산출물은 `.gitignore`로 제외했습니다.
실제 실행 결과 요약은 [검증 결과](검증-결과.md)를 확인하세요.

성능 수치, 장시간 부하, 보안 구성, 실제 호스트/디스크 장애, CDC 또는 Spring 통합은 이 검사의 확인 범위에 포함되지 않습니다.
