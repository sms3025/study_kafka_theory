# 12. Lag · 스키마 · 운영 점검

지연·장애·데이터 호환성을 관찰하는 방법을 익히고, 복구까지 포함한 기준으로 최종 파이프라인을 검증합니다.

먼저 [대응 이론 HTML](../../이론/고급/12.html)을 읽으세요.

## 준비

- 저장소 루트의 PowerShell에서 실행합니다.
- 01단원의 up → build → init을 완료해야 합니다.
- 기존 기록은 유지됩니다. 새 실험은 고유 group / runId를 사용하세요.
- `max=0`은 무한 상주가 아니라 입력이 끊긴 뒤 25초까지 읽는 모드입니다.

## 학습 목표

- Lag을 처리 시간과 구분해 해석한다
- 추가 필드와 호환 불가 변경을 구분한다
- 장애·중복·복구를 포함한 완료 기준을 만든다
- 운영 지표와 보안 구성 요소의 출발점을 정리한다

## 실습 전에 떠올릴 핵심 개념

- Lag은 log end offset과 committed offset의 차이일 뿐 시간이 아닙니다. 커밋 기록이 없다는 것은 Lag 0의 증거가 아닙니다.
- Lag의 원인은 유입 속도, 처리율, 파티션 쏠림, 리밸런싱, 재시도를 함께 봐야 좁혀집니다.
- 알람은 순간값보다 추세와 시간 기준 지연(종단 간 지연, 가장 오래된 미처리 레코드의 나이)으로 겁니다.
- 선택 필드 추가는 대개 호환되지만 필드 삭제·이름·타입·단위 변경은 계약 위반입니다. Registry는 이를 등록 시점에 검사합니다.
- 운영 환경은 TLS·SASL·ACL과 비밀정보 관리가 필요하며, 이 학습용 Compose를 그대로 배포하지 않습니다.
- 완료 기준은 정상 동작이 아니라 장애 후 복구입니다. 검사가 확인한 것과 확인하지 않은 것을 구분합니다.

## 실행과 관찰

### 1. 느린 Consumer: 터미널 A

```powershell
.\실습\study.ps1 run consumer lag-day12 0 orders 1000
```

예상 결과: 레코드당 1초를 기다립니다. 이 Consumer는 출력 대역 뒤에 커밋합니다.

**결과 해석**

```text
ASSIGNED [orders-0, orders-1, orders-2]
RECEIVED group=lag-day12 key=order-2 partition=0 offset=0 value={"schemaVersion":1,...}
RECEIVED group=lag-day12 key=order-2 partition=0 offset=1 value={"schemaVersion":1,...}
...
```

네 번째 인자 1000은 레코드마다 1초씩 쉬는 `delayMs`입니다. 느린 DB나 외부 API를 흉내 낸 것으로, 출력 → 1초 대기 → 커밋 순서로 동작합니다. 새 그룹이라 `orders`의 처음부터 읽으므로 이전 단원에서 쌓인 레코드가 많을수록 처음부터 Lag이 큽니다.

`max=0`이라 입력이 끊긴 뒤 25초가 지나야 끝나는데, 초당 1건씩이면 한참 걸립니다. 관찰을 마치면 `Ctrl+C`로 멈추세요. 마지막으로 처리 중이던 레코드는 커밋 전이라 다음 실행 때 다시 읽을 수 있습니다.

### 2. 입력: 터미널 B

```powershell
.\실습\study.ps1 run producer 60 orders day12
```

예상 결과: Consumer 처리보다 빠르게 입력을 만듭니다.

**결과 해석**

```text
SENT event=day12-0 key=order-0 partition=1 offset=52
SENT event=day12-1 key=order-1 partition=1 offset=53
...
SENT event=day12-59 key=order-2 partition=0 offset=31
```

60건이 몇 초 안에 기록됩니다. 터미널 A의 Consumer는 초당 1건밖에 처리하지 못하므로, 이 순간 입력 속도가 처리 속도를 크게 앞서고 Lag이 늘어납니다. 운영에서 트래픽 급증이 일어났을 때와 같은 모양입니다. offset 숫자는 예시입니다.

### 3. 관측: 터미널 C

```powershell
.\실습\study.ps1 run inspect lag-day12
```

예상 결과: next, end, lag과 Leader/ISR 정보가 출력됩니다. 주기적으로 다시 실행해 비교하세요.

**결과 해석**

```text
CLUSTER MkU3OEVBNTcwNTJENDM2Qk
PARTITION 0 leader=kafka:9092 (id: 1 rack: null isFenced: false) replicas=[kafka:9092 (id: 1 ...)] ISR=[kafka:9092 (id: 1 ...)]
PARTITION 1 leader=kafka:9092 (id: 1 rack: null isFenced: false) replicas=[...] ISR=[...]
PARTITION 2 leader=kafka:9092 (id: 1 rack: null isFenced: false) replicas=[...] ISR=[...]
LAG group=lag-day12 partition=orders-0 next=14 end=92 lag=78
```

`CLUSTER`는 클러스터 ID이고, `PARTITION` 줄은 `orders` 각 파티션의 리더와 복제본, ISR입니다. `LAG` 줄에서 `next`는 그룹의 committed offset(다음에 읽을 위치), `end`는 log end offset, `lag`은 그 차이입니다.

위 예시처럼 `LAG` 줄이 일부 파티션만 나올 수 있습니다. Consumer가 아직 다른 파티션의 레코드를 처리·커밋하지 않았기 때문이며, 그 파티션의 Lag은 합계에서 빠져 있습니다. 시작 직후라면 `NO_COMMITTED_OFFSETS (not proof of zero lag)`가 나옵니다. 몇 초 간격으로 다시 실행해 lag이 늘고 줄어드는 **추세**를 보세요. 부록 A1의 `groups --describe` 출력과도 비교해 보세요.

### 4. 스키마 확인

```powershell
.\실습\study.ps1 run schema
```

예상 결과: OPTIONAL_FIELD_OK와 UNSUPPORTED_VERSION_REJECTED가 출력됩니다.

**결과 해석**

```text
OPTIONAL_FIELD_OK schema-1
UNSUPPORTED_VERSION_REJECTED
```

`schema` 명령은 정상 이벤트 JSON에 `optionalCampaign`이라는 새 필드를 붙여 `OrderEvent.parse`로 읽습니다. 모르는 필드를 무시하도록 설정되어 있으므로 파싱에 성공해 `OPTIONAL_FIELD_OK`가 나옵니다. 선택 필드 추가가 호환되는 변경이라는 확인입니다.

이어서 `schemaVersion`을 2로 바꿔 다시 읽으면 `IllegalArgumentException`이 나고, 이를 기대한 대로 잡아 `UNSUPPORTED_VERSION_REJECTED`를 출력합니다. 아직 모르는 버전의 이벤트를 추측으로 처리하지 않고 거부하는 정책입니다. 운영에서는 거부된 이벤트를 07단원처럼 DLT로 보내 원인을 남깁니다.

### 5. 실제 통합 검사

```powershell
.\실습\study.ps1 run verify
```

예상 결과: 송수신, key 분배, 복구 위치, 트랜잭션 격리, DB 멱등성, Outbox 롤백이 통과합니다.

**결과 해석**

```text
PASS 6 records roundtrip
PASS unique eventIds, stable key partition, increasing offsets
PASS committed consumer resumes without replay
PASS read_committed hides aborted transaction
PASS read_uncommitted exposes aborted record
PASS DB duplicate is skipped
PASS dedup marker and total are atomic
PASS outbox rollback removes order and event together
OPTIONAL_FIELD_OK schema-1
UNSUPPORTED_VERSION_REJECTED
ALL_CHECKS_PASSED verify-1a2b3c4d
```

각 `PASS` 줄은 이 과정의 단원과 이어집니다. 송수신과 key·offset 규칙은 01~04단원, 커밋 후 재생성 시 재처리 없음은 05단원, 트랜잭션 격리는 09단원, DB 중복 제거와 원자성은 06단원, Outbox 롤백은 10단원, 마지막 두 줄은 이 단원의 스키마 규칙입니다.

`verify-`로 시작하는 고유 ID로 전용 토픽과 그룹을 만들어 검사하고, 끝나면 자신이 만든 것만 정리합니다. 3노드 환경에서는 `.\실습\study.ps1 run -Cluster verify`로 같은 보장을 확인합니다. 마지막 ID는 실행마다 달라집니다.

## 더 실험하기

- `verify`의 PASS가 어떤 보장 범위를 확인하고, 어떤 부하·보안·성능은 확인하지 않는지 적어 보세요.
- 최종 프로젝트 안내에 따라 주문 생성 대역 → Outbox → Kafka → 집계 Consumer를 실행하고 장애 전후 결과를 기록하세요.
- 터미널 A를 `Ctrl+C`로 멈춘 뒤 `inspect`를 몇 번 실행해, Consumer가 멈췄을 때 offset 기준 Lag과 가장 오래된 미처리 이벤트의 나이가 각각 어떻게 변하는지 비교하세요.

## 주의할 경계

운영 환경은 이 학습용 Compose를 그대로 배포하는 대상이 아닙니다. PLAINTEXT 통신, 로컬 전용 비밀번호, 단일 노드 구성은 모두 학습용입니다. 이 과정의 통합 검사는 기능적 보장을 확인하며 운영 부하·장시간 안정성·보안 검증을 대신하지 않습니다.

## 로그 수집 파이프라인에 적용하면

**로그 수집 파이프라인의 운영 기준 세우기**

로그 수집 시스템을 운영한다면, 이 단원의 개념은 다음과 같은 **수집 목표(SLO)와 점검 항목**으로 바뀝니다.

- **저장 그룹의 Lag과 시간 지연:** 로그 저장소에 적재하는 Consumer Group의 Lag을 보되, 알람은 '장비 시각 → 저장소에 저장된 시각'의 종단 간 지연으로 겁니다. 예를 들어 '99%의 로그가 1분 안에 검색 가능해야 한다'처럼 목표를 정하면 어떤 지표를 봐야 할지가 분명해집니다. 장비 시계가 틀어진 경우를 고려해 수집 서버의 수신 시각도 함께 기록해 두면 구간별로 지연을 나눠 볼 수 있습니다.
- **DLT 비율:** 파싱에 실패해 DLT로 가는 로그의 비율을 봅니다. 특정 장비 모델이나 펌웨어 배포 뒤에 비율이 튀면 로그 형식이 바뀐 것입니다.
- **디스크 계획:** 장비 수 × 장비당 로그량으로 하루 유입량을 추정하고, '하루 유입량 × 보관 일수 × 복제 수 × 여유율'로 브로커 디스크를 정합니다. 보관 기간은 로그 저장소가 최대 며칠 멈춰도 재적재할 수 있어야 하는지로 정합니다.
- **공통 로그 스키마의 진화:** 여러 장비의 로그를 하나의 공통 스키마로 다룬다면, 새 정보는 선택 필드로만 추가하고 기존 필드의 이름·타입·단위는 바꾸지 않습니다. 스키마 버전 필드를 두고, 저장 Consumer가 모르는 버전은 DLT로 보내 데이터를 잃지 않게 합니다.
- **복구 리허설:** 저장소를 일부러 멈췄다가 다시 켜서 밀린 로그가 따라잡는 데 걸리는 시간, 중복 적재 여부를 확인합니다. 이 결과가 보관 기간과 Consumer 수를 정하는 근거가 됩니다.

## 자주 묻는 질문

### Q. Lag은 0인데 사용자는 데이터가 늦게 보인다고 합니다.

Lag은 그 Consumer Group의 위치만 보여 줍니다. 지연은 Kafka 앞단(수집기 버퍼, Relay, Outbox 대기)이나 뒷단(저장소의 반영 지연, 화면의 캐시)에서 생길 수 있습니다. 이벤트의 발생 시각부터 화면에 보이는 시각까지 구간별 시각을 기록해 어느 구간이 느린지 나눠 보세요. Lag 0은 'Kafka에서 이 그룹이 밀려 있지 않다'는 뜻일 뿐입니다.

### Q. `inspect`에서 `NO_COMMITTED_OFFSETS`가 나왔습니다. 문제가 없다는 뜻인가요?

아닙니다. 그 그룹이 아직 한 번도 offset을 커밋하지 않았다는 뜻입니다. Consumer가 시작하지 못했거나, 처리에 실패해 커밋까지 가지 못했거나, 그룹 이름이 틀렸을 수 있습니다. 출력 문구 그대로 Lag 0의 증거가 아니므로, Consumer의 로그와 `groups --describe`로 그룹 상태를 함께 확인하세요.

### Q. 이벤트의 필드 이름을 바꾸고 싶습니다. 어떻게 해야 하나요?

이름 변경은 호환되지 않는 변경이므로 단계를 나눕니다. 먼저 새 이름의 필드를 선택 필드로 추가해 두 필드를 함께 발행하고, 모든 Consumer가 새 필드를 읽도록 바꾼 뒤, 충분한 기간이 지나면 옛 필드를 뺍니다. 과거 이벤트가 보관 기간 동안 남는다는 점도 고려합니다. 변경이 너무 크다면 schemaVersion을 올리거나 새 토픽을 만드는 편이 안전합니다.

### Q. 이 학습용 Compose를 조금 고쳐서 운영에 써도 되나요?

권하지 않습니다. 이 구성은 PLAINTEXT 통신, 로컬 전용 비밀번호, broker와 controller를 겸하는 학습용 노드, 학습 편의를 위한 짧은 타임아웃 같은 선택을 담고 있습니다. 운영 구성은 보안, 장애 도메인 분산, 디스크와 네트워크 용량, 모니터링, 백업과 업그레이드 절차를 처음부터 설계해야 합니다.

### Q. Kafka 4.3에서 classic 그룹 프로토콜을 쓰면 안내 로그가 나온다고 들었습니다.

Kafka 4.3 릴리스 공지에 따르면, Consumer가 classic 리밸런스 프로토콜을 쓸 때 새 프로토콜(`group.protocol=consumer`, KIP-848)을 권장하는 로그를 남기는 변경(KIP-1274 1단계)이 들어갔습니다. classic은 향후 버전에서 사용 중단(deprecated)될 예정이라는 안내입니다. 이 실습은 장애 실험의 재현성을 위해 classic과 짧은 세션 타임아웃을 명시했지만, 새 프로젝트라면 03단원에서 본 새 프로토콜을 검토하세요.

## 읽을 소스

- [발췌한 전체 소스](../고급/src/main/java/study/kafka/OperationsLab.java)

## 기록할 것

| 예상 결과 | 실제 결과 | 차이의 이유 |
|---|---|---|
| 직접 작성 | 직접 작성 | 직접 작성 |

## 완료 기준

- [ ] Lag을 처리 시간과 구분해 해석한다
- [ ] 추가 필드와 호환 불가 변경을 구분한다
- [ ] 장애·중복·복구를 포함한 완료 기준을 만든다
- [ ] 운영 지표와 보안 구성 요소의 출발점을 정리한다

이해도 확인: Consumer Lag=0이면 항상 업무 처리 지연도 0초인가요?

<details><summary>답 확인</summary>

아니다. 커밋 위치와 업무 시간은 별도 지표다: 커밋 위치와 실제 업무 완료·시간 지연은 같은 지표가 아닙니다. Kafka 앞뒤 단계의 지연도 있으므로 시간 기준 지표를 함께 봐야 합니다.

</details>
