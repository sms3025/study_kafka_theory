# 10. Transactional Outbox

주문과 outbox를 함께 저장하고, Relay의 중복 발행과 Consumer 멱등성까지 이어서 실험합니다.

먼저 [대응 이론 HTML](../../이론/고급/10.html)을 읽으세요.

## 준비

- 저장소 루트의 PowerShell에서 실행합니다.
- 01단원의 up → build → init을 완료해야 합니다.
- 기존 기록은 유지됩니다. 새 실험은 고유 group / runId를 사용하세요.
- `max=0`은 무한 상주가 아니라 입력이 끊긴 뒤 25초까지 읽는 모드입니다.

## 학습 목표

- DB/Kafka 이중 쓰기의 실패 구간을 찾는다
- 주문과 발행 의도를 원자적으로 저장한다
- Relay 재발행을 Consumer 멱등성과 연결한다
- Polling Relay와 CDC 기반 Outbox의 차이를 설명한다

## 실습 전에 떠올릴 핵심 개념

- DB와 Kafka에 순서대로 쓰는 코드는 어떤 순서든 중간에 끊기면 유실이나 유령 이벤트를 만듭니다.
- Outbox는 업무 데이터와 보낼 이벤트를 같은 DB 트랜잭션에 저장해 둘의 불일치를 없앱니다.
- Relay는 ack를 받은 뒤에만 발행 완료를 표시하므로 at least once입니다. 중복은 eventId로 Consumer가 제거합니다.
- `SKIP LOCKED`는 Relay 간 충돌을 줄일 뿐 순서를 지키지 않습니다. 순서가 필요하면 key, Relay 직렬화, 순번을 함께 설계합니다.
- 미발행 건수와 가장 오래된 미발행 이벤트의 나이를 관찰하고, 발행 완료 행의 정리 정책을 정합니다.
- CDC 기반 Outbox는 Relay 코드를 없애고 지연을 줄이지만, Connect 운영과 중복 제거는 여전히 필요합니다.

## 실행과 관찰

### 1. 발행 의도 저장

```powershell
.\실습\study.ps1 run outbox-create 1 day10
```

예상 결과: OUTBOX_CREATED day10-0이 출력됩니다. 같은 runId 재사용은 PK 충돌이므로 새 실험에는 새 runId를 쓰세요.

**결과 해석**

```text
OUTBOX_CREATED day10-0
```

이 한 줄이 나왔다면 DB 트랜잭션 하나에서 두 행이 함께 커밋된 것입니다. `orders_db`에는 주문 `day10-order-0`(금액 1000)이, `outbox`에는 eventId `day10-0`, `aggregate_id`가 `day10-order-0`이고 `published_at`이 NULL인 행이 생겼습니다.

**Kafka에는 아직 아무것도 없습니다.** 주문 API라면 여기서 성공 응답을 돌려줘도 됩니다. 보내야 한다는 사실이 DB에 남았기 때문입니다. 같은 runId로 다시 실행하면 `orders_db`의 기본 키(order_id) 충돌로 실패하므로 새 실험에는 새 runId를 쓰세요.

### 2. ack 직후 장애

```powershell
.\실습\study.ps1 run outbox-relay crash-after-send 1
```

예상 결과: RELAY_ACK 뒤 exit 17입니다. published_at은 비어 있습니다.

**결과 해석**

```text
RELAY_ACK day10-0
CRASH: Kafka ack received, outbox not marked. Restart will resend.
Docker command failed (exit 17). See the output above.
```

`RELAY_ACK`는 Kafka가 레코드를 기록하고 ack를 돌려줬다는 뜻입니다. 그 직후 `Runtime.halt(17)`로 프로세스가 강제 종료되었습니다. `published_at` 갱신도, DB 커밋도 실행되지 않았으므로 DB 연결이 끊기면서 트랜잭션이 롤백되고 행 잠금이 풀립니다. 결과적으로 **Kafka에는 이벤트가 있고 DB에는 미발행으로 남은** 상태가 됩니다.

exit 17과 PowerShell 오류는 의도된 결과입니다. Relay는 가장 오래된 미발행 행부터 보내므로, 이전 실험에서 남은 미발행 행이 있다면 `day10-0`이 아닌 다른 eventId가 출력될 수 있습니다.

### 3. 재발행

```powershell
.\실습\study.ps1 run outbox-relay normal 10
```

예상 결과: 이전에 ack된 eventId가 다시 RELAY_ACK될 수 있습니다.

**결과 해석**

```text
RELAY_ACK day10-0
RELAYED 1
```

재시작한 Relay가 같은 행을 다시 찾아 한 번 더 보냈습니다. 이번에는 ack 뒤 `published_at`을 갱신하고 커밋까지 마쳤습니다. `RELAYED`는 이번 실행에서 보낸 건수이며, 더 보낼 행이 없으면 max(10)에 닿기 전에 끝납니다.

이제 `orders` 토픽에는 eventId가 `day10-0`인 레코드가 **두 건** 있습니다. key가 같은 `day10-order-0`이므로 두 레코드는 같은 파티션에 연속된 offset으로 들어갔습니다. Outbox가 유실은 막았지만 중복은 남긴다는 사실을 눈으로 확인한 셈입니다.

### 4. 중복 방지 확인

```powershell
.\실습\study.ps1 run dedup outbox-dedup-day10 0 normal
```

예상 결과: 재발행된 같은 eventId 중 하나가 DUPLICATE_SKIPPED입니다.

**결과 해석**

```text
ASSIGNED [orders-0, orders-1, orders-2]
DUPLICATE_SKIPPED event=day01-0 order=order-0
...
APPLIED event=day10-0 order=day10-order-0
DUPLICATE_SKIPPED event=day10-0 order=day10-order-0
...
REVOKED [orders-0, orders-1, orders-2]
HANDLED 143
```

새 그룹이라 `orders`를 처음부터 읽으므로 이전 단원의 이벤트도 함께 처리합니다. 핵심은 `day10-0`이 연속으로 두 번 나오며 **첫 번째는 APPLIED, 두 번째는 DUPLICATE_SKIPPED**라는 점입니다.

이전 단원에서 이미 처리한 이벤트들도 DUPLICATE_SKIPPED로 나올 수 있습니다. `processed_events`의 중복 판정 기준이 Consumer Group이 아니라 `handler='order-totals'`이기 때문입니다. 그룹 이름을 바꿔도 같은 업무 처리기는 같은 eventId를 두 번 반영하지 않습니다. 줄 수와 HANDLED 숫자는 토픽에 쌓인 데이터에 따라 달라집니다.

### 5. 롤백 실험

```powershell
.\실습\study.ps1 run outbox-create 1 day10-rollback rollback
```

예상 결과: INJECTED 오류와 비정상 종료가 예상 결과입니다. 주문과 outbox가 모두 저장되지 않아야 합니다.

**결과 해석**

```text
Exception in thread "main" java.lang.IllegalStateException: INJECTED failure: roll back order AND outbox
	at study.kafka.OutboxLab.createOne(OutboxLab.java:...)
	...
Docker command failed (exit 1). See the output above.
```

`rollback` 모드는 두 INSERT를 모두 실행한 **뒤에** 일부러 예외를 던집니다. catch 블록이 `db.rollback()`을 호출하므로, 이미 실행된 `orders_db` INSERT와 `outbox` INSERT가 함께 취소됩니다. `OUTBOX_CREATED` 줄이 출력되지 않은 것도 확인하세요.

이 실험이 보여 주는 것은 '주문만 저장되고 이벤트는 없는 상태'가 원리적으로 불가능하다는 점입니다. 주문 API가 실패 응답을 돌려줄 때 DB에도 Kafka에도 흔적이 남지 않습니다.

### 6. DB 확인

```powershell
.\실습\study.ps1 sql
```

예상 결과: outbox 발행 시각과 집계 결과를 확인합니다.

**결과 해석**

```text
   order_id    | event_count | total_amount
---------------+-------------+--------------
 day10-order-0 |           1 |         1000
 order-0       |          12 |        ...
(n rows)

 event_id |         published_at
----------+-------------------------------
 day10-0  | 2026-10-20 02:15:42.123456+00
(n rows)
```

첫 번째 표는 `order_totals`입니다. Kafka에 같은 이벤트가 두 번 있었는데도 `day10-order-0`의 `event_count`는 1, 합계는 1000입니다. 두 번째 표는 `outbox`이며 `day10-0`에 발행 시각이 채워졌습니다. `day10-rollback-0`은 어디에도 없어야 합니다.

`sql` 명령은 `order_totals`와 `outbox`만 조회합니다. `orders_db`까지 확인하려면 `docker compose -f .\실습\compose.yaml -p kafka-field-guide exec -T postgres psql -U study -d kafka_study -c "SELECT * FROM orders_db WHERE order_id LIKE 'day10%'"`를 실행하세요. 숫자는 예시입니다.

## 더 실험하기

- `outbox-create`만 실행하고 Relay를 실행하지 않은 채 `sql`로 미발행 행이 남는지 확인하세요. 이 상태가 Relay 장애 때 운영에서 보게 될 모습입니다.
- 하나의 Relay로 복구를 끝낸 뒤에만 다음 실험을 시작해, 이전 미발행 행과 새 행이 섞이지 않게 하세요.
- '함께 알아둘 개념'에 있는 미발행 건수·나이 SQL을 psql로 실행해, `crash-after-send` 직후와 `normal` 실행 뒤의 값을 비교해 보세요.

## 주의할 경계

SKIP LOCKED는 이벤트의 업무상 순서를 보장하는 기능이 아닙니다. DB에 발행 표식을 남겨도 Kafka 중복 발행 구간은 남습니다. 하류 Consumer가 eventId로 멱등하게 처리해야 패턴이 완성됩니다.

## 로그 수집 파이프라인에 적용하면

**원본 로그에는 Outbox가 아니라 같은 질문이 필요합니다**

장비 로그는 DB 트랜잭션에서 태어나는 데이터가 아니므로, 원본 로그 수집 경로에 Outbox 테이블을 둘 이유는 거의 없습니다. 하지만 이 단원의 핵심 질문, 즉 **'보내야 한다는 사실을 언제 영속적으로 남기는가'**는 수집 경로에도 그대로 적용됩니다.

- **수집 서버의 HTTP 응답 시점:** 수집 서버가 요청을 받자마자 200을 돌려주고 Kafka 전송은 나중에 한다면, 그 사이에 서버가 죽으면 에이전트는 성공한 줄 알고 로그를 버립니다(유실). Kafka ack를 받은 뒤 200을 돌려주면 유실은 막지만, 응답이 타임아웃 난 경우 에이전트가 다시 보내 중복이 생깁니다(at least once). 대부분의 로그 시스템은 후자를 택하고 중복을 정리합니다.
- **에이전트 쪽 버퍼:** 수집 서버나 Kafka가 멈춘 동안의 로그를 지키려면 에이전트가 보내지 못한 로그를 디스크에 쌓아 두고 재시도해야 합니다. Fluent Bit 같은 에이전트의 파일 시스템 버퍼가 이 역할을 하며, 개념적으로는 '에이전트 안의 outbox'입니다.
- **중복 정리용 ID:** 재시도로 생긴 중복을 저장소에서 걸러내려면 로그마다 고유 ID(에이전트가 붙인 ID, 또는 장비 ID·장비 시각·순번 조합)가 필요합니다. 이 실습의 eventId와 같은 역할입니다.

반면 수집 서버가 **자기 DB에 쓰면서 이벤트도 내야 하는 경우**에는 Outbox가 정확히 맞습니다. 예를 들어 새 장비가 처음 접속해 장비 목록 DB에 등록되고, 동시에 '장비 등록됨' 이벤트를 다른 서비스에 알려야 한다면 이 단원의 패턴을 그대로 쓰면 됩니다.

## 자주 묻는 질문

### Q. Kafka 트랜잭션으로 DB와 Kafka를 함께 묶으면 Outbox가 필요 없지 않나요?

Kafka 트랜잭션은 Kafka 안의 쓰기와 Consumer Group offset만 묶습니다. Kafka는 외부 DB와 XA 같은 분산 트랜잭션에 참여하지 않으므로 DB 커밋과 Kafka 커밋을 하나로 만들 수 없습니다. 그래서 DB가 이미 가진 트랜잭션 안에 '보낼 이벤트'를 넣는 Outbox가 쓰입니다.

### Q. `published_at`을 먼저 표시하고 보내면 중복이 사라지지 않나요?

중복은 사라지지만 대신 유실이 생깁니다. 표시하고 커밋한 직후 전송 전에 장애가 나면, 그 이벤트는 '발행 완료'로 기록된 채 Kafka에는 영영 도착하지 않습니다. 유실은 나중에 알아채기도 어렵습니다. 중복은 eventId로 제거할 수 있으므로 ack 후 표시하는 쪽이 일반적인 선택입니다.

### Q. Relay를 여러 개 띄워 처리량을 늘려도 되나요?

`FOR UPDATE SKIP LOCKED` 덕분에 여러 Relay가 같은 행을 동시에 집는 일은 줄어듭니다. 다만 같은 업무 대상의 이벤트가 서로 다른 Relay에 나뉘면 전달 순서가 뒤바뀔 수 있습니다. 순서가 중요하면 `aggregate_id` 해시로 Relay의 담당 범위를 나누거나, 이벤트에 순번을 넣어 Consumer가 판단하게 합니다. 처리량이 더 필요하면 CDC 방식도 검토합니다.

### Q. CDC를 쓰면 중복 발행이 완전히 없어지나요?

그렇게 가정하지 않는 편이 안전합니다. 커넥터가 재시작하면 마지막으로 기록한 위치 이후의 변경을 다시 보낼 수 있습니다. Kafka Connect에는 소스 커넥터용 exactly-once 설정이 있지만, 분산 모드에서 켜야 하고 커넥터가 이를 지원해야 합니다. 어느 경우든 Consumer의 eventId 중복 제거는 유지하세요.

### Q. outbox 테이블은 언제 비우나요?

발행이 끝난 행은 추적에 필요한 기간이 지나면 지웁니다. 장애 조사 때 '이 이벤트가 언제 발행되었는가'를 확인할 수 있도록 며칠 정도 남겨 두는 경우가 많습니다. 삭제는 작은 배치로 나누고, 양이 매우 많다면 날짜별 파티션 테이블로 만들어 오래된 파티션을 통째로 떼어 내는 편이 DB에 부담이 적습니다.

## 읽을 소스

- [발췌한 전체 소스](../고급/src/main/java/study/kafka/OutboxLab.java)

## 기록할 것

| 예상 결과 | 실제 결과 | 차이의 이유 |
|---|---|---|
| 직접 작성 | 직접 작성 | 직접 작성 |

## 완료 기준

- [ ] DB/Kafka 이중 쓰기의 실패 구간을 찾는다
- [ ] 주문과 발행 의도를 원자적으로 저장한다
- [ ] Relay 재발행을 Consumer 멱등성과 연결한다
- [ ] Polling Relay와 CDC 기반 Outbox의 차이를 설명한다

이해도 확인: Kafka ack 직후, published_at 저장 전에 Relay가 종료되면?

<details><summary>답 확인</summary>

같은 이벤트가 재발행될 수 있다: DB 트랜잭션이 롤백되어 outbox는 미발행으로 남으므로, 다음 Relay가 같은 eventId로 다시 전송합니다. Consumer의 멱등 처리가 이 중복을 흡수합니다.

</details>
