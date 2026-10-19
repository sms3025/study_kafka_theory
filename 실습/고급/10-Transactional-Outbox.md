# 10. Transactional Outbox

주문과 outbox를 함께 저장하고, Relay의 중복 발행까지 실험합니다.

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

## 실행과 관찰

### 1. 발행 의도 저장

```powershell
.\실습\study.ps1 run outbox-create 1 day10
```

예상 결과: OUTBOX_CREATED day10-0이 출력됩니다. 같은 runId 재사용은 PK 충돌이므로 새 실험에는 새 runId를 쓰세요.

### 2. ack 직후 장애

```powershell
.\실습\study.ps1 run outbox-relay crash-after-send 1
```

예상 결과: RELAY_ACK 뒤 exit 17입니다. published_at은 비어 있습니다.

### 3. 재발행

```powershell
.\실습\study.ps1 run outbox-relay normal 10
```

예상 결과: 이전에 ack된 eventId가 다시 RELAY_ACK될 수 있습니다.

### 4. 중복 방지 확인

```powershell
.\실습\study.ps1 run dedup outbox-dedup-day10 0 normal
```

예상 결과: 재발행된 같은 eventId 중 하나가 DUPLICATE_SKIPPED입니다.

### 5. 롤백 실험

```powershell
.\실습\study.ps1 run outbox-create 1 day10-rollback rollback
```

예상 결과: INJECTED 오류와 비정상 종료가 예상 결과입니다. 주문과 outbox가 모두 저장되지 않아야 합니다.

### 6. DB 확인

```powershell
.\실습\study.ps1 sql
```

예상 결과: outbox 발행 시각과 집계 결과를 확인합니다.

## 더 실험하기

- outbox-create만 실행하고 Relay를 실행하지 않아도 미발행 정보가 남는지 확인하세요.
- 하나의 Relay에서 복구한 뒤에만 다음 실험을 시작해, 이전 미발행 행과 새 행이 섞이지 않게 하세요.

## 주의할 경계

SKIP LOCKED는 이벤트의 업무상 순서를 보장하는 기능이 아닙니다. DB 발행 표식을 남겨도 Kafka 중복 발행 구간은 남습니다. 배달 결과의 멱등성을 끝까지 유지해야 합니다.

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

이해도 확인: Kafka ack 직후, published_at 저장 전에 Relay가 종료되면?

<details><summary>답 확인</summary>

같은 이벤트가 재발행될 수 있다 — outbox는 아직 미발행으로 남으므로 다음 Relay가 다시 전송할 수 있습니다.

</details>
