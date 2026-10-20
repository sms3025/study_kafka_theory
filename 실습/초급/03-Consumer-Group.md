# 03. Consumer Group · Rebalance

같은 그룹의 분담과 다른 그룹의 독립 구독을 구분하고, 구성원이 바뀔 때 파티션이 다시 배정되는 리밸런싱을 관찰합니다.

먼저 [대응 이론 HTML](../../이론/초급/03.html)을 읽으세요.

## 준비

- 저장소 루트의 PowerShell에서 실행합니다.
- 01단원의 up → build → init을 완료해야 합니다.
- 기존 기록은 유지됩니다. 새 실험은 고유 group / runId를 사용하세요.
- `max=0`은 무한 상주가 아니라 입력이 끊긴 뒤 25초까지 읽는 모드입니다.

## 학습 목표

- 동일 그룹과 다른 그룹의 차이를 관찰한다
- 파티션 수와 Consumer 병렬성의 관계를 설명한다
- 리밸런싱 시 할당 변화를 읽는다
- 처음 읽는 그룹의 시작 위치(auto.offset.reset)와 그룹 시간 설정을 구분한다

## 실습 전에 떠올릴 핵심 개념

- 같은 group.id의 Consumer들은 파티션을 나눠 맡고, 다른 그룹은 같은 토픽을 각자 전부 읽습니다.
- 그룹 안에서 파티션 하나는 Consumer 하나만 맡으므로, 병렬성의 상한은 파티션 수입니다.
- 그룹 구성원이 바뀌면 리밸런싱이 일어나고, 이때 처리가 멈추거나 커밋하지 못한 레코드가 다시 처리될 수 있습니다.
- `poll()` 사이가 `max.poll.interval.ms`를 넘으면 살아 있는 Consumer도 그룹에서 빠지므로, 배치 크기와 처리 시간을 함께 관리합니다.
- `auto.offset.reset`은 커밋 기록이 없을 때만 적용되며, Kafka 기본값은 latest, 이 실습은 earliest입니다.
- Kafka 4.x의 새 리밸런스 프로토콜(`group.protocol=consumer`)은 브로커가 배정을 계산하고 리밸런싱이 더 빠릅니다.

## 실행과 관찰

### 1. 터미널 A

```powershell
.\실습\study.ps1 run consumer notify-demo 0 orders 500
```

예상 결과: Consumer가 실행됩니다. 기록이 없으면 25초 뒤 종료하므로 이어지는 명령을 바로 실행하세요.

**결과 해석**

```text
ASSIGNED [orders-0, orders-1, orders-2]
RECEIVED group=notify-demo key=order-2 partition=0 offset=0 value={...}
RECEIVED group=notify-demo key=order-2 partition=0 offset=1 value={...}
```

`notify-demo`는 처음 쓰는 그룹이라 earliest 규칙에 따라 01·02단원에서 쌓인 기록부터 읽습니다. 마지막 인자 500 때문에 레코드마다 0.5초씩 기다리므로 출력이 천천히 올라옵니다. 혼자 가입했으니 세 파티션을 모두 배정받습니다.

이 Consumer는 밀린 기록을 처리하느라 한동안 바쁘므로, 그사이에 터미널 B를 띄우면 리밸런싱을 관찰하기 좋습니다.

### 2. 터미널 B

```powershell
.\실습\study.ps1 run consumer notify-demo 0 orders 500
```

예상 결과: 동일 그룹으로 가입하고 ASSIGNED 로그가 바뀝니다.

**결과 해석**

B가 가입하면 두 터미널에 다음과 같은 변화가 나타납니다(어느 쪽이 어떤 묶음을 받는지는 실행마다 다를 수 있습니다).

```text
# 터미널 A
REVOKED [orders-0, orders-1, orders-2]
ASSIGNED [orders-0, orders-1]

# 터미널 B
ASSIGNED [orders-2]
```

A가 세 파티션을 모두 `REVOKED`한 뒤 일부만 다시 받는 모습이 eager 방식의 Range 배정입니다. 리밸런싱 이후 A는 P0·P1의 레코드만, B는 P2의 레코드만 출력합니다. 다만 이 과정의 key 3개는 P2에 배정되지 않으므로(02단원), P2만 맡은 쪽은 레코드를 하나도 받지 못할 수 있습니다. 고장이 아니라 key 분포 때문입니다. 같은 partition 번호가 두 터미널에 동시에 나타나지 않는지 확인하세요. B는 P2의 마지막 커밋 위치부터 읽으므로, A가 P2에서 이미 처리하고 커밋한 레코드는 다시 읽지 않습니다.

### 3. 터미널 C

```powershell
.\실습\study.ps1 run consumer analytics-demo 0 orders
```

예상 결과: 다른 그룹은 기록 전체를 독립적으로 읽습니다.

**결과 해석**

```text
ASSIGNED [orders-0, orders-1, orders-2]
RECEIVED group=analytics-demo key=order-2 partition=0 offset=0 value={...}
...
```

`analytics-demo`는 notify 그룹과 다른 그룹이므로 리밸런싱에 끼지 않고, 혼자서 세 파티션을 모두 배정받습니다. 지연 인자가 없어 기존 기록을 빠르게 다 읽은 뒤 새 레코드를 기다립니다. A나 B의 출력에는 아무 변화도 일어나지 않는다는 점이 '독립 구독'의 증거입니다.

### 4. 터미널 D

```powershell
.\실습\study.ps1 run producer 60 orders day03
```

예상 결과: notify 그룹 두 Consumer의 처리 합계와 analytics 그룹의 처리량을 비교합니다.

**결과 해석**

60건이 들어오면 세 터미널이 동시에 반응합니다. 각 Consumer는 입력이 25초 동안 끊기면 `REVOKED`와 함께 `CONSUMED <개수>`를 출력하고 종료합니다.

```text
# 예시: A가 P0·P1을 맡은 경우
# 터미널 A: CONSUMED 75 / 터미널 B: CONSUMED 0 / 터미널 C: CONSUMED 75
```

비교할 것은 **A와 B의 합계 ≈ C의 개수**입니다. 01~03단원 명령만 실행했다면 `orders`에는 75건(3 + 12 + 60)이 있고, notify 그룹은 이 레코드들을 둘이 나눠 처리했으며 analytics 그룹은 혼자 전부 처리했기 때문입니다. 리밸런싱 도중 커밋하지 못한 레코드가 있었다면 notify 쪽 합계가 조금 더 클 수 있습니다.

B가 0건이어도 고장이 아닙니다. 이 과정의 key 3개는 partition 0과 1에만 배정되므로(02단원), P2만 맡은 Consumer에게는 줄 레코드가 없습니다. 반대로 B가 P0·P1을 받았다면 B가 대부분을 처리하고, A는 B가 가입하기 전에 혼자 읽은 만큼만 셉니다. Consumer 수를 늘려도 key 분포가 고르지 않으면 일은 고르게 나뉘지 않습니다. 02단원의 핫 파티션 문제가 여기서 그대로 드러납니다. A와 B 중 먼저 끝난 쪽이 그룹을 떠나면, 남은 쪽에서 다시 `REVOKED`/`ASSIGNED`가 출력되는 것도 확인해 보세요.

## 더 실험하기

- A와 B의 `ASSIGNED` 파티션이 같은 시점에 겹치는지 확인하세요. 겹치지 않아야 정상입니다.
- 동일 그룹 Consumer를 4개 실행해 할당이 비어 있는(`ASSIGNED []`) Consumer를 찾아보세요.
- 실행 중 하나를 Ctrl+C로 종료하고 남은 Consumer의 `REVOKED`/`ASSIGNED` 로그가 나오기까지 걸린 시간을 재 보세요. 입력이 끊겨 스스로 종료할 때(정상 `close()`)와 비교하면 세션 타임아웃의 역할이 보입니다.
- `.\실습\study.ps1 groups --describe --group notify-demo`를 실행 중에 호출해 CONSUMER-ID 열로 파티션 담당자를 확인해 보세요.

## 주의할 경계

이 강의는 일반 Consumer Group을 다룹니다. 레코드 단위로 작업을 나눠 주는 Share Group(Kafka Queues)은 별도 모델이며, 그룹 멤버 수가 파티션 수에 묶이지 않습니다. 그룹의 멤버 수와 파티션 수 관계를 모든 Kafka 소비 방식에 일반화하지 마세요.

## 로그 수집 파이프라인에 적용하면

**저장용 · 알람용 Consumer Group을 나누고, 파티션 수 안에서 늘립니다**

장비 로그 파이프라인에서는 같은 로그 토픽을 목적이 다른 여러 그룹이 읽는 구성이 흔합니다.

- **저장 그룹(예: `log-writer`):** 로그를 로그 저장소에 적재합니다. 처리량이 중요하므로 인스턴스를 여러 개 띄워 파티션을 나눠 맡깁니다.
- **알람 그룹(예: `log-alert`):** 오류 패턴을 찾아 알림을 보냅니다. 저장 그룹과 다른 group.id를 쓰므로, 저장소 적재가 밀려도 알람은 따로 진행합니다.

유입량이 늘어 저장 그룹이 뒤처지면 인스턴스를 늘리면 되지만, **파티션 수를 넘어서는 인스턴스는 일하지 않습니다.** 로그 저장소가 Kafka를 직접 구독하는 기능(예: ClickHouse의 Kafka 테이블 엔진)을 써도 마찬가지로, ClickHouse 문서는 한 Consumer Group의 Consumer 수가 토픽의 파티션 수를 넘을 수 없다고 안내합니다.

배치 적재 시간도 그룹 안정성에 영향을 줍니다. 저장소에 수천 건씩 묶어 넣는 Consumer가 저장소 지연으로 한 배치에 5분 넘게 걸리면 `max.poll.interval.ms`를 넘겨 그룹에서 빠지고, 같은 배치를 다른 인스턴스가 다시 적재하는 중복이 생깁니다. 배치 크기를 저장소 응답 시간에 맞춰 제한하고, 저장소 호출에는 타임아웃을 둡니다.

인스턴스를 자주 배포한다면 리밸런싱으로 수집이 끊기는 시간을 줄이는 것도 중요합니다. 새 리밸런스 프로토콜(`group.protocol=consumer`)이나 정적 멤버십을 검토할 만한 이유입니다.

## 자주 묻는 질문

### Q. Consumer를 파티션 수보다 많이 띄우면 어떻게 되나요?

남는 Consumer는 파티션을 배정받지 못하고(`ASSIGNED []`) 대기합니다. 처리량은 늘지 않지만, 다른 Consumer가 장애로 빠지면 리밸런싱으로 즉시 그 파티션을 넘겨받으므로 대기 인력 역할을 합니다. 처리량을 더 늘리려면 파티션 수부터 늘려야 합니다(02단원의 주의점 참고).

### Q. 리밸런싱 중에 메시지가 유실되나요?

레코드는 Kafka에 그대로 남아 있으므로 유실되지 않습니다. 다만 리밸런싱 동안 처리가 잠시 멈추고, 이전 담당자가 처리했지만 커밋하지 못한 레코드는 새 담당자가 다시 처리합니다. 즉 위험은 유실보다 **지연과 중복**입니다. 반대로 처리하기 전에 커밋하는 코드라면 리밸런싱 때 레코드를 건너뛸 수 있습니다(05단원).

### Q. `earliest`로 바꿨는데 처음부터 읽히지 않아요.

그 그룹에 이미 커밋된 offset이 있기 때문입니다. `auto.offset.reset`은 커밋 기록이 없을 때만 쓰입니다. 실습에서는 새 group 이름을 쓰는 것이 가장 간단하고, 운영 중인 그룹의 위치를 옮겨야 한다면 Consumer를 모두 멈춘 뒤 `kafka-consumer-groups.sh --reset-offsets`로 재설정합니다(부록 A1).

### Q. Share Group은 Consumer Group과 무엇이 다른가요?

Share Group(Kafka Queues, KIP-932)은 Kafka 4.2에서 운영 환경에서 쓸 수 있는 상태가 된 별도 소비 모델입니다. 파티션을 한 Consumer에게 통째로 맡기는 대신 레코드 단위로 여러 Consumer에게 나눠 주므로, 파티션 수보다 많은 Consumer가 함께 일할 수 있습니다. 그 대신 파티션 단위의 순서 처리를 기대하기 어렵습니다. 순서보다 작업 분배가 중요한 경우를 위한 모델이며, 이 과정은 다루지 않습니다.

## 읽을 소스

- [발췌한 전체 소스](../초급/src/main/java/study/kafka/OrderConsumer.java)

## 기록할 것

| 예상 결과 | 실제 결과 | 차이의 이유 |
|---|---|---|
| 직접 작성 | 직접 작성 | 직접 작성 |

## 완료 기준

- [ ] 동일 그룹과 다른 그룹의 차이를 관찰한다
- [ ] 파티션 수와 Consumer 병렬성의 관계를 설명한다
- [ ] 리밸런싱 시 할당 변화를 읽는다
- [ ] 처음 읽는 그룹의 시작 위치(auto.offset.reset)와 그룹 시간 설정을 구분한다

이해도 확인: 알림과 통계가 각각 모든 이벤트를 받아야 할 때 group.id는?

<details><summary>답 확인</summary>

서로 달라야 한다: 서로 다른 그룹은 같은 토픽을 독립적으로 구독하고 각자 offset을 관리합니다. 같은 group.id를 쓰면 둘이 이벤트를 나눠 가져 각자 일부만 받게 됩니다.

</details>
