# 02. Partition · Key · Offset

같은 주문의 이벤트를 key로 묶고, 파티션 안에서 순서와 위치(offset)를 관찰합니다. 파티션 수와 보관 정책이 무엇을 바꾸는지도 함께 정리합니다.

먼저 [대응 이론 HTML](../../이론/초급/02.html)을 읽으세요.

## 준비

- 저장소 루트의 PowerShell에서 실행합니다.
- 01단원의 up → build → init을 완료해야 합니다.
- 기존 기록은 유지됩니다. 새 실험은 고유 group / runId를 사용하세요.
- `max=0`은 무한 상주가 아니라 입력이 끊긴 뒤 25초까지 읽는 모드입니다.

## 학습 목표

- 파티션별 순서 보장을 설명한다
- key와 partition의 관계를 확인한다
- offset을 토픽 전체의 번호로 오해하지 않는다
- 파티션 수를 정할 때 따져 볼 기준과 보관 정책(delete · compact)을 구분한다

## 실습 전에 떠올릴 핵심 개념

- 토픽은 여러 파티션으로 나뉘고, Kafka가 보장하는 순서는 각 파티션 내부의 순서뿐입니다.
- key가 있으면 key 해시로 파티션이 정해지므로 같은 key는 같은 파티션으로 갑니다. 서로 다른 key가 같은 파티션에 모이는 것도 정상입니다.
- offset은 파티션마다 따로 매기는 위치 번호이며, 레코드를 가리키려면 토픽 · 파티션 · offset이 모두 필요합니다.
- 파티션 수는 Consumer 병렬성의 상한이고, 줄일 수 없으며, 늘리면 key 배치가 바뀝니다.
- 오래된 데이터는 세그먼트 단위로 정리되며, delete 정책은 기간·크기로, compact 정책은 key별 최신 값으로 남깁니다.

## 실행과 관찰

### 1. 주문 이벤트 전송

```powershell
.\실습\study.ps1 run producer 12 orders day02
```

예상 결과: 3가지 key에 대해 SENT 로그 12줄이 출력됩니다.

**결과 해석**

```text
SENT event=day02-0 key=order-0 partition=1 offset=2
SENT event=day02-1 key=order-1 partition=1 offset=3
SENT event=day02-2 key=order-2 partition=0 offset=1
SENT event=day02-3 key=order-0 partition=1 offset=4
...
SENT event=day02-11 key=order-2 partition=0 offset=4
```

위 숫자는 01단원의 3건이 먼저 쌓여 있다고 가정한 예시이며, offset은 토픽에 이미 쌓인 기록에 따라 다릅니다. 확인할 것은 두 가지입니다.

- **같은 key는 같은 partition:** `order-0`이 나온 줄은 모두 같은 partition 번호를 가져야 합니다.
- **같은 partition 안에서 offset 증가:** 같은 partition의 줄만 모아 보면 offset이 1씩 커집니다. partition 0과 partition 1의 offset은 서로 무관합니다.

이 저장소의 검증 기록에서는 `order-0`과 `order-1`이 partition 1로, `order-2`가 partition 0으로 갔습니다. 기본 파티셔너는 같은 key와 같은 파티션 수에 대해 항상 같은 파티션을 고르므로, 파티션이 3개인 `orders`라면 여러분의 결과도 같게 나옵니다. partition 2에는 이 세 key가 배정되지 않을 수 있으며, 이는 해시 충돌로 생기는 정상 결과입니다.

### 2. 파티션 정보

```powershell
.\실습\study.ps1 topics
```

예상 결과: orders에 P0~P2가 있고 각 파티션의 Leader가 표시됩니다.

**결과 해석**

`topics`는 브로커 컨테이너 안에서 `kafka-topics.sh --describe`를 실행합니다. 토픽마다 요약 한 줄과 파티션별 줄이 이어집니다(탭 간격은 정리했습니다).

```text
Topic: orders  TopicId: 4i58hW...  PartitionCount: 3  ReplicationFactor: 1  Configs: min.insync.replicas=1,retention.ms=604800000
    Topic: orders  Partition: 0  Leader: 1  Replicas: 1  Isr: 1  Elr:   LastKnownElr:
    Topic: orders  Partition: 1  Leader: 1  Replicas: 1  Isr: 1  Elr:   LastKnownElr:
    Topic: orders  Partition: 2  Leader: 1  Replicas: 1  Isr: 1  Elr:   LastKnownElr:
```

- `PartitionCount: 3`은 `init`에서 정한 파티션 수, `Configs`는 토픽에 직접 지정한 설정입니다.
- `Leader`, `Replicas`, `Isr`의 숫자는 브로커 ID입니다. 단일 환경에는 브로커 1번만 있습니다.
- 이 명령은 토픽 이름을 지정하지 않으므로 다른 실습 토픽도 함께 나옵니다. Consumer Group이 커밋을 남긴 뒤라면 `__consumer_offsets` 같은 내부 토픽이 보일 수도 있습니다(03단원).

이 출력에는 레코드 개수가 나오지 않습니다. 파티션별로 어디까지 쌓였는지는 다음 실습 뒤에 `groups --describe`의 LOG-END-OFFSET으로 확인합니다.

### 3. 읽기

```powershell
.\실습\study.ps1 run consumer day02-reader 0 orders
```

예상 결과: 그룹이 처음이라면 기존 기록도 읽습니다. key별 partition과 offset을 기록하세요.

**결과 해석**

```text
ASSIGNED [orders-0, orders-1, orders-2]
RECEIVED group=day02-reader key=order-2 partition=0 offset=0 value={...}
RECEIVED group=day02-reader key=order-2 partition=0 offset=1 value={...}
...
RECEIVED group=day02-reader key=order-0 partition=1 offset=0 value={...}
RECEIVED group=day02-reader key=order-1 partition=1 offset=1 value={...}
...
REVOKED [orders-0, orders-1, orders-2]
CONSUMED 15
```

- `max=0`이므로 개수 제한 없이 읽다가, 새 레코드가 25초 동안 없으면 종료합니다. `CONSUMED` 숫자는 01단원 기록을 포함한 전체 개수라 실행 환경마다 다릅니다. 01단원 3건과 이번 12건만 있었다면 15입니다.
- 출력은 파티션 단위로 몰려 나오는 경우가 많습니다. Consumer가 한 번의 fetch로 파티션별 묶음을 받아 오기 때문입니다. 파티션끼리의 출력 순서는 의미가 없습니다.
- 같은 partition의 줄만 보면 offset이 빠짐없이 증가하고, 같은 key의 이벤트는 보낸 순서대로 나옵니다. 이것이 Kafka가 보장하는 순서입니다.

## 더 실험하기

- 전송 로그에서 `order-0`에 해당하는 줄만 골라 partition이 모두 같은지 확인하세요.
- key 3개가 반드시 서로 다른 파티션에 들어가는지 확인해 보세요. 충돌은 정상이며, 어느 파티션이 비어 있는지도 기록합니다.
- `.\실습\study.ps1 groups --describe --group day02-reader`로 파티션별 LOG-END-OFFSET을 보고, 어느 파티션에 레코드가 몰렸는지 숫자로 비교해 보세요.

## 주의할 경계

그림의 order-A → P0 배치는 설명용입니다. 특정 문자열이 특정 파티션으로 간다고 코드에 하드코딩하지 않습니다. 파티션 수를 바꿔 보는 실험은 기존 `orders`를 건드리지 말고 새 토픽으로 분리하는 편이 비교하기 쉽고, 다른 단원의 실습에도 영향을 주지 않습니다.

## 로그 수집 파이프라인에 적용하면

**장비 로그의 key와 파티션 수는 어떻게 정할까?**

장비 로그 수집에서 key 선택은 '장비별 순서가 꼭 필요한가'라는 질문으로 시작합니다.

- **deviceId를 key로 쓰는 경우:** 한 장비의 로그가 항상 같은 파티션에 들어가므로 장비 단위로 발생 순서가 유지됩니다. 장비의 상태 전이(가동 → 정지 → 오류)를 순서대로 해석해야 하는 Consumer가 있다면 이 방식이 맞습니다. 대신 로그를 유난히 많이 쏟아내는 장비 하나가 파티션 하나를 뜨겁게 만들 수 있습니다. 장애가 난 장비는 평소보다 훨씬 많은 로그를 남기므로 쏠림은 하필 장애 상황에서 커집니다.
- **key를 비우는 경우:** 저장소에 적재만 하고 순서는 저장소의 시각 컬럼으로 정렬해 조회한다면 key가 없어도 됩니다. sticky 파티셔너가 배치를 잘 채워 처리량이 좋고 파티션 간 부하도 고르게 퍼집니다.

파티션 수는 '로그 저장 Consumer를 최대 몇 개까지 늘릴 것인가'와 '최대 초당 유입량'으로 정합니다. 나중에 줄일 수 없고, 늘리면 deviceId 기준 배치가 바뀌므로 처음부터 여유 있게 잡습니다.

보관 용량도 계산해 둡니다. `retention.bytes`는 파티션 하나의 한도이므로, '하루 유입량 × 보관 일수 × 복제 수'가 브로커 디스크에 들어가는지 확인합니다. 보관 기간은 '로그 저장소가 며칠 멈춰도 Kafka에서 다시 적재할 수 있어야 하는가'로 정합니다. 오래된 로그를 검색하는 일은 Kafka가 아니라 로그 저장소의 몫입니다.

## 자주 묻는 질문

### Q. 파티션이 3개인데 key가 3개면 파티션마다 하나씩 들어가나요?

보장되지 않습니다. 파티션은 `해시(key) % 파티션 수`로 정해지므로 서로 다른 key가 같은 파티션에 모일 수 있고, 어떤 파티션은 비어 있을 수 있습니다. 이 저장소의 실행 기록에서도 `order-0`과 `order-1`이 같은 partition 1로 갔습니다. key 종류가 수천 개 이상으로 많고 key마다 레코드 양이 비슷하다면 통계적으로 고르게 퍼집니다.

### Q. 토픽 전체에서 순서를 지키려면 어떻게 하나요?

파티션을 1개로 만들면 토픽 전체가 하나의 순서를 갖습니다. 대신 그룹 안에서 그 토픽을 읽는 Consumer도 사실상 1개로 제한되어 병렬 처리를 포기하게 됩니다. 대부분의 업무는 전역 순서가 아니라 '주문 단위', '장비 단위' 같은 범위의 순서만 필요하므로, 그 범위를 key로 삼는 편이 낫습니다.

### Q. offset이 0부터 시작하지 않거나 중간 번호가 비어 있어요.

정상입니다. 보관 기간이 지나 앞쪽 세그먼트가 지워지면 가장 오래된 offset이 0보다 커집니다. 트랜잭션을 쓰면 commit/abort를 표시하는 제어 레코드가 offset을 차지하지만 Consumer에게는 전달되지 않아 번호가 건너뛴 것처럼 보입니다. compaction이 정리한 자리도 빈 번호로 남습니다.

### Q. 파티션을 나중에 늘리면 기존 데이터도 다시 나눠지나요?

아닙니다. 기존 레코드는 원래 파티션에 그대로 남고, 증설 이후에 들어오는 레코드만 새 파티션 수로 계산된 위치에 들어갑니다. 그래서 같은 key의 과거 이벤트는 partition 1에, 새 이벤트는 partition 4에 있을 수 있습니다. key 단위 순서가 중요한 토픽은 증설 시점에 Consumer가 이 경계를 어떻게 다룰지 미리 정해야 합니다.

## 읽을 소스

- [발췌한 전체 소스](../초급/src/main/java/study/kafka/OrderProducer.java)

## 기록할 것

| 예상 결과 | 실제 결과 | 차이의 이유 |
|---|---|---|
| 직접 작성 | 직접 작성 | 직접 작성 |

## 완료 기준

- [ ] 파티션별 순서 보장을 설명한다
- [ ] key와 partition의 관계를 확인한다
- [ ] offset을 토픽 전체의 번호로 오해하지 않는다
- [ ] 파티션 수를 정할 때 따져 볼 기준과 보관 정책(delete · compact)을 구분한다

이해도 확인: Kafka가 기본적으로 보장하는 순서의 범위는?

<details><summary>답 확인</summary>

각 파티션 내부: 하나의 토픽에 여러 파티션이 있으면 파티션 간 전역 순서는 보장되지 않습니다. 순서가 필요한 이벤트는 같은 key로 보내 같은 파티션에 모읍니다.

</details>
