# 01. 이벤트와 Kafka의 구조

주문 한 건이 여러 서비스에 도착하는 길을 따라가며 Kafka의 역할과 구성 요소를 이해합니다.

먼저 [대응 이론 HTML](../../이론/초급/01.html)을 읽으세요.

## 준비

- 저장소 루트의 PowerShell에서 실행합니다.
- 01단원의 up → build → init을 완료해야 합니다.
- 기존 기록은 유지됩니다. 새 실험은 고유 group / runId를 사용하세요.
- `max=0`은 무한 상주가 아니라 입력이 끊긴 뒤 25초까지 읽는 모드입니다.

## 학습 목표

- 요청·응답과 이벤트 전달을 구분한다
- Producer → Broker → Consumer의 책임을 설명한다
- 레코드를 이루는 key · value · timestamp · headers를 말할 수 있다
- 토픽을 만들고 저장된 메시지를 읽는다

## 실습 전에 떠올릴 핵심 개념

- 이벤트는 이미 일어난 사실이고, 받는 쪽이 각자 자기 업무에 맞게 반응합니다.
- Kafka는 레코드를 지우지 않고 로그에 덧붙여 보관하며, Consumer는 자기 위치(offset)부터 스스로 가져가 읽습니다.
- Producer의 ack는 'Kafka 기록 확인'이지 '후속 업무 완료'가 아닙니다. 성공의 시점은 여러 개로 나뉩니다.
- 레코드는 key · value · timestamp · headers로 이루어지며, key는 파티션 선택과 순서에 영향을 줍니다.
- 토픽은 자동 생성에 맡기지 않고 파티션 수 · 복제 수 · 보관 기간을 정해 명시적으로 만듭니다.

## 실행과 관찰

### 1. 환경 시작

```powershell
.\실습\study.ps1 up
```

예상 결과: Kafka와 PostgreSQL이 healthy가 됩니다. 첫 실행은 이미지 다운로드 시간이 필요합니다.

**결과 해석**

`up`은 Docker Compose로 `kafka`(브로커 겸 컨트롤러 1대)와 `postgres` 컨테이너를 띄우고, 상태 검사(healthcheck)가 통과할 때까지 기다립니다. Kafka의 상태 검사는 컨테이너 안에서 `kafka-topics.sh --list`가 성공하는지로 판단합니다. 마지막에 `Container kafka-field-guide-kafka-1 Healthy` 같은 줄이 보이면 준비된 것입니다.

```text
 Container kafka-field-guide-postgres-1 Healthy
 Container kafka-field-guide-kafka-1 Healthy
```

이 단계가 오래 걸리거나 실패하면 Docker Desktop이 실행 중인지, 19092·15432 포트를 다른 프로그램이 쓰고 있지 않은지 확인하세요. `.\실습\study.ps1 status`로 컨테이너 상태를 볼 수 있습니다.

### 2. 코드 빌드

```powershell
.\실습\study.ps1 build
```

예상 결과: 실습/target/kafka-labs.jar가 생성됩니다.

**결과 해석**

호스트에 Java와 Maven이 없어도 되도록, Maven 컨테이너가 `실습` 폴더를 빌드해 실행 가능한 JAR 하나를 만듭니다. 이후의 모든 `run` 명령은 이 JAR를 Java 컨테이너에서 실행합니다.

`-q` 옵션으로 조용히 빌드하므로 성공하면 별다른 출력 없이 끝납니다. 오류가 나면 컴파일 오류 메시지가 그대로 보입니다. Java 소스를 고쳤다면 반드시 다시 `build`해야 변경이 반영됩니다.

### 3. 토픽 생성

```powershell
.\실습\study.ps1 init
```

예상 결과: orders 등 5개 토픽과 각 파티션의 Leader/Replicas/ISR가 출력됩니다.

**결과 해석**

`TopicAdmin`이 Admin API로 토픽 5개를 만들고, 각 토픽의 파티션 정보를 출력합니다. 한 토픽의 출력은 다음과 같습니다(줄바꿈은 읽기 쉽게 넣었습니다).

```text
orders [(partition=0, leader=kafka:9092 (id: 1 rack: null isFenced: false),
         replicas=kafka:9092 (id: 1 ...), isr=kafka:9092 (id: 1 ...), elr=, lastKnownElr=),
        (partition=1, ...), (partition=2, ...)]
```

- `partition=0`, `1`, `2`는 토픽이 3개의 파티션으로 나뉘었다는 뜻입니다.
- `leader`는 그 파티션의 읽기·쓰기를 담당하는 브로커입니다. 단일 환경에는 브로커가 하나뿐이라 모두 `id: 1`입니다.
- `replicas`는 복제본을 가진 브로커 목록, `isr`는 그중 현재 리더와 동기화된 복제본 목록입니다.
- `elr`, `lastKnownElr`는 Kafka 4.x의 Eligible Leader Replicas 정보입니다. 정상 상태에서는 비어 있습니다(08단원).

### 4. 첫 이벤트

```powershell
.\실습\study.ps1 run producer 3 orders day01
```

예상 결과: SENT 3줄이 출력됩니다.

**결과 해석**

```text
SENT event=day01-0 key=order-0 partition=1 offset=0
SENT event=day01-1 key=order-1 partition=1 offset=1
SENT event=day01-2 key=order-2 partition=0 offset=0
```

한 줄은 '레코드 한 건이 브로커에 기록되고 ack를 받았다'는 뜻입니다. `event`는 eventId, `key`는 orderId입니다. `partition`과 `offset`은 브로커가 ack와 함께 돌려준 **저장 위치**입니다.

offset은 파티션마다 0부터 따로 셉니다. 위 예에서 partition 1의 offset 0과 partition 0의 offset 0은 서로 다른 레코드입니다. 어떤 key가 어느 파티션으로 가는지는 key의 해시 값과 파티션 수로 정해집니다. 그래서 같은 key, 같은 파티션 수라면 누가 언제 실행해도 결과가 같고, 이 실습에서는 `order-0`과 `order-1`이 partition 1로, `order-2`가 partition 0으로 갑니다. partition 2에는 이 key들이 하나도 가지 않는다는 점도 기억해 두세요. 03단원의 실험 결과를 해석할 때 다시 등장합니다. 이미 데이터가 있는 토픽이라면 offset은 0이 아닌 다음 번호부터 이어집니다.

### 5. 읽기

```powershell
.\실습\study.ps1 run consumer day01-reader 3 orders
```

예상 결과: RECEIVED 3줄과 CONSUMED 3이 출력됩니다.

**결과 해석**

```text
ASSIGNED [orders-0, orders-1, orders-2]
RECEIVED group=day01-reader key=order-2 partition=0 offset=0 value={"schemaVersion":1,"eventId":"day01-2",...}
RECEIVED group=day01-reader key=order-0 partition=1 offset=0 value={"schemaVersion":1,"eventId":"day01-0",...}
RECEIVED group=day01-reader key=order-1 partition=1 offset=1 value={"schemaVersion":1,"eventId":"day01-1",...}
REVOKED [orders-0, orders-1, orders-2]
CONSUMED 3
```

- `ASSIGNED`는 이 Consumer가 그룹에 참여해 세 파티션을 모두 배정받았다는 뜻입니다. 그룹에 Consumer가 하나뿐이기 때문입니다.
- `RECEIVED`의 순서가 `SENT`와 다를 수 있습니다. Kafka는 **파티션 안에서만** 순서를 지키며, 여러 파티션의 레코드가 섞여 나오는 것은 정상입니다.
- `day01-reader`는 처음 쓰는 그룹이라 커밋 기록이 없고, 이 과정의 Consumer 설정이 `auto.offset.reset=earliest`이므로 각 파티션의 가장 오래된 레코드부터 읽습니다.
- 끝의 `REVOKED`는 Consumer가 종료하면서 그룹을 떠나 파티션 배정이 해제되었다는 표시입니다.

## 더 실험하기

- 새 그룹 `day01-other`로 같은 3건을 다시 읽어 보세요. 읽었다고 레코드가 삭제되지는 않으며, 다른 그룹은 처음부터 독립적으로 읽습니다.
- 같은 그룹 `day01-reader`로 다시 실행하면 커밋한 위치에서 이어 읽습니다. 새 메시지가 없으면 25초 뒤 `CONSUMED 0`으로 종료합니다.
- `.\실습\study.ps1 groups --describe --group day01-reader`로 그룹의 파티션별 CURRENT-OFFSET, LOG-END-OFFSET, LAG을 확인해 보세요. 명령 사용법은 부록 A1에 있습니다.

## 주의할 경계

실습 명령은 이미 저장된 데이터에 이어서 동작합니다. 처음부터 관찰하려면 새 group 이름을 쓰세요. 이미 커밋 기록이 있는 그룹은 `auto.offset.reset=earliest`를 설정해도 처음으로 돌아가지 않습니다. 이 설정은 커밋 기록이 없는 그룹에만 적용됩니다.

## 로그 수집 파이프라인에 적용하면

**장비 로그 수집에서 Kafka는 '완충 지대'를 맡습니다**

장비 수백~수천 대가 남기는 로그를 모아 저장·검색하는 시스템을 떠올려 보세요. 보통 장비 옆의 **수집 에이전트**(Fluent Bit 등)가 로그를 읽어 **수집 서버**(Spring 등으로 만든 HTTP API)로 보내고, 수집 서버가 이를 Kafka에 기록합니다. 그 뒤에서 **저장 Consumer**가 로그 저장소(ClickHouse, Elasticsearch 등)에 적재하고, 필요하면 **알람 Consumer**나 실시간 대시보드가 같은 토픽을 따로 구독합니다.

이 구조에서 Kafka가 하는 일은 이 단원의 개념 그대로입니다.

- **수집과 저장의 분리:** 로그 저장소가 점검이나 장애로 멈춰도 수집 서버는 Kafka에 계속 기록합니다. 저장 Consumer가 돌아오면 밀린 로그를 이어서 적재합니다. 수집 서버가 저장소의 상태를 몰라도 됩니다.
- **폭주 흡수:** 장비 재부팅이나 장애 상황에서는 로그가 평소의 몇 배로 쏟아집니다. Kafka가 디스크에 쌓아 두면 저장소는 자기 속도로 처리합니다.
- **여러 구독자:** 저장용, 알람용, 통계용 Consumer Group이 같은 로그를 서로 간섭 없이 읽습니다.
- **재적재:** 저장소의 테이블 구조를 바꾸거나 적재 버그를 고친 뒤, 보관 기간 안의 로그를 새 그룹으로 다시 읽어 적재할 수 있습니다. 보관 기간은 '저장소가 최대 며칠 멈춰도 복구할 수 있어야 하는가'를 기준으로 정합니다.

로그 한 줄도 이미 일어난 사실의 기록이므로 이벤트와 성격이 같습니다. 다만 주의할 점이 있습니다. 장비 시계가 틀어져 있는 경우가 흔하므로, 장비가 기록한 시각은 value 안의 필드(예: `deviceTime`)로 보관하고 레코드 timestamp는 수집 서버의 시각을 쓰는 편이 안전합니다. Kafka 4.x는 1시간 넘게 미래로 찍힌 `CreateTime` 레코드를 기본적으로 거부하기 때문입니다.

## 자주 묻는 질문

### Q. Kafka는 Consumer가 메시지를 읽으면 지우나요?

지우지 않습니다. 삭제는 읽기 여부와 관계없이 토픽의 보관 정책(`retention.ms`, `retention.bytes`)에 따라 오래된 세그먼트 단위로 일어납니다. 기본 보관 기간은 7일이며, 이 과정의 토픽도 7일로 만듭니다. 그래서 새 Consumer Group은 보관 기간 안의 과거 데이터를 처음부터 읽을 수 있습니다.

### Q. Kafka가 데이터베이스를 대신할 수 있나요?

일반적으로는 아닙니다. Kafka는 순서대로 덧붙이고 위치로 읽는 로그라서, '주문 order-7의 현재 상태'처럼 key로 최신 값을 즉시 조회하는 데 적합하지 않습니다. 보관 기간이 지나면 데이터도 사라집니다. 업무 상태는 DB에 두고, 상태가 바뀌었다는 사실을 Kafka로 전파하는 조합이 흔합니다. 10단원의 Outbox 패턴이 이 둘을 안전하게 잇는 방법입니다.

### Q. ZooKeeper는 설치하지 않아도 되나요?

Kafka 4.0부터 ZooKeeper 모드가 제거되어 KRaft 모드만 지원합니다. 클러스터 메타데이터는 Kafka의 컨트롤러들이 Raft 합의로 관리합니다. 이 실습의 `compose.yaml`이 `KAFKA_PROCESS_ROLES: broker,controller`로 한 프로세스에 두 역할을 맡기는 것도 그래서입니다. 오래된 블로그 글에서 ZooKeeper 설정을 보더라도 4.x에는 적용되지 않습니다.

### Q. `send()`가 예외 없이 끝나면 저장된 것인가요?

아닙니다. `send()`는 레코드를 Producer 내부 버퍼에 넣고 곧바로 반환하는 비동기 API입니다. 저장 여부는 반환된 `Future`의 `get()`이나 callback에서 확인해야 합니다. 이 과정의 `OrderProducer`가 `send(...).get()`을 쓰는 이유이며, 04단원에서 자세히 다룹니다.

## 읽을 소스

- [발췌한 전체 소스](../초급/src/main/java/study/kafka/TopicAdmin.java)

## 기록할 것

| 예상 결과 | 실제 결과 | 차이의 이유 |
|---|---|---|
| 직접 작성 | 직접 작성 | 직접 작성 |

## 완료 기준

- [ ] 요청·응답과 이벤트 전달을 구분한다
- [ ] Producer → Broker → Consumer의 책임을 설명한다
- [ ] 레코드를 이루는 key · value · timestamp · headers를 말할 수 있다
- [ ] 토픽을 만들고 저장된 메시지를 읽는다

이해도 확인: Producer가 전송 성공 응답을 받았습니다. 확실히 말할 수 있는 것은?

<details><summary>답 확인</summary>

설정된 ack 조건에 따라 Kafka 기록이 확인되었다: Kafka 기록 확인과 Consumer의 업무 완료는 서로 다른 시점입니다. 기록은 보관 기간 동안 다시 읽을 수도 있습니다.

</details>
