# 04. Producer · Consumer API

JSON 이벤트를 만들고, 전송 결과와 소비 위치를 코드에서 다룹니다. ack · 재시도 · 배치 · 압축 같은 Producer 설정과 Consumer의 fetch 설정이 무엇을 바꾸는지도 정리합니다.

먼저 [대응 이론 HTML](../../이론/초급/04.html)을 읽으세요.

## 준비

- 저장소 루트의 PowerShell에서 실행합니다.
- 01단원의 up → build → init을 완료해야 합니다.
- 기존 기록은 유지됩니다. 새 실험은 고유 group / runId를 사용하세요.
- `max=0`은 무한 상주가 아니라 입력이 끊긴 뒤 25초까지 읽는 모드입니다.

## 학습 목표

- 직렬화와 역직렬화를 설명한다
- 비동기 send의 결과를 확인한다
- 설정과 업무 코드를 구분한다
- acks · 재시도 · 타임아웃 · 배치 설정이 신뢰성과 처리량을 어떻게 바꾸는지 설명한다

## 실습 전에 떠올릴 핵심 개념

- 브로커는 바이트만 다루므로 Producer와 Consumer가 직렬화 형식을 약속해야 합니다.
- `send()`는 버퍼에 넣고 바로 반환하는 비동기 API이며, 결과는 `Future.get()`이나 callback으로 확인합니다.
- Kafka 4.x 기본값은 acks=all · 멱등 Producer · 사실상 무제한 재시도이며, 재시도는 `delivery.timeout.ms`로 시간을 제한합니다.
- 멱등 Producer는 내부 재시도 중복만 막고, 애플리케이션이 같은 이벤트를 두 번 보내는 중복은 막지 못합니다.
- `linger.ms`(4.0부터 기본 5) · `batch.size` · 압축으로 처리량과 지연의 균형을 조절합니다.
- `KafkaProducer`는 공유해서 쓰고, `KafkaConsumer`는 한 스레드에서만 다루며 `wakeup()`으로 안전하게 멈춥니다.

## 실행과 관찰

### 1. 실행 옵션 확인

```powershell
.\실습\study.ps1 run help
```

예상 결과: 전체 명령과 위치 인자가 출력됩니다.

**결과 해석**

```text
Kafka Field Guide | Java 21 + Kafka 4.3.1
init
producer [count=12] [topic=orders] [runId=random]
producer-async [count=100] [topic=orders] [runId=random]
consumer [group=beginner] [max=12; 0=until idle] [topic=orders] [delayMs=0] [isolation=read_uncommitted]
...
Consumer commands finish after 25 seconds without records.
```

`Lab.java`의 `help` 분기가 출력하는 내용입니다. 대괄호는 생략 가능한 위치 인자와 기본값을 뜻합니다. 인자는 이름이 아니라 **순서**로 전달하므로, 예를 들어 Consumer의 네 번째 인자(delayMs)를 주려면 앞의 세 인자도 모두 적어야 합니다. 이후 단원의 명령이 왜 그런 모양인지 이 목록으로 읽을 수 있습니다.

### 2. 같은 eventId로 발행

```powershell
.\실습\study.ps1 run producer 3 orders day04
```

예상 결과: SENT 3건입니다. 같은 명령을 한 번 더 실행하세요.

**결과 해석**

```text
# 첫 번째 실행
SENT event=day04-0 key=order-0 partition=1 offset=50
SENT event=day04-1 key=order-1 partition=1 offset=51
SENT event=day04-2 key=order-2 partition=0 offset=25
# 두 번째 실행
SENT event=day04-0 key=order-0 partition=1 offset=52
SENT event=day04-1 key=order-1 partition=1 offset=53
SENT event=day04-2 key=order-2 partition=0 offset=26
```

offset 숫자는 01~03단원 명령만 그대로 실행해 partition 1에 50건, partition 0에 25건이 쌓여 있다고 가정한 값입니다. 다른 실험을 했다면 달라집니다. runId를 `day04`로 고정했으므로 두 실행 모두 같은 eventId(`day04-0` 등)를 만듭니다. 그런데 두 번째 실행의 offset은 새 번호입니다. 멱등 Producer가 켜져 있어도 **새 `send()` 호출은 새 레코드**로 저장된다는 증거입니다. 멱등성은 한 Producer가 같은 배치를 재시도할 때만 중복을 걸러냅니다.

### 3. 중복을 관찰

```powershell
.\실습\study.ps1 run consumer java-demo 0 orders
```

예상 결과: day04-0 등 같은 eventId가 서로 다른 offset으로 두 번 존재하는지 확인합니다.

**결과 해석**

```text
RECEIVED group=java-demo key=order-0 partition=1 offset=50 value={"schemaVersion":1,"eventId":"day04-0",...}
RECEIVED group=java-demo key=order-0 partition=1 offset=52 value={"schemaVersion":1,"eventId":"day04-0",...}
```

새 그룹이므로 토픽의 기록 전체를 읽습니다. 출력이 길다면 `day04-0`이 들어간 줄만 찾아보세요. 같은 eventId가 같은 partition의 서로 다른 offset에 두 번 있습니다. Kafka 입장에서 이 둘은 완전히 별개의 레코드이며, value 안의 eventId를 해석하는 것은 Consumer의 몫입니다. 이 중복을 업무 결과에 반영하지 않는 방법이 06단원의 멱등 처리입니다.

### 4. 비동기 결과와 대기열 제한

```powershell
.\실습\study.ps1 run producer-async 12 orders day04-async
```

예상 결과: ASYNC_ACK 12줄이 출력됩니다. 파티션 간 callback 출력 순서는 다를 수 있습니다.

**결과 해석**

```text
ASYNC_ACK event=day04-async-0 partition=1 offset=54
ASYNC_ACK event=day04-async-2 partition=0 offset=27
ASYNC_ACK event=day04-async-5 partition=0 offset=28
ASYNC_ACK event=day04-async-8 partition=0 offset=29
ASYNC_ACK event=day04-async-11 partition=0 offset=30
ASYNC_ACK event=day04-async-1 partition=1 offset=55
...
```

이 저장소의 검증 기록(`async-producer.log`)에서도 출력이 eventId 순서(0, 1, 2, ...)가 아니라 파티션별 묶음으로 나왔습니다. 레코드가 파티션별 배치로 묶여 전송되고, 응답도 배치 단위로 돌아와 callback이 몰아서 호출되기 때문입니다. **같은 partition 안에서는 offset이 보낸 순서대로 증가**하는지 확인하세요. 이 Producer는 금액을 모두 1000으로 보내며, `linger.ms=5`, `batch.size=32768`, `lz4` 압축을 씁니다.

### 5. 실제 연결 검증

```powershell
.\실습\study.ps1 run verify
```

예상 결과: 전용 토픽을 만든 뒤 송수신·커밋·트랜잭션·DB를 검증하고 ALL_CHECKS_PASSED를 출력합니다.

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

`IntegrationCheck`가 `verify-`로 시작하는 고유 이름의 토픽과 그룹을 만들어 검사하고, 끝나면 자기 데이터만 정리합니다. 지금 단계에서는 앞의 세 줄(송수신, key별 파티션과 offset 증가, 커밋 후 재시작)이 이 단원과 관련 있습니다. 나머지 PASS는 06·09·10·12단원의 보장이며, 해당 단원을 마친 뒤 다시 실행하면 각 줄의 의미가 읽힙니다. 하나라도 실패하면 `AssertionError`와 함께 실패한 항목 이름이 출력됩니다.

## 더 실험하기

- `OrderProducer`에서 amount 계산식을 바꾸고 `build` 후 다시 실행하세요. 빌드 전후 결과가 어떻게 다른지 확인합니다. 빌드하지 않으면 이전 JAR가 그대로 실행됩니다.
- 한글 주문 정보를 JSON의 선택 필드로 추가하고 `schema` 명령의 호환성 실습(12단원)과 연결해 보세요.
- `AsyncOrderProducer`의 `compression.type`을 `zstd`나 `none`으로 바꿔 빌드하고, 많은 건수(예: 5000건)를 보낼 때 걸리는 시간을 비교해 보세요. 결과는 PC 성능에 따라 다릅니다.

## 주의할 경계

제공 코드는 운영에서 쓰는 API와 패턴을 재현한 학습용 구현입니다. 인증, 커넥션 풀, 관측, 부하 제어, 개인정보 정책을 갖춘 완성 서비스라는 뜻은 아닙니다. 특히 짧은 타임아웃 값은 실험 결과를 빨리 보기 위한 것이므로 운영 설정으로 그대로 옮기지 않습니다.

## 로그 수집 파이프라인에 적용하면

**수집 서버의 Producer: 배치 · 압축 · 역압력(backpressure)**

장비 로그 수집 서버는 HTTP로 로그 묶음을 받아 Kafka로 넘기는 Producer입니다. 초당 수만 줄이 오가는 환경에서는 이 단원의 설정이 그대로 성능과 신뢰성을 정합니다.

- **비동기 전송과 배치:** 로그 한 줄마다 `send().get()`으로 기다리면 처리량이 크게 떨어집니다. callback 방식으로 보내고, `linger.ms`와 `batch.size`를 키워 배치를 잘 채웁니다. 텍스트 로그는 반복되는 문자열이 많아 `lz4`나 `zstd` 압축의 효과가 큰 편입니다.
- **응답 시점 정하기:** 에이전트에게 HTTP 200을 언제 돌려줄지가 유실 여부를 가릅니다. Kafka ack를 받은 뒤 응답하면, 그 전에 수집 서버가 죽더라도 응답을 받지 못한 에이전트가 재전송하므로 로그를 잃지 않습니다. 대신 재전송 때문에 같은 로그가 두 번 들어갈 수 있으므로 로그마다 고유 ID를 붙여 두는 편이 좋습니다(06단원). ack 전에 응답하면 빠르지만 그 사이 장애 때 로그가 사라질 수 있습니다.
- **역압력:** Kafka가 느려지면 Producer 버퍼(`buffer.memory`)가 차고 `send()`가 `max.block.ms`만큼 막힙니다. HTTP 처리 스레드가 모두 여기 묶이면 수집 서버 전체가 멈춘 것처럼 보입니다. `AsyncOrderProducer`의 `Semaphore`처럼 동시에 대기하는 전송 수를 제한하고, 한도를 넘으면 에이전트에게 429나 503을 돌려 잠시 뒤 재시도하게 하는 편이 안전합니다. 수집 에이전트는 보통 자체 버퍼와 재시도 기능을 갖추고 있습니다.
- **Producer는 하나를 공유:** HTTP 요청마다 `KafkaProducer`를 새로 만들지 않습니다. 애플리케이션 전체에서 하나를 재사용합니다.

로그는 주문만큼 한 건 한 건이 중요하지 않을 수도 있습니다. 그래도 장애 분석에 쓰이는 로그를 잃지 않으려면 `acks=all`과 멱등 Producer(기본값)를 유지하고, 처리량은 배치와 압축으로 확보하는 것이 일반적인 출발점입니다.

## 자주 묻는 질문

### Q. `send().get()`을 항상 쓰면 더 안전하지 않나요?

결과를 한 건씩 확실히 알 수 있지만, 레코드마다 브로커 왕복을 기다리므로 처리량이 크게 떨어집니다. Kafka 4.x에서는 `linger.ms` 기본값이 5라서 한 건씩 기다리면 건마다 배치 대기 시간까지 더해질 수 있습니다. callback 방식으로도 실패를 놓치지 않을 수 있으므로, 대량 전송에서는 callback과 `flush()`로 결과를 확인합니다.

### Q. `acks=all`이면 데이터가 절대 사라지지 않나요?

`acks=all`은 '현재 ISR 전체'의 확인을 기다립니다. 복제 수가 1이거나 ISR이 리더 하나로 줄어든 상태라면, 리더 하나만 확인해도 `all` 조건을 만족합니다. 그래서 복제 수 3과 `min.insync.replicas=2`처럼 최소 동기화 복제본 수를 함께 정해야 실제 내구성을 기대할 수 있습니다. 08단원에서 브로커를 멈추며 확인합니다.

### Q. 멱등 Producer를 쓰면 Exactly Once가 되나요?

아닙니다. 멱등 Producer는 한 Producer 세션 안의 재시도 중복을 막을 뿐입니다. 애플리케이션이 같은 이벤트를 다시 보내거나, Consumer가 처리 후 커밋 전에 죽어 다시 처리하는 중복은 그대로 남습니다. Kafka 안에서 읽기·처리·쓰기를 한 번으로 묶는 방법은 09단원의 트랜잭션, 외부 DB까지 고려한 방법은 06·10단원에서 다룹니다.

### Q. callback 안에서 DB에 결과를 저장해도 되나요?

권하지 않습니다. callback은 Producer의 I/O 스레드에서 실행되므로, 그 안에서 오래 걸리는 작업을 하면 다른 레코드의 전송과 응답 처리까지 함께 늦어집니다. callback에서는 성공·실패를 기록하거나 큐에 넘기는 정도로 짧게 끝내고, 무거운 작업은 별도 스레드에서 처리합니다.

## 읽을 소스

- [발췌한 전체 소스](../공통/src/main/java/study/kafka/Settings.java)

## 기록할 것

| 예상 결과 | 실제 결과 | 차이의 이유 |
|---|---|---|
| 직접 작성 | 직접 작성 | 직접 작성 |

## 완료 기준

- [ ] 직렬화와 역직렬화를 설명한다
- [ ] 비동기 send의 결과를 확인한다
- [ ] 설정과 업무 코드를 구분한다
- [ ] acks · 재시도 · 타임아웃 · 배치 설정이 신뢰성과 처리량을 어떻게 바꾸는지 설명한다

이해도 확인: 멱등 Producer를 사용하면서 같은 이벤트를 새 send 호출로 두 번 보내면?

<details><summary>답 확인</summary>

두 레코드가 기록될 수 있다: Producer 멱등성은 같은 배치의 재시도 중복만 걸러내며, 애플리케이션의 업무 eventId 중복은 판별하지 않습니다. 실습의 두 번째 발행이 새 offset을 받은 것이 그 증거입니다.

</details>
