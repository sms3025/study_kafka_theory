# 11. Kafka Streams · 상태 저장

key별 누적 count를 만들고, 상태 저장소 · changelog · 처리 보장이 어떻게 맞물리는지 관찰합니다.

먼저 [대응 이론 HTML](../../이론/고급/11.html)을 읽으세요.

## 준비

- 저장소 루트의 PowerShell에서 실행합니다.
- 01단원의 up → build → init을 완료해야 합니다.
- 기존 기록은 유지됩니다. 새 실험은 고유 group / runId를 사용하세요.
- `max=0`은 무한 상주가 아니라 입력이 끊긴 뒤 25초까지 읽는 모드입니다.

## 학습 목표

- Consumer 처리와 Streams topology를 비교한다
- KStream과 집계 KTable을 구분한다
- 상태 저장소·changelog·application.id를 설명한다
- 윈도우 집계와 이벤트 시간의 관계를 설명한다

## 실습 전에 떠올릴 핵심 개념

- Kafka Streams는 앱 안에 넣는 라이브러리이며, `application.id`가 같은 인스턴스끼리 Consumer Group처럼 파티션을 나눠 처리합니다.
- KStream은 사건의 흐름, `count()`로 만든 KTable은 key별 최신 상태입니다. 출력 토픽은 key별 마지막 값으로 해석합니다.
- 상태는 로컬 저장소에 두고 모든 변경을 changelog 토픽에 기록해 복구합니다. key를 바꾼 뒤의 집계에는 repartition 토픽이 생깁니다.
- `exactly_once_v2`는 입력에 실제로 있는 레코드를 한 번씩 반영할 뿐, 같은 eventId의 중복 이벤트를 걸러 주지 않습니다.
- 레코드 캐시와 커밋 주기 때문에 모든 중간 count가 출력되지는 않습니다.
- 병렬성은 입력 파티션 수(task 수)가 상한이며, 윈도우 집계는 이벤트 시간과 grace period로 늦은 레코드를 다룹니다.

## 실행과 관찰

### 1. 60초 동안 집계

```powershell
.\실습\study.ps1 run streams 60 order-counts-day11
```

예상 결과: STREAMS 상태가 RUNNING으로 이동하고 지정 시간 후 종료합니다.

**결과 해석**

```text
STREAMS CREATED -> REBALANCING
STREAMS REBALANCING -> RUNNING
STREAMS RUNNING -> PENDING_SHUTDOWN
STREAMS PENDING_SHUTDOWN -> NOT_RUNNING
[main] WARN org.apache.kafka.streams.StreamsConfig - Processing exception handler is not enabled for the GlobalThread. ...
[...-StreamThread-1] WARN org.apache.kafka.clients.NetworkClient - ... {order-counts-day11-order-counts-repartition=UNKNOWN_TOPIC_OR_PARTITION}
```

`STREAMS` 줄은 `KafkaStreams`의 상태 변화입니다. `REBALANCING`은 그룹에 참여해 task를 배정받는 중, `RUNNING`은 처리 중이라는 뜻이며, 60초가 지나면 `close()`로 정상 종료합니다. 표준 출력과 경고의 순서는 섞여 보일 수 있습니다.

두 WARN은 예상된 것입니다. 첫 번째는 예외 처리기의 기본 동작이 향후 버전에서 바뀐다는 안내이고, 두 번째는 Streams가 repartition 토픽을 만들기 직전에 메타데이터를 조회해서 생기는 일시적 경고로 스스로 회복됩니다. 새 `application.id`이므로 `orders`의 처음부터 읽어 이전 단원의 이벤트까지 모두 셉니다.

### 2. 다른 터미널에서 입력

```powershell
.\실습\study.ps1 run producer 12 orders day11
```

예상 결과: 주문별 이벤트가 입력됩니다. 이전 orders 기록도 같은 app의 진행 상태에 따라 집계합니다.

**결과 해석**

```text
SENT event=day11-0 key=order-0 partition=1 offset=40
SENT event=day11-1 key=order-1 partition=1 offset=41
SENT event=day11-2 key=order-2 partition=0 offset=20
...
```

Streams가 `RUNNING`인 60초 안에 실행해야 합니다. `order-0`, `order-1`, `order-2`에 각각 4건씩 들어가며, Streams는 커밋 주기(1초)마다 바뀐 count를 내보냅니다.

offset 숫자는 예시입니다. 이 명령이 Streams 종료 뒤에 실행되었더라도 데이터는 `orders`에 남아 있으므로, 다음에 같은 `application.id`로 Streams를 실행하면 커밋된 위치부터 이어서 반영합니다.

### 3. 결과 조회

```powershell
.\실습\study.ps1 run consumer count-reader-day11 0 orders.counts
```

예상 결과: key별 count 문자열이 출력됩니다. 중간값 전부가 출력될 필요는 없습니다.

**결과 해석**

```text
ASSIGNED [orders.counts-0, orders.counts-1, orders.counts-2]
RECEIVED group=count-reader-day11 key=order-0 partition=1 offset=0 value=10
RECEIVED group=count-reader-day11 key=order-1 partition=1 offset=1 value=10
RECEIVED group=count-reader-day11 key=order-2 partition=0 offset=0 value=1
RECEIVED group=count-reader-day11 key=order-2 partition=0 offset=2 value=10
RECEIVED group=count-reader-day11 key=day10-order-0 partition=0 offset=3 value=2
...
CONSUMED 9
```

value가 그 시점의 key별 누적 이벤트 건수입니다. 같은 key(`order-2`)가 1과 10처럼 여러 번 나오는 것은 커밋 주기마다 내보낸 갱신이며, **마지막 값이 현재 상태**입니다. 1과 10 사이의 값들은 캐시에서 합쳐져 나오지 않았습니다.

`day10-order-0`의 값이 2라면 10단원에서 Relay가 같은 이벤트를 두 번 발행했기 때문입니다. exactly_once_v2로 실행해도 입력에 실제로 두 건이 있으면 둘 다 셉니다. 숫자와 줄 수는 토픽에 쌓인 데이터에 따라 달라집니다.

### 4. 같은 application.id로 재시작

```powershell
.\실습\study.ps1 run streams 30 order-counts-day11
```

예상 결과: 기존 입력 offset과 상태에서 복구하여 이어 처리합니다.

**결과 해석**

```text
STREAMS CREATED -> REBALANCING
STREAMS REBALANCING -> RUNNING
STREAMS RUNNING -> PENDING_SHUTDOWN
STREAMS PENDING_SHUTDOWN -> NOT_RUNNING
```

같은 `application.id`이므로 Streams는 커밋된 입력 위치와 로컬 상태 저장소(`streams-state` 볼륨)를 그대로 이어받습니다. 이미 반영한 이벤트를 다시 세지 않으므로, 새 입력이 없으면 `orders.counts`에 새 출력도 생기지 않습니다.

이어서 계산하는 모습을 보려면 이 30초 동안 다른 터미널에서 `producer`를 한 번 더 실행하고 결과를 다시 조회하세요. 예를 들어 `order-0`이 10이었다면 14처럼 이전 값에서 이어집니다. 로컬 상태가 없는 새 인스턴스였다면 changelog 토픽을 읽어 같은 상태를 복구한 뒤 시작합니다. `.\실습\study.ps1 topics`로 `...-changelog`와 `...-repartition` 내부 토픽도 확인해 보세요.

## 더 실험하기

- 새 `application.id`로 실행한 결과와 기존 `application.id`로 재시작한 결과를 비교하세요. 새 ID는 처음부터 다시 세고, 기존 ID는 이어서 셉니다.
- 같은 runId로 Producer를 두 번 실행해, EOS가 업무 eventId 중복 제거와 다른 이유를 count 값으로 확인하세요.
- `.\실습\study.ps1 topics` 출력에서 changelog 토픽의 설정(`cleanup.policy`)과 repartition 토픽의 파티션 수를 찾아보세요.

## 주의할 경계

동일 `application.id`를 서로 다른 Kafka 클러스터에 쓸 때 로컬 state 디렉터리를 공유하지 마세요. single과 cluster 환경은 같은 `streams-state` 볼륨을 쓰므로, 환경을 오갈 때는 `counts-single-v1`, `counts-cluster-v1`처럼 app ID에 접미사를 붙여 상태를 분리합니다.

## 로그 수집 파이프라인에 적용하면

**장비별 오류 건수를 1분 단위로 실시간 집계하기**

로그 파이프라인에서 Streams가 가장 잘 맞는 일은 **저장 전에 흐름 위에서 바로 계산하는 일**입니다. 예를 들어 '장비별 ERROR 로그가 1분에 몇 건인가'를 실시간으로 세어, 기준을 넘으면 알람 토픽으로 보내는 처리기를 생각해 보세요.

- **key는 장비 ID:** 로그를 장비 ID로 `selectKey`한 뒤 그룹화합니다. 수집 단계에서 이미 장비 ID를 key로 썼다면 repartition 없이 집계할 수 있습니다.
- **1분 Tumbling 윈도우:** ERROR만 `filter`로 남긴 뒤 1분 Tumbling 윈도우로 `count()`합니다. 결과는 '장비 A, 10:03~10:04, 17건'처럼 구간별로 나옵니다.
- **이벤트 시간과 늦은 로그:** 에이전트가 네트워크 장애 동안 로그를 버퍼에 쌓았다가 한꺼번에 보내면, 도착 시각이 아닌 장비 시각 기준으로 세야 정확합니다. `TimestampExtractor`로 value의 장비 시각을 쓰고, 버퍼링 시간을 고려해 grace period를 정합니다. 장비 시계가 틀어진 경우도 있으므로 터무니없는 시각의 로그는 미리 걸러냅니다.
- **확정된 결과만 알람으로:** 윈도우 결과는 늦은 레코드 때문에 여러 번 갱신될 수 있습니다. 알람이 여러 번 울리지 않게 하려면 구간이 닫힌 뒤 최종값만 내보내도록(`suppress`) 설계합니다.

원본 로그 전체의 영구 저장과 검색은 로그 저장소의 몫이고, Streams는 실시간으로 봐야 하는 소수의 지표를 계산하는 데 씁니다. 처리량은 입력 토픽의 파티션 수가 상한이므로, 장비 수가 늘 것을 고려해 파티션 수를 정합니다(02단원).

## 자주 묻는 질문

### Q. Kafka Streams를 쓰려면 별도 서버를 설치해야 하나요?

아닙니다. Streams는 Java 라이브러리이므로 의존성을 추가하고 애플리케이션 안에서 실행합니다. 필요한 것은 Kafka 클러스터뿐입니다. 처리량을 늘리려면 같은 `application.id`로 애플리케이션 인스턴스를 더 띄우면 되고, 그 상한은 입력 토픽의 파티션 수입니다.

### Q. 실행할 때 나오는 WARN 로그는 문제가 있다는 뜻인가요?

이 실습에서 보이는 두 경고는 정상입니다. 하나는 예외 처리기 관련 설정의 기본 동작이 향후 버전에서 바뀐다는 안내이고, 다른 하나는 repartition 토픽이 만들어지기 전 잠깐 `UNKNOWN_TOPIC_OR_PARTITION`이 보고되는 일시적 경고입니다. 상태가 `RUNNING`으로 바뀌고 결과가 나온다면 처리는 정상입니다. 같은 경고가 계속 반복되거나 `ERROR`가 나오면 그때 원인을 확인합니다.

### Q. `orders.counts`에 같은 key가 여러 번 나오는데 무엇을 믿어야 하나요?

가장 마지막 값을 믿습니다. `count()` 결과는 KTable이고, `toStream()`은 상태가 바뀔 때마다 그 결과를 내보냅니다. 출력 토픽을 KTable로 다시 읽거나, compaction 정책의 토픽으로 만들어 key별 최신 값만 남기는 방법도 있습니다.

### Q. `application.id`를 바꾸면 이전 상태와 내부 토픽은 어떻게 되나요?

새 ID는 완전히 새 애플리케이션으로 취급되어 입력을 처음부터 다시 읽고 새 상태와 내부 토픽을 만듭니다. 이전 ID의 내부 토픽과 로컬 상태는 자동으로 지워지지 않습니다. 같은 ID로 처음부터 다시 계산하고 싶다면 Kafka에 포함된 `kafka-streams-application-reset` 도구로 입력 위치와 내부 토픽을 초기화하는 방법이 있습니다.

### Q. Streams의 Exactly Once를 켜면 중복 이벤트도 한 번만 세나요?

아닙니다. `exactly_once_v2`는 장애와 재시작이 있어도 입력 토픽의 각 레코드를 상태와 출력에 정확히 한 번 반영한다는 보장입니다. 원본 토픽에 같은 eventId의 레코드가 두 건 들어 있다면 Streams에게는 서로 다른 두 레코드이므로 둘 다 셉니다. 업무 중복 제거는 topology 안에 직접 구현해야 합니다.

## 읽을 소스

- [발췌한 전체 소스](../고급/src/main/java/study/kafka/OrderStreams.java)

## 기록할 것

| 예상 결과 | 실제 결과 | 차이의 이유 |
|---|---|---|
| 직접 작성 | 직접 작성 | 직접 작성 |

## 완료 기준

- [ ] Consumer 처리와 Streams topology를 비교한다
- [ ] KStream과 집계 KTable을 구분한다
- [ ] 상태 저장소·changelog·application.id를 설명한다
- [ ] 윈도우 집계와 이벤트 시간의 관계를 설명한다

이해도 확인: 이벤트 count를 만든 KTable은 무엇을 나타내나요?

<details><summary>답 확인</summary>

key별 최신 누적 집계 상태: KTable은 key별 최신 상태를 나타냅니다. 같은 key의 새 레코드는 이전 값을 덮어쓰며, 입력 이벤트 로그와는 해석이 다릅니다.

</details>
