# 08. Replication · ISR · KRaft

3개 브로커로 Leader 교체와 쓰기 가용성의 조건을 관찰하고, 복제 수 · 최소 ISR · acks가 내구성을 어떻게 정하는지 이해합니다.

먼저 [대응 이론 HTML](../../이론/중급/08.html)을 읽으세요.

## 준비

- 저장소 루트의 PowerShell에서 실행합니다.
- 01단원의 up → build → init을 완료해야 합니다.
- 기존 기록은 유지됩니다. 새 실험은 고유 group / runId를 사용하세요.
- `max=0`은 무한 상주가 아니라 입력이 끊긴 뒤 25초까지 읽는 모드입니다.

## 학습 목표

- 데이터 복제와 Consumer 분담을 구분한다
- acks=all과 min.insync.replicas를 함께 설명한다
- 한 노드 중단과 두 노드 중단의 차이를 이해한다
- High Watermark와 ISR로 Consumer가 읽을 수 있는 범위를 설명한다

## 실습 전에 떠올릴 핵심 개념

- 복제는 파티션 단위이며, Leader가 읽기·쓰기를 맡고 Follower는 Leader의 로그를 따라 기록합니다.
- `acks=all`은 현재 ISR 전체의 확인이고, `min.insync.replicas`가 ISR의 최소 크기를 강제합니다.
- 복제 수 3 · `min.insync.replicas=2` · `acks=all`은 브로커 1대 장애에서도 쓰기와 내구성을 함께 지킵니다.
- Consumer는 High Watermark(ISR 전체가 기록한 위치)까지만 읽으므로, 복제되지 않은 데이터를 보지 않습니다.
- 이 실습의 combined 구성에서 두 노드가 멈추면 ISR 조건과 컨트롤러 과반을 함께 잃습니다.

## 실행과 관찰

### 1. 3노드 환경 시작

```powershell
.\실습\study.ps1 up -Cluster
```

예상 결과: kafka1~3과 postgres가 healthy가 됩니다. PC 자원이 충분해야 합니다.

**결과 해석**

```text
 Container kafka-field-guide-kafka3-1 Started
 Container kafka-field-guide-kafka2-1 Started
 Container kafka-field-guide-kafka1-1 Started
 Container kafka-field-guide-postgres-1 Healthy
 Container kafka-field-guide-kafka1-1 Healthy
 ...
```

세 브로커가 각자 broker와 controller를 겸하므로, 서로 과반이 모여 컨트롤러 리더를 뽑은 뒤에야 healthy가 됩니다. single 환경보다 시작이 조금 더 걸리는 것이 정상입니다. 메모리가 부족하면 컨테이너가 재시작을 반복할 수 있습니다. 실습 README는 3노드와 Streams를 함께 실행할 때 Docker 메모리 8GB 이상을 출발점으로 제시하니, Docker Desktop의 메모리 할당을 확인하세요.

cluster 환경은 single 환경과 다른 로그 볼륨(`cluster1-data` 등)을 씁니다. 초급에서 만든 토픽과 데이터는 이 클러스터에 없습니다.

### 2. 클러스터 토픽 생성

```powershell
.\실습\study.ps1 init -Cluster
```

예상 결과: 복제 수 3, ISR 3인 토픽을 확인합니다.

**결과 해석**

```text
orders [(partition=0, leader=kafka2:9092 (id: 2 rack: null isFenced: false),
         replicas=kafka2:9092 (id: 2 ...), kafka3:9092 (id: 3 ...), kafka1:9092 (id: 1 ...),
         isr=kafka2:9092 (id: 2 ...), kafka3:9092 (id: 3 ...), kafka1:9092 (id: 1 ...),
         elr=, lastKnownElr=), (partition=1, ...), (partition=2, ...)]
```

single 환경의 01단원 출력과 비교해 보세요. `replicas`와 `isr`에 브로커가 3개씩 나열됩니다. 파티션마다 `leader`가 서로 다른 브로커로 흩어져 있는지도 확인하세요. `rack: null`은 브로커에 랙 정보를 지정하지 않았다는 뜻입니다.

### 3. Leader 조회

```powershell
.\실습\study.ps1 topics -Cluster
```

예상 결과: orders 파티션별 Leader ID를 기록합니다.

**결과 해석**

```text
Topic: orders  TopicId: 4i58hWBNSViNeNGdm7wX1g  PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2,retention.ms=604800000
  Topic: orders  Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1  Elr:   LastKnownElr:
  Topic: orders  Partition: 1  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2  Elr:   LastKnownElr:
  Topic: orders  Partition: 2  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3  Elr:   LastKnownElr:
```

이번에는 브로커에 포함된 `kafka-topics.sh --describe`의 출력입니다. 위 예시에서는 브로커 1이 파티션 2의 Leader입니다. `Replicas`의 첫 번째 값이 선호 리더이므로, 정상 상태에서는 `Leader`와 `Replicas` 첫 값이 같습니다.

다음 단계에서 멈출 브로커를 이 출력으로 정합니다. Leader를 하나 이상 맡은 브로커를 골라야 리더 교체를 관찰할 수 있습니다.

### 4. 한 노드 중단

```powershell
.\실습\study.ps1 stop kafka1
```

예상 결과: 예시로 kafka1을 중단합니다. 실제 Leader가 다른 노드라면 그 노드를 선택하세요.

**결과 해석**

```text
 Container kafka-field-guide-kafka1-1 Stopping
 Container kafka-field-guide-kafka1-1 Stopped
```

몇 초 기다린 뒤 `topics -Cluster`를 다시 실행하면 다음처럼 바뀝니다.

```text
  Topic: orders  Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3  Elr:   LastKnownElr:
  Topic: orders  Partition: 1  Leader: 3  Replicas: 3,1,2  Isr: 3,2  Elr:   LastKnownElr:
  Topic: orders  Partition: 2  Leader: 2  Replicas: 1,2,3  Isr: 2,3  Elr:   LastKnownElr:
```

- 모든 파티션의 `Isr`에서 1이 빠졌습니다. `Replicas`는 그대로입니다. 복제본의 '자리'는 남아 있고 지금 동기화되지 않았을 뿐입니다.
- 파티션 2의 `Leader`가 1에서 2로 바뀌었습니다. 컨트롤러가 ISR에 남은 복제본 중에서 새 Leader를 고른 결과입니다.
- ISR이 2개로 `min.insync.replicas=2`를 여전히 만족하므로 쓰기가 가능해야 합니다.

### 5. 장애 중 전송

```powershell
.\실습\study.ps1 run -Cluster producer 6 orders failover-demo
```

예상 결과: 메타데이터 갱신 후 전송 성공 여부와 일시 재시도 로그를 관찰합니다.

**결과 해석**

```text
[main] WARN org.apache.kafka.clients.ClientUtils - Couldn't resolve server kafka1:9092 from bootstrap.servers as DNS resolution failed for kafka1
SENT event=failover-demo-0 key=order-0 partition=1 offset=0
SENT event=failover-demo-1 key=order-1 partition=1 offset=1
SENT event=failover-demo-2 key=order-2 partition=0 offset=0
...
```

`WARN ... Couldn't resolve server kafka1:9092`는 `bootstrap.servers`에 적힌 세 주소 중 kafka1 컨테이너가 멈춰 이름을 찾지 못했다는 경고입니다. 나머지 주소로 클러스터에 접속하므로 전송에는 문제가 없습니다. bootstrap 주소를 여러 개 적는 이유가 이것입니다.

`SENT` 6줄이 모두 나왔다면, `acks=all`과 `min.insync.replicas=2` 조건에서 ISR 2개로 쓰기가 계속된 것입니다. 리더 교체 직후라면 Producer가 내부적으로 재시도해 첫 줄이 조금 늦게 나올 수 있습니다. 여기서 kafka2까지 멈추고 같은 명령을 실행하면, 살아 있는 복제본이 하나뿐이고 컨트롤러 과반도 사라져 전송이 성공하지 못합니다. 이 실습 설정에서는 대략 15초(`delivery.timeout.ms`, `max.block.ms`) 뒤 실패로 끝납니다. 실험 뒤에는 반드시 두 노드를 모두 다시 시작하세요.

### 6. 복구

```powershell
.\실습\study.ps1 start kafka1
```

예상 결과: 잠시 후 topics -Cluster로 ISR이 회복되는지 확인합니다.

**결과 해석**

```text
 Container kafka-field-guide-kafka1-1 Starting
 Container kafka-field-guide-kafka1-1 Started
```

브로커 1은 멈춰 있던 동안 쌓인 로그(위 단계의 failover-demo 6건 등)를 각 파티션의 Leader에게서 가져와 따라잡은 뒤 ISR에 다시 들어갑니다. `topics -Cluster`에서 `Isr`가 다시 3개(예: `2,3,1`)가 되면 복구가 끝난 것입니다.

복구 직후 파티션 2의 `Leader`가 여전히 2일 수 있습니다. 선호 리더(Replicas의 첫 값인 1)로 되돌리는 자동 재조정은 기본 300초 간격의 점검에서 일어나므로, 바로 바뀌지 않아도 정상입니다.

## 더 실험하기

- single과 cluster는 서로 다른 로그 볼륨을 사용합니다. cluster의 그룹과 토픽은 별도로 시작합니다.
- 두 노드 중단은 controller 과반도 잃으므로 별도의 복구 실험으로 구분하세요. 반드시 중단한 노드를 모두 start로 복구합니다.
- 장애 중 전송한 failover-demo 6건을 `.\실습\study.ps1 run -Cluster consumer failover-reader 0 orders`로 읽어, 리더가 바뀐 파티션의 레코드도 빠짐없이 읽히는지 확인하세요.
- kafka1을 복구한 뒤 몇 분 간격으로 `topics -Cluster`를 실행해, 파티션 2의 Leader가 선호 리더로 돌아오는지 관찰해 보세요.

## 주의할 경계

초급 토픽이 들어 있는 single Kafka와 3노드 cluster는 별도 클러스터입니다. -Cluster를 빠뜨리면 다른 환경을 조회하거나 실행합니다. PostgreSQL은 공용이므로 DB 중복 표식은 환경 전환 후에도 남습니다. 복제 수 3은 사본 3벌이라는 뜻이지 Consumer가 같은 레코드를 세 번 처리한다는 뜻이 아닙니다.

## 로그 수집 파이프라인에 적용하면

**로그 토픽의 복제 설정과, 쓰기가 막혔을 때 수집 서버의 선택**

장비 로그는 장애 분석의 근거가 되므로, 수집된 로그가 브로커 한 대의 고장으로 사라지면 곤란합니다. 그래서 로그 토픽도 보통 **복제 수 3, `min.insync.replicas=2`, Producer `acks=all`**로 둡니다.

- **처리량 비용:** `acks=all`은 Follower의 기록까지 기다리므로 `acks=1`보다 지연이 조금 늘어납니다. 하지만 로그 수집은 배치와 압축으로 처리량을 확보하는 경우가 많아(04단원), 이 비용은 대개 감당할 만합니다. `acks=1`로 낮추면 Leader 장애 순간에 ack를 받은 로그가 사라질 수 있다는 점을 알고 선택해야 합니다.
- **한 대 장애:** 위 설정이면 브로커 한 대가 멈춰도 수집은 계속됩니다. 수집 서버는 잠깐의 재시도만 겪습니다.
- **두 대 장애:** ISR이 1개가 되어 쓰기가 거절되고, Producer는 `delivery.timeout.ms` 뒤 실패를 보고합니다. 이때 수집 서버가 할 수 있는 일은 둘 중 하나입니다. 로컬 디스크 버퍼에 잠시 쌓아 두고 Kafka가 돌아오면 다시 보내거나, 수집 에이전트에게 실패 응답(예: HTTP 503)을 돌려줘 에이전트가 자기 버퍼에 보관하고 재시도하게 하는 것입니다.
- **가장 위험한 선택:** Kafka ack를 받기 전에 에이전트에게 성공(2xx)을 돌려주는 것입니다. 에이전트는 로그를 지우고, 수집 서버는 Kafka에 쓰지 못해 로그가 어디에도 남지 않습니다. 수집 서버의 성공 응답은 Kafka ack 이후에 보냅니다.

브로커 배치도 중요합니다. 브로커 3대를 같은 서버나 같은 랙에 두면 복제 수 3의 의미가 크게 줄어듭니다.

## 자주 묻는 질문

### Q. 복제 수가 3이면 Consumer가 같은 메시지를 3번 받나요?

아닙니다. 복제는 장애에 대비한 사본일 뿐이고, Consumer는 기본적으로 각 파티션의 Leader 한 곳에서만 읽습니다. 같은 그룹 안에서 한 파티션은 한 Consumer만 맡으므로(03단원), 복제 수와 관계없이 그룹은 각 레코드를 한 번 받습니다.

### Q. 브로커 두 대가 멈추면 데이터가 사라지나요?

데이터가 지워지는 것은 아닙니다. 멈춘 브로커의 디스크에는 로그가 그대로 있고, 남은 한 대에도 사본이 있습니다. 다만 살아 있는 복제본이 최소 ISR보다 적어 `acks=all` 쓰기가 성공할 수 없고, 이 실습 구성에서는 컨트롤러 과반도 잃어 클러스터가 정상적으로 동작하지 않습니다. 브로커가 돌아오면 다시 동기화됩니다. `unclean.leader.election.enable`이 기본값 `false`이므로, Kafka는 데이터가 덜 복제된 복제본을 억지로 Leader로 세우지 않습니다.

### Q. `acks=all`인데 ISR이 Leader 하나로 줄면 어떻게 되나요?

`min.insync.replicas`가 1이면 Leader 혼자 기록해도 ack가 나가므로, 그 순간부터는 사실상 복제 없이 쓰는 셈입니다. 그래서 복제 수 3에서는 `min.insync.replicas=2`를 함께 설정해, ISR이 하나로 줄면 차라리 쓰기를 거절하게 합니다. 토픽 설정의 Kafka 기본값은 1이므로 명시적으로 지정해야 합니다.

### Q. 장애 중 전송에서 kafka1 DNS 경고가 나오는데 문제인가요?

문제가 아닙니다. Producer 설정의 `bootstrap.servers`에 세 브로커 주소가 모두 들어 있고, 멈춘 kafka1 컨테이너의 이름을 찾지 못했다는 경고일 뿐입니다. 클라이언트는 나머지 주소로 접속해 전체 클러스터의 메타데이터를 받아 옵니다. 운영에서도 bootstrap 주소를 여러 개 적어 두는 이유입니다.

## 읽을 소스

- [발췌한 전체 소스](../초급/src/main/java/study/kafka/TopicAdmin.java)

## 기록할 것

| 예상 결과 | 실제 결과 | 차이의 이유 |
|---|---|---|
| 직접 작성 | 직접 작성 | 직접 작성 |

## 완료 기준

- [ ] 데이터 복제와 Consumer 분담을 구분한다
- [ ] acks=all과 min.insync.replicas를 함께 설명한다
- [ ] 한 노드 중단과 두 노드 중단의 차이를 이해한다
- [ ] High Watermark와 ISR로 Consumer가 읽을 수 있는 범위를 설명한다

이해도 확인: acks=all에서 all이 가리키는 대상은?

<details><summary>답 확인</summary>

현재 ISR에 속한 복제본의 확인 조건: 현재 ISR 전체의 기록 확인을 뜻합니다. ISR이 줄어들 수 있으므로 min.insync.replicas와 함께 해석해야 합니다.

</details>
