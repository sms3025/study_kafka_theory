# Kafka 실습 실행 안내

먼저 [이론 로드맵](../이론/index.html)을 열고 같은 번호의 실습으로 진행하세요.

## 준비와 시작

- Windows PowerShell, 실행 중인 Docker Desktop(Linux containers), 인터넷 연결
- 단일 환경 권장 Docker 메모리 4GB 이상, 3노드+Streams 동시 실행은 8GB 이상을 출발점으로 조정합니다. 이는 용량 보장이 아닌 학습 환경 권장값입니다.
- Kafka와 PostgreSQL 포트는 `127.0.0.1`에만 공개합니다. 실습 계정/평문 연결을 외부에 공개하지 않습니다.
- 예제 소스는 Java 21이며 Maven과 JRE를 Docker 이미지로 제공합니다.

**아래 명령은 저장소 루트에서 실행합니다.**

```powershell
.\실습\study.ps1 up
.\실습\study.ps1 build
.\실습\study.ps1 init
.\실습\study.ps1 run help
```

`up`은 정상 상태를 기다립니다. `init`은 기존 토픽을 지우지 않고 필요한 토픽만 생성합니다.
소스를 수정한 경우 `build`를 다시 실행해야 JAR에 반영됩니다.
실행 중인 예제가 JAR를 읽고 있을 때 같은 파일을 덮어쓰지 않도록, 실행을 종료한 뒤 빌드하고 다시 실행합니다.

| 접속 대상 | 호스트(IDE에서 Java 실행) | Docker 내부 cli |
|---|---|---|
| 단일 Kafka | localhost:19092 | kafka:9092 |
| 3노드 Kafka | localhost:29092,39092,49092 각각 host:port로 지정 | kafka1:9092,kafka2:9092,kafka3:9092 |
| PostgreSQL | localhost:15432 / kafka_study | postgres:5432 / kafka_study |

DB 사용자: `study`, 로컬 학습용 비밀번호: `study-local-only`.

## 단계별 안내

- [초급 01~04](초급/README.md)
- [중급 05~08](중급/README.md)
- [고급 09~12](고급/README.md)
- [최종 프로젝트](최종-프로젝트.md)

## 명령 인자

인자는 순서대로 전달합니다. 대괄호는 생략 가능하다는 뜻이며 실제로 입력하지 않습니다.

```text
producer [count=12] [topic=orders] [runId=random]
producer-async [count=100] [topic=orders] [runId=random]
consumer [group=beginner] [max=12] [topic=orders] [delayMs=0] [isolation=read_uncommitted]
commit [after|before|crash-after|crash-before] [group=commit-demo] [max=12]
dedup [group=dedup-demo] [max=12] [normal|crash-after-db] [topic=orders]
poison
retry [group=retry-demo] [max=3]
tx-produce [commit|abort] [runId=random]
tx-process [group=tx-worker] [max=12]
outbox-create [count=3] [runId=random] [normal|rollback]
outbox-relay [normal|crash-after-send] [max=10]
streams [seconds=60] [applicationId=order-counts-v1]
inspect [group=beginner]
schema
verify
```

예:

```powershell
.\실습\study.ps1 run producer 12 orders unique-run-001
.\실습\study.ps1 run consumer notify-001 0 orders 500 read_committed
```

- Consumer 계열의 `max=0`은 모든 입력을 읽되 **입력이 끊긴 후 25초**가 지나면 끝납니다. 장기 상주 서비스가 아닙니다.
- 아직 파티션을 배정받지 못한 그룹 참여/리밸런싱은 최대 90초까지 기다립니다. 장애 실험의 재시작이 장애 판정보다 먼저 끝나지 않도록 구분한 제한입니다.
- 장애 실험을 빠르게 관찰하기 위해 classic 그룹 프로토콜, session timeout 10초, heartbeat 3초를 명시했습니다. 운영 값은 처리 특성·네트워크·프로토콜에 맞게 결정합니다.
- 이전 단원의 기록도 보관됩니다. 새 group은 과거 기록부터 읽습니다. 고정 max보다 앞선 데이터가 많다면 `0`으로 읽고 runId를 찾으세요.
- `runId`를 같게 발행하면 같은 eventId를 다시 만듭니다. 중복 실험에 사용합니다. 새 업무 이벤트는 새 runId를 사용합니다.
- commit 값은 다음 읽을 offset입니다. tx-process는 poll 배치 전체를 처리하므로 max를 약간 초과할 수 있습니다.
- `crash-*`는 의도적으로 프로세스를 강제 종료합니다. **exit 17, Docker/PowerShell 오류가 예상 결과**입니다. 다음 정상 명령으로 재시작하세요.
- `rollback`은 의도적으로 예외를 던져 DB 변경을 모두 취소합니다.
- DLT 입력은 `orders.retry`에 격리되어 있습니다. 시간 지연을 제공하는 retry-topic 시스템을 구현한 것은 아닙니다.

## 여러 터미널과 종료

Consumer A/B/Producer를 나눠 실행하면 그룹 분담을 관찰할 수 있습니다. Consumer 명령은 한동안 입력이 없으면 종료하므로 Producer를 바로 실행하세요.
종료는 `Ctrl+C` 또는 정해진 실행 시간이 끝날 때까지 기다립니다. 강제 종료 시 미커밋 레코드는 다시 읽힐 수 있습니다.

```powershell
.\실습\study.ps1 status
.\실습\study.ps1 sql
.\실습\study.ps1 groups --describe --group notify-001
.\실습\study.ps1 topics
.\실습\study.ps1 down
```

`down`은 Kafka/DB/Streams 데이터를 지우지 않습니다. 정상 재시작은 `up`으로 합니다. 완전 초기화가 필요하면 Docker Desktop에서 **kafka-field-guide 프로젝트에 속한 볼륨만** 정확히 식별해 삭제합니다. 학습 데이터가 사라지므로 일반 학습 과정에서는 새 그룹·runId로 분리하는 편이 안전합니다.

## 3노드 실습

```powershell
.\실습\study.ps1 up -Cluster
.\실습\study.ps1 init -Cluster
.\실습\study.ps1 run -Cluster producer 6 orders cluster-001
.\실습\study.ps1 topics -Cluster
.\실습\study.ps1 stop kafka1
.\실습\study.ps1 run -Cluster producer 6 orders failover-001
.\실습\study.ps1 start kafka1
.\실습\study.ps1 topics -Cluster
```

- 단일과 3노드는 별도 Kafka 클러스터이며 로그 볼륨이 다릅니다. `-Cluster`가 클라이언트 주소와 복제 수를 선택합니다.
- 두 환경의 PostgreSQL은 공용입니다. DB 중복 표식도 공유되므로 runId를 구분하세요.
- Streams의 application.id와 로컬 상태는 환경별로 다르게 지정합니다. 예: `counts-single-v1`, `counts-cluster-v1`.
- 3노드는 broker/controller combined 학습 구성입니다. 두 노드를 중단하면 controller quorum 과반도 사라집니다.
- 한 노드 장애 시 잠시 재시도할 수 있습니다. 복구 뒤 ISR=3 회복을 기다립니다.

## IDE에서 읽고 실행하기

`실습/pom.xml`을 Maven 프로젝트로 엽니다. build-helper 플러그인이 공통·초급·중급·고급·검증의 소스 디렉터리를 등록합니다.
호스트에서 실행하려면 JDK 21과 Maven이 별도로 필요합니다.

```powershell
Set-Location .\실습
mvn -B package
$env:BOOTSTRAP_SERVERS = 'localhost:19092'
$env:JDBC_URL = 'jdbc:postgresql://localhost:15432/kafka_study'
java -jar .\target\kafka-labs.jar producer 3 orders ide-run
```

동시 트랜잭션 인스턴스는 서로 다른 `TX_ID`가 필요합니다. Docker cli에 환경을 전달하는 원형:

```powershell
docker compose -f .\실습\compose.yaml -p kafka-field-guide --profile tools run --rm --no-deps -e TX_ID=processor-2 cli tx-process worker-group 12
```

같은 논리적 인스턴스의 재시작에는 안정된 ID를 사용합니다. 예제의 기본 ID로 동일 프로그램을 동시에 두 개 실행하면 fencing이 발생할 수 있습니다.

## 문제 해결

| 증상 | 확인할 점 |
|---|---|
| Docker 연결 실패 | Docker Desktop과 Linux containers 모드 확인 |
| 포트 충돌 | 19092/29092/39092/49092/15432 사용 여부 확인, compose의 호스트 포트를 함께 조정 |
| JAR가 없음 | `build` 실행 여부, 빌드 오류 출력 확인 |
| 전송 시간 초과 | `up` 상태, `init`, `-Cluster` 선택, 광고된 주소가 실행 위치에서 도달 가능한지 확인 |
| Consumer가 출력 없이 종료 | 기존 그룹은 이미 커밋했을 수 있음. 새 그룹 또는 새 입력 사용 |
| 일부 eventId만 보임 | max 제한과 이전 단원 누적 기록 확인. max=0으로 읽기 |
| DB 테이블 없음 | 오래된 PG 볼륨은 init 스크립트를 재실행하지 않음. 아래 명령으로 스키마를 멱등 적용 |
| scripts disabled | README의 해당 실행에만 적용하는 ExecutionPolicy 명령 참고 |

기존 DB 볼륨에 스키마를 다시 적용할 때:

```powershell
Get-Content -Raw -Encoding UTF8 .\실습\공통\schema.sql | docker compose -f .\실습\compose.yaml -p kafka-field-guide exec -T postgres psql -U study -d kafka_study
```
