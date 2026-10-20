"""부록 A1~A3: CLI와 운영 도구, Kafka 생태계, 용어집의 원본입니다."""

CLI = dict(
    slug='cli',
    no='A1',
    title='CLI와 운영 도구로 들여다보기',
    short='CLI · Kafka UI',
    desc='실습 프로그램의 출력만으로 부족할 때, Kafka에 포함된 명령줄 도구와 웹 UI로 토픽·그룹·설정을 직접 확인하는 방법을 정리합니다.',
    intro=r"""
단원 실습의 Java 프로그램은 관찰하기 좋게 필요한 정보만 출력합니다. 하지만 실제로 문제를 조사할 때는 '지금 이 그룹은 어디까지 읽었나', '이 파티션의 리더는 누구인가'처럼 프로그램이 알려 주지 않는 정보가 필요합니다. 그럴 때 쓰는 것이 Kafka 배포판에 함께 들어 있는 명령줄 도구(CLI)입니다.

이 저장소의 Kafka 이미지(`apache/kafka:4.3.1`)에는 도구가 `/opt/kafka/bin` 아래에 들어 있습니다. 호스트에 Kafka를 설치하지 않고 **컨테이너 안에서 실행**하면 됩니다. 아래 명령은 모두 저장소 루트의 PowerShell에서 실행한다고 가정합니다.

명령마다 데이터를 바꾸는지 여부를 함께 적었습니다. 토픽 생성, 파티션 추가, 설정 변경, 오프셋 재설정은 학습 데이터에 영향을 주므로 먼저 확인용 옵션(`--dry-run`)이나 별도 연습 토픽으로 시험해 보세요.
""",
    sections=[
        dict(
            id='run-cli',
            kicker='A1-1 / HOW TO RUN',
            title='컨테이너 안에서 CLI를 실행하는 세 가지 방법',
            toc='실행 방법',
            body=r"""
가장 간단한 방법은 `study.ps1`에 이미 들어 있는 두 명령입니다. `topics`는 모든 토픽의 파티션 상태를 보여 주고, `groups`는 뒤에 붙인 인자를 `kafka-consumer-groups.sh`에 그대로 넘깁니다. 접속 주소(`--bootstrap-server`)는 스크립트가 알아서 채웁니다.

```powershell
.\실습\study.ps1 topics                       # 단일 환경의 토픽 상세
.\실습\study.ps1 topics -Cluster              # 3노드 환경의 토픽 상세
.\실습\study.ps1 groups --list                # 그룹 목록
.\실습\study.ps1 groups --describe --group day01-reader
```

그 밖의 도구는 `docker compose exec`로 브로커 컨테이너 안에서 직접 실행합니다. 단일 환경의 서비스 이름은 `kafka`, 접속 주소는 `kafka:9092`입니다.

```powershell
docker compose -f .\실습\compose.yaml -p kafka-field-guide exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

3노드 환경에서는 서비스 이름을 `kafka1`로, 접속 주소를 `kafka1:9092,kafka2:9092,kafka3:9092`로 바꿉니다. 컨테이너 안에서는 Docker 네트워크의 서비스 이름(`kafka:9092`, 3노드는 `kafka1:9092` 등)을 쓰고, 호스트의 IDE에서 실행하는 프로그램만 `localhost:19092`(3노드는 `localhost:29092` 등)를 씁니다. 두 주소를 섞으면 연결 오류가 납니다.

명령이 길어 번거롭다면 PowerShell 세션 안에서만 쓰는 짧은 함수를 만들어 두세요. 창을 닫으면 사라지므로 시스템 설정은 바뀌지 않습니다.

```powershell
function kcli { docker compose -f .\실습\compose.yaml -p kafka-field-guide exec -T kafka @args }
kcli /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic orders
```

Kafka 4.2에서는 CLI 인자 표기를 통일하는 작업(KIP-1147)으로 일부 옵션 이름이 바뀌었습니다. 예를 들어 콘솔 Consumer의 `--property`는 `--formatter-property`로, 콘솔 도구의 `--consumer-property`·`--producer-property`는 `--command-property`로 바뀌었습니다. 옛 이름도 사용 중단(deprecated) 경고를 낸 뒤 그대로 동작하지만, 옛 이름과 새 이름을 한 명령에 섞으면 오류가 납니다. 이 부록은 4.2 이후의 이름을 쓰며, 다른 옵션이 낯설면 해당 도구에 `--help`를 붙여 현재 버전의 옵션 이름을 확인하세요.
""",
            tables=[dict(
                title='알고 싶은 것과 명령 찾기',
                intro="도구 이름은 모두 `/opt/kafka/bin/` 아래에 있습니다. **데이터 변경** 열이 '있음'인 명령은 실행 전에 대상을 다시 확인하세요.",
                columns=['알고 싶은 것', '명령의 요지', '데이터 변경'],
                rows=[
                    ['토픽 목록', '`kafka-topics.sh --list`', '없음'],
                    ['파티션별 리더와 ISR', '`.\\실습\\study.ps1 topics` 또는 `kafka-topics.sh --describe --topic orders`', '없음'],
                    ['복제가 부족한 파티션만 보기', '`kafka-topics.sh --describe --under-replicated-partitions`', '없음'],
                    ['토픽 설정값', '`kafka-configs.sh --describe --entity-type topics --entity-name orders`', '없음'],
                    ['저장된 메시지 내용', '`kafka-console-consumer.sh --topic orders --from-beginning --max-messages 5`', '없음 (임시 그룹이 생깁니다)'],
                    ['그룹의 진행 위치와 Lag', '`.\\실습\\study.ps1 groups --describe --group <그룹>`', '없음'],
                    ['그룹 멤버와 상태', '`groups --describe --group <그룹> --members` / `--state`', '없음'],
                    ['메시지를 직접 넣기', '`kafka-console-producer.sh --topic <토픽>`', '있음 (레코드 추가)'],
                    ['토픽 만들기, 파티션 늘리기', '`kafka-topics.sh --create` / `--alter --partitions`', '있음'],
                    ['보관 기간 바꾸기', '`kafka-configs.sh --alter --add-config retention.ms=...`', '있음'],
                    ['그룹의 읽기 위치 되돌리기', '`groups --reset-offsets ... --execute`', '있음'],
                ],
                note='표의 `groups`는 `.\\실습\\study.ps1 groups`의 줄임입니다.',
            )],
        ),
        dict(
            id='topics',
            kicker='A1-2 / TOPICS',
            title='kafka-topics: 토픽과 파티션 상태 읽기',
            toc='kafka-topics',
            body=r"""
`--describe`는 토픽의 파티션마다 리더, 복제본, ISR을 보여 줍니다. 아래는 이 저장소의 3노드 환경에서 실제로 기록한 출력입니다. 첫 번째는 정상 상태, 두 번째는 `kafka1`을 멈춘 직후입니다.

```text
Topic: orders  PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2,retention.ms=604800000
    Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1  Elr:   LastKnownElr:
    Partition: 1  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2  Elr:   LastKnownElr:
    Partition: 2  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3  Elr:   LastKnownElr:
```

```text
    Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3    Elr:   LastKnownElr:
    Partition: 1  Leader: 3  Replicas: 3,1,2  Isr: 3,2    Elr:   LastKnownElr:
    Partition: 2  Leader: 2  Replicas: 1,2,3  Isr: 2,3    Elr:   LastKnownElr:
```

각 열의 뜻은 다음과 같습니다(실제 출력은 탭으로 구분되며 `TopicId` 열도 함께 나옵니다).

- **PartitionCount / ReplicationFactor:** 파티션 수와 파티션마다 둔 복제본 수입니다.
- **Configs:** 토픽에 명시적으로 지정한 설정입니다. 기본값을 그대로 쓰는 설정은 보이지 않습니다.
- **Leader:** 그 파티션의 읽기와 쓰기를 맡은 브로커 ID입니다.
- **Replicas:** 복제본을 갖도록 배정된 브로커 목록입니다. 브로커가 멈춰도 배정 자체는 바뀌지 않습니다.
- **Isr:** 배정된 복제본 중 지금 리더와 동기화된 목록입니다. `acks=all`과 `min.insync.replicas`가 이 목록을 기준으로 동작합니다.
- **Elr / LastKnownElr:** Kafka 4.x의 Eligible Leader Replicas 정보입니다. ISR 밖이지만 안전하게 리더가 될 수 있는 복제본을 기록하며, 정상 상태에서는 비어 있습니다.

두 출력을 비교하면 08단원의 내용이 그대로 보입니다. `kafka1`(ID 1)이 멈추자 모든 파티션의 Isr에서 1이 빠졌고, 1이 리더였던 파티션 2는 리더가 2로 바뀌었습니다. Isr이 2개 남아 `min.insync.replicas=2`를 만족하므로 쓰기는 계속됩니다.

장애를 빠르게 찾을 때는 문제가 있는 파티션만 골라 보는 옵션이 편합니다.

```powershell
kcli /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --under-replicated-partitions
kcli /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --unavailable-partitions
```

아무것도 출력되지 않으면 해당하는 파티션이 없다는 뜻입니다.
""",
            after=r"""
**토픽 만들기와 파티션 늘리기 (데이터 변경)**

연습용 토픽은 학습용 토픽과 분리해 만드는 것이 안전합니다. 이 환경은 자동 생성이 꺼져 있으므로 명시적으로 만들어야 합니다.

```powershell
kcli /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic cli.practice --partitions 3 --replication-factor 1 --config retention.ms=86400000
```

`--alter --partitions 6`으로 파티션을 늘릴 수는 있지만 줄일 수는 없습니다. 늘리면 기본 파티셔너의 key 배치가 바뀌어, 같은 key의 새 레코드가 예전과 다른 파티션으로 갈 수 있습니다. 기존 레코드는 원래 파티션에 남으므로 key별 순서가 섞일 수 있다는 점을 먼저 따져 보세요(02단원).
""",
        ),
        dict(
            id='console',
            kicker='A1-3 / MESSAGES',
            title='콘솔 Consumer·Producer로 메시지 보고 넣기',
            toc='콘솔 도구',
            body=r"""
`kafka-console-consumer.sh`는 토픽의 레코드를 화면에 출력합니다. `--formatter-property` 옵션으로 key, 파티션, offset, timestamp를 함께 보이게 할 수 있습니다. Kafka 4.2 이전 자료에서 보이는 `--property`는 이 옵션의 옛 이름이며, 지금도 사용 중단 경고와 함께 동작합니다.

```powershell
kcli /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic orders --from-beginning --max-messages 5 --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true --formatter-property print.timestamp=true
```

- **--from-beginning:** 처음 쓰는 그룹이 각 파티션의 가장 오래된 레코드부터 읽게 합니다. 빼면 Kafka 기본값(`auto.offset.reset=latest`)에 따라 실행 이후 새로 들어온 레코드만 보입니다.
- **--max-messages:** 지정한 건수를 읽으면 끝냅니다. 빼면 Ctrl+C로 멈출 때까지 계속 기다립니다.
- **--isolation-level read_committed:** 트랜잭션으로 커밋된 레코드만 보여 줍니다. 09단원의 abort 실험 결과를 CLI로 확인할 때 씁니다.
- **--group:** 지정하지 않으면 도구가 임시 그룹을 만들어 읽습니다. 지정하면 그 그룹의 구성원으로 참여하고 진행 위치도 커밋합니다.

마지막 옵션은 주의가 필요합니다. 실습에서 쓰는 그룹 이름(예: `dedup-demo`)을 `--group`에 넣으면 그 그룹의 커밋 위치가 움직여 이후 실습 결과가 달라집니다. 확인용으로 읽을 때는 `--group`을 빼거나 `cli-` 같은 접두사를 붙인 별도 이름을 쓰세요.

`kafka-console-producer.sh`는 한 줄을 레코드 한 건으로 보냅니다. `parse.key=true`와 `key.separator`를 주면 `key:value` 형식으로 key를 함께 넣을 수 있습니다. 입력 해석에 쓰는 Producer 쪽 `--property`는 KIP-1147에서 이름이 바뀌지 않았습니다. 직접 입력해야 하므로 `-T` 없이 실행해 터미널을 연결합니다.

```powershell
docker compose -f .\실습\compose.yaml -p kafka-field-guide exec kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic cli.practice --property parse.key=true --property key.separator=:
```

```text
>order-1:hello
>order-1:second
```

입력을 마치면 Ctrl+C로 종료합니다. **`orders` 토픽에는 직접 넣지 마세요.** 이 과정의 Consumer와 Streams는 `orders`의 값을 주문 JSON으로 해석하므로, 형식이 다른 레코드가 섞이면 `dedup`이나 `streams` 실습이 파싱 오류로 멈춥니다. 연습은 위에서 만든 `cli.practice`처럼 별도 토픽에서 하세요.
""",
        ),
        dict(
            id='groups',
            kicker='A1-4 / GROUPS',
            title='kafka-consumer-groups: 그룹의 위치와 Lag 보기, 되돌리기',
            toc='consumer-groups',
            body=r"""
05단원과 12단원에서 다루는 '커밋된 위치'와 'Lag'를 가장 직접적으로 보여 주는 도구입니다. 이 저장소에서는 `study.ps1 groups`로 실행합니다.

```powershell
.\실습\study.ps1 groups --list
.\실습\study.ps1 groups --describe --group day01-reader
.\실습\study.ps1 groups --describe --group day01-reader --members
.\실습\study.ps1 groups --describe --group day01-reader --state
```

`--describe`는 파티션마다 한 줄씩 진행 상황을 보여 줍니다. 버전에 따라 열 구성이 조금 다를 수 있지만 핵심 열은 아래 표와 같습니다. 그룹에 실행 중인 Consumer가 없으면 `Consumer group '...' has no active members.` 같은 안내와 함께 커밋 위치만 보여 줍니다. `--members`는 그룹에 참여한 Consumer와 배정받은 파티션 수를, `--state`는 그룹 상태와 코디네이터 브로커를 보여 줍니다.
""",
            tables=[dict(
                title='`--describe` 출력의 열',
                columns=['열', '뜻', '읽는 법'],
                rows=[
                    ['GROUP / TOPIC / PARTITION', '어느 그룹이 어느 토픽의 몇 번 파티션을 읽는지', '그룹 하나에 토픽 파티션 수만큼 줄이 나옵니다.'],
                    ['CURRENT-OFFSET', '그룹이 커밋한 위치, 즉 다음에 읽을 offset', '처리한 마지막 offset이 12였다면 13입니다(05단원).'],
                    ['LOG-END-OFFSET', '파티션에 다음 레코드가 기록될 위치', '파티션 끝을 가리킵니다.'],
                    ['LAG', 'LOG-END-OFFSET과 CURRENT-OFFSET의 차이', '아직 커밋하지 않은 진행량입니다. 시간 지연과 같은 지표가 아닙니다(12단원).'],
                    ['CONSUMER-ID / HOST / CLIENT-ID', '그 파티션을 배정받은 Consumer', '실행 중인 멤버가 없으면 `-`로 표시됩니다.'],
                ],
                note='CURRENT-OFFSET이 `-`라면 그 파티션에 대한 커밋 기록이 없다는 뜻입니다. Lag이 0이라는 증거가 아닙니다.',
            )],
            after=r"""
**읽기 위치 되돌리기 (데이터 변경)**

버그를 고친 뒤 같은 이벤트를 다시 처리하거나, 실험을 처음부터 다시 하고 싶을 때 그룹의 커밋 위치를 옮깁니다. 반드시 두 단계로 진행합니다.

1. 그룹의 Consumer를 모두 종료합니다. 실행 중인 멤버가 있으면 재설정이 거부됩니다. 강제 종료했다면 세션 타임아웃(이 실습은 10초)이 지나 그룹에서 빠질 때까지 기다립니다.
2. `--dry-run`으로 바뀔 위치를 확인한 뒤, 같은 명령의 `--dry-run`을 `--execute`로 바꿔 실행합니다.

```powershell
.\실습\study.ps1 groups --group day01-reader --topic orders --reset-offsets --to-earliest --dry-run
.\실습\study.ps1 groups --group day01-reader --topic orders --reset-offsets --to-earliest --execute
```

위치를 고르는 옵션은 여러 가지입니다.

- **--to-earliest / --to-latest:** 파티션의 처음 또는 끝으로 옮깁니다. 끝으로 옮기면 밀린 레코드를 건너뜁니다.
- **--to-datetime 2026-10-20T00:00:00.000:** 지정한 시각 이후 첫 레코드로 옮깁니다. 시각은 도구가 실행되는 컨테이너의 시간대 기준이므로, 컨테이너가 UTC라면 한국 시각에서 9시간을 빼서 넣습니다.
- **--shift-by -5:** 현재 커밋 위치에서 앞뒤로 지정한 수만큼 옮깁니다.
- **--to-offset 100:** 지정한 offset으로 옮깁니다.

되돌리면 이미 처리한 이벤트가 다시 처리됩니다. 06단원의 멱등 처리가 없는 Consumer라면 합계가 두 번 더해지는 식의 부작용이 생기므로, 재처리 전에 Consumer가 중복을 견디는지 먼저 확인하세요.
""",
        ),
        dict(
            id='configs',
            kicker='A1-5 / CONFIGS',
            title='kafka-configs: 토픽 설정 확인과 변경',
            toc='kafka-configs',
            body=r"""
토픽에 어떤 설정이 적용되어 있는지 확인하고, 보관 기간 같은 값을 바꿀 때 씁니다.

```powershell
kcli /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 --entity-type topics --entity-name orders --describe
kcli /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 --entity-type topics --entity-name orders --describe --all
```

첫 명령은 토픽에 명시적으로 지정한 값만 보여 줍니다. 이 저장소의 `orders`라면 `min.insync.replicas`와 `retention.ms`가 보입니다. `--all`을 붙이면 기본값을 포함한 전체 설정이 나옵니다.

값을 바꾸는 명령은 데이터 보관에 직접 영향을 줍니다. 예를 들어 `retention.ms`를 줄이면 그 기간보다 오래된 세그먼트가 정리 대상이 되어 재처리할 수 있는 범위가 줄어듭니다.

```powershell
kcli /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 --entity-type topics --entity-name cli.practice --alter --add-config retention.ms=3600000
kcli /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 --entity-type topics --entity-name cli.practice --alter --delete-config retention.ms
```

`--delete-config`는 토픽의 개별 지정을 지워 브로커 기본값으로 되돌립니다.
""",
        ),
        dict(
            id='kafka-ui',
            kicker='A1-6 / WEB UI',
            title='Kafka UI로 한눈에 보기',
            toc='Kafka UI',
            body=r"""
CLI가 익숙해지기 전에는 웹 화면이 전체 그림을 잡는 데 도움이 됩니다. 오픈소스인 **Kafbat UI**(`ghcr.io/kafbat/kafka-ui`, 2026년 4월에 나온 v1.5.0)를 실습용 Docker 네트워크에 붙여 실행할 수 있습니다.

```powershell
docker run --rm -p 127.0.0.1:8080:8080 --network kafka-field-guide_default -e KAFKA_CLUSTERS_0_NAME=single -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:9092 ghcr.io/kafbat/kafka-ui:v1.5.0
```

실행 후 브라우저에서 `http://localhost:8080`을 엽니다. 명령의 각 부분은 이런 뜻입니다.

- **--network kafka-field-guide_default:** Compose는 프로젝트 이름(`-p kafka-field-guide`) 뒤에 `_default`를 붙인 네트워크를 만듭니다. UI 컨테이너가 이 네트워크에 들어가야 `kafka:9092`라는 이름으로 브로커에 닿습니다. 이름이 다르면 `docker network ls`로 확인하세요.
- **-p 127.0.0.1:8080:8080:** UI를 내 PC에서만 열리게 합니다. 이 실습 환경은 인증이 없으므로 다른 기기에 공개하지 않습니다.
- **KAFKA_CLUSTERS_0_*:** 첫 번째(0번) 클러스터의 표시 이름과 접속 주소입니다. 3노드 환경이라면 `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka1:9092,kafka2:9092,kafka3:9092`로 바꿉니다.
- **--rm:** Ctrl+C로 멈추면 UI 컨테이너를 지웁니다. Kafka 데이터에는 영향이 없습니다.

화면에서는 다음을 중심으로 보세요.

- **Topics:** 파티션 수, 복제 상태, 설정을 확인합니다. 08단원 장애 실험 중에 ISR 변화를 지켜보기 좋습니다.
- **Messages:** 토픽의 레코드를 파티션·offset별로 훑어봅니다. JSON 값도 읽기 좋게 보여 줍니다.
- **Consumers:** 그룹별, 파티션별 Lag을 보여 줍니다. 12단원의 느린 Consumer 실험과 함께 열어 두면 Lag이 쌓이고 줄어드는 모습이 보입니다.
- **Brokers:** 브로커 목록과 파티션 배치, 컨트롤러 상태를 확인합니다.

UI에서도 토픽 삭제, 메시지 전송, 오프셋 재설정 같은 변경 작업을 할 수 있습니다. 학습 데이터를 지키려면 조회 위주로 쓰고, 변경 작업은 연습 토픽에서만 시도하세요.
""",
        ),
        dict(
            id='troubleshoot',
            kicker='A1-7 / TROUBLESHOOTING',
            title='자주 만나는 오류와 확인 순서',
            toc='문제 해결',
            body=r"""
CLI를 쓰다 보면 비슷한 오류를 반복해서 만납니다. 원인은 대부분 접속 주소, 그룹 상태, 토픽 존재 여부 세 가지 중 하나입니다.
""",
            tables=[dict(
                title='증상별 확인할 점',
                columns=['증상', '흔한 원인', '확인할 것'],
                rows=[
                    ['`UNKNOWN_TOPIC_OR_PARTITION`', '토픽이 없습니다. 이 환경은 자동 생성이 꺼져 있습니다.', '`init`을 실행했는지, 토픽 이름 철자와 `-Cluster` 선택이 맞는지 확인합니다.'],
                    ['호스트 프로그램이 `kafka:9092`에 연결하지 못함', '`kafka`는 Docker 네트워크 안에서만 통하는 이름입니다.', '호스트에서는 `localhost:19092`(3노드는 29092·39092·49092)를 씁니다.'],
                    ['오프셋 재설정이 거부됨', '그룹에 실행 중인 멤버가 남아 있습니다.', '`--describe --members`로 멤버를 확인하고, 모두 종료한 뒤 다시 시도합니다.'],
                    ['콘솔 Consumer가 아무것도 출력하지 않음', '새 그룹의 기본 시작 위치가 끝(latest)입니다.', '`--from-beginning`을 붙이거나, 실행 중에 새 레코드를 보냅니다.'],
                    ["3노드에서 `Couldn't resolve server kafka1:9092` 경고", '멈춘 노드의 이름을 찾지 못한 것입니다.', '중단 실험 중이라면 정상입니다. 클라이언트는 나머지 주소로 접속합니다.'],
                    ['`Docker command failed (exit 17)`', '`crash-*` 실험의 의도된 강제 종료입니다.', '다음 정상 명령으로 재시작합니다.'],
                    ['스크립트 실행이 차단됨', 'PowerShell 실행 정책입니다.', '저장소 README의 해당 실행에만 적용하는 ExecutionPolicy 명령을 씁니다.'],
                ],
            )],
        ),
    ],
    sources=['tconfig', 'cconfig', 'rebalance', 'elr', 'monitor', 'kafbat', 'release42'],
)

ECOSYSTEM = dict(
    slug='ecosystem',
    no='A2',
    title='Kafka 생태계 둘러보기',
    short='Connect · 스키마 · Spring',
    desc='Kafka 주변에서 자주 함께 쓰는 Kafka Connect, Schema Registry, Spring for Apache Kafka, 로그 파이프라인 도구를 단원 개념과 연결해 정리합니다.',
    intro=r"""
이 과정은 Kafka의 동작 원리를 드러내기 위해 순수 Java 클라이언트로 모든 코드를 직접 작성했습니다. 실무에서는 같은 일을 대신해 주는 도구를 함께 쓰는 경우가 많습니다. 이 부록은 각 도구가 **단원에서 직접 구현한 어떤 부분을 대신하는지**에 초점을 맞춥니다. 도구를 쓰더라도 커밋, 중복, 재시도, 스키마 호환성 같은 원리는 그대로 적용됩니다.
""",
    sections=[
        dict(
            id='connect',
            kicker='A2-1 / KAFKA CONNECT',
            title='Kafka Connect: 코드 없이 외부 시스템과 잇기',
            toc='Kafka Connect',
            body=r"""
Kafka Connect는 Kafka와 다른 시스템 사이에서 데이터를 옮기는 **표준 실행 틀**입니다. DB, 파일, 검색 엔진, 객체 저장소 등과 연결하는 코드를 매번 새로 짜는 대신, 이미 만들어진 **커넥터(connector)**를 설정 파일만으로 실행합니다.

- **Source 커넥터:** 외부 시스템에서 데이터를 읽어 Kafka 토픽에 씁니다. DB 변경 내용을 이벤트로 보내는 CDC 커넥터가 대표적입니다(10단원의 Outbox와 연결됩니다).
- **Sink 커넥터:** Kafka 토픽을 읽어 외부 시스템에 씁니다. 토픽의 로그를 검색 엔진이나 분석 DB에 적재하는 식입니다.

커넥터를 실행하는 프로세스를 **워커(worker)**라고 합니다. 워커는 두 가지 방식으로 띄웁니다.

- **Standalone:** 프로세스 하나에서 모든 작업을 실행합니다. 설정이 간단하지만 장애 대비가 없어 개발용이나 아주 작은 환경에 맞습니다.
- **Distributed:** 여러 워커가 Kafka의 그룹 관리 기능으로 작업을 나눠 맡습니다. 워커를 늘리면 처리량이 늘고, 하나가 죽으면 다른 워커가 작업을 넘겨받습니다. 커넥터 설정은 REST API로만 관리합니다.

커넥터는 실제 일을 **태스크(task)** 단위로 쪼개 실행합니다. `tasks.max`는 태스크의 최대 개수이며, 커넥터가 그보다 적게 만들 수도 있습니다. Sink 커넥터의 태스크는 결국 Consumer이므로, 토픽 파티션 수보다 많아도 병렬성이 늘지 않습니다(03단원).

**Converter**는 Kafka에 저장되는 바이트와 Connect 내부 데이터 사이의 변환을 맡습니다. `key.converter`, `value.converter`로 JSON, Avro 등을 고르며, 커넥터와 독립적이라 어떤 커넥터와도 조합할 수 있습니다. 04단원의 Serializer/Deserializer 역할을 설정으로 고르는 셈입니다.

**SMT(Single Message Transform)**는 레코드가 지나갈 때 간단한 변형을 가합니다. 공식 문서의 예처럼 파일에서 읽은 한 줄을 `line` 필드로 감싸고, 출처를 나타내는 고정 필드를 덧붙일 수 있습니다.

```properties
transforms=MakeMap,InsertSource
transforms.MakeMap.type=org.apache.kafka.connect.transforms.HoistField$Value
transforms.MakeMap.field=line
transforms.InsertSource.type=org.apache.kafka.connect.transforms.InsertField$Value
transforms.InsertSource.static.field=data_source
transforms.InsertSource.static.value=test-file-source
```

Distributed 모드는 커넥터 설정, 소스 쪽 진행 위치(offset), 상태를 Kafka의 내부 토픽 세 개(`config.storage.topic`, `offset.storage.topic`, `status.storage.topic`)에 저장합니다. 공식 문서는 이 토픽들을 복제하고 compaction을 적용해 직접 만들어 두기를 권장합니다. 워커의 `group.id`는 Connect 클러스터를 식별하므로 일반 Consumer Group 이름과 겹치면 안 됩니다.

관리는 REST API로 합니다. `listeners`를 따로 정하지 않으면 8083 포트의 HTTP로 열립니다.

```text
GET    /connectors                    커넥터 목록
POST   /connectors                    커넥터 생성 (JSON 설정)
GET    /connectors/{name}/status      커넥터와 태스크 상태
PUT    /connectors/{name}/pause       일시 정지
POST   /connectors/{name}/restart     재시작
DELETE /connectors/{name}             삭제
GET    /connector-plugins             설치된 커넥터 플러그인
```
""",
            after=r"""
**오류 처리와 DLQ**

커넥터는 기본적으로 오류가 나면 바로 실패합니다(`errors.tolerance=none`). 07단원에서 직접 구현한 실패 기록을 Connect는 설정으로 제공합니다.

```properties
errors.tolerance=all
errors.deadletterqueue.topic.name=device-logs.connect.dlq
errors.deadletterqueue.context.headers.enable=true
errors.log.enable=true
```

`errors.tolerance=all`은 오류가 난 레코드 때문에 태스크를 멈추지 않게 하고, Dead Letter Queue 토픽은 Sink 커넥터가 처리하지 못한 레코드를 모아 둡니다. 컨텍스트 헤더를 켜면 원본 토픽과 오류 정보가 헤더로 붙습니다. `errors.log.include.messages`로 레코드 내용까지 로그에 남길 수 있지만 개인정보가 포함될 수 있으니 주의합니다. 재시도는 `errors.retry.timeout`, `errors.retry.delay.max.ms`로 조절합니다.

**Exactly-once**

Source 커넥터는 모든 워커에 `exactly.once.source.support=enabled`를 설정하면 정확히 한 번 전달을 지원합니다. 새 클러스터라면 처음부터 `enabled`로 두고, 기존 클러스터는 `preparing`으로 한 번, `enabled`로 한 번 롤링 재시작합니다. Distributed 모드에서만 동작하며, 커넥터 자체가 이 기능을 지원하도록 만들어져 있어야 합니다. Sink 쪽은 워커의 `consumer.isolation.level=read_committed` 같은 설정과 대상 시스템의 멱등 쓰기가 함께 필요합니다(09단원).
""",
            tables=[dict(
                title='Standalone과 Distributed 비교',
                columns=['항목', 'Standalone', 'Distributed'],
                rows=[
                    ['프로세스', '워커 1개', '워커 여러 개가 작업을 나눔'],
                    ['장애 대비', '없음. 프로세스가 죽으면 멈춤', '다른 워커가 태스크를 넘겨받음'],
                    ['설정 관리', '시작 시 설정 파일 또는 REST API', 'REST API만 사용'],
                    ['상태 저장', '로컬 파일', 'Kafka 내부 토픽 3개'],
                    ['어울리는 곳', '개발, 아주 작은 환경', '운영 환경 전반'],
                ],
            )],
        ),
        dict(
            id='schema-registry',
            kicker='A2-2 / SCHEMA',
            title='Schema Registry: 이벤트 계약을 한곳에서 관리',
            toc='Schema Registry',
            body=r"""
12단원에서 이벤트 형식은 서비스 사이의 **계약**이라고 했습니다. 이 과정은 `schemaVersion` 필드와 `OrderEvent.parse`의 검증 코드로 계약을 지켰지만, 서비스와 팀이 늘어나면 '지금 이 토픽의 형식이 무엇이고, 이번 변경이 기존 구독자를 깨뜨리는가'를 사람이 일일이 확인하기 어렵습니다.

**Schema Registry**는 스키마를 중앙 저장소에 등록하고 버전을 매기며, 새 버전을 등록할 때 **호환성 규칙을 자동으로 검사**하는 서비스입니다. 스키마는 **subject**라는 이름 단위로 묶여 버전 1, 2, 3...으로 쌓입니다. 형식은 Avro, Protobuf, JSON Schema를 주로 씁니다.

Producer는 전용 Serializer로 스키마를 등록하거나 조회한 뒤 메시지에 스키마 ID를 붙여 보내고, Consumer는 그 ID로 스키마를 받아 역직렬화합니다. 호환되지 않는 스키마는 등록 단계에서 거부되므로, 잘못된 형식이 토픽에 들어가기 전에 막을 수 있습니다.

Kafka에 포함된 기능이 아니라 **별도로 운영하는 제품**이라는 점도 기억해 두세요. 널리 쓰이는 Confluent Schema Registry는 자체 라이선스를 따르며, Apicurio Registry처럼 다른 구현도 있습니다. 어느 것을 쓰든 아래의 호환성 개념은 비슷합니다.
""",
            tables=[dict(
                title='호환성 규칙과 배포 순서 (Confluent Schema Registry 기준)',
                intro='기본값은 `BACKWARD`이며, 새 스키마를 직전 버전 하나와만 비교합니다. `_TRANSITIVE`가 붙으면 이전 모든 버전과 비교합니다.',
                columns=['규칙', '보장하는 것', '대표적인 허용 변경', '먼저 배포할 쪽'],
                rows=[
                    ['BACKWARD', '새 스키마의 Consumer가 이전 스키마로 쓴 데이터를 읽음', '선택 필드 추가, 필드 삭제', 'Consumer → Producer'],
                    ['BACKWARD_TRANSITIVE', 'BACKWARD를 이전 모든 버전에 대해 보장', '위와 같음', 'Consumer → Producer'],
                    ['FORWARD', '이전 스키마의 Consumer가 새 스키마로 쓴 데이터를 읽음', '필드 추가, 선택 필드 삭제', 'Producer → Consumer'],
                    ['FORWARD_TRANSITIVE', 'FORWARD를 이전 모든 버전에 대해 보장', '위와 같음', 'Producer → Consumer'],
                    ['FULL', 'BACKWARD와 FORWARD를 모두 만족', '선택 필드의 추가·삭제만', '어느 쪽이든 독립적으로'],
                    ['FULL_TRANSITIVE', 'FULL을 이전 모든 버전에 대해 보장', '위와 같음', '어느 쪽이든 독립적으로'],
                    ['NONE', '검사하지 않음', '모든 변경', '양쪽을 함께 조율해야 함'],
                ],
                note='정확히 허용되는 변경은 형식(Avro, Protobuf, JSON Schema)과 필드의 기본값 여부에 따라 달라집니다.',
            )],
            after=r"""
이 과정의 방식과 비교하면 차이가 분명합니다. `OrderEvent`는 알 수 없는 선택 필드를 무시하고(`FAIL_ON_UNKNOWN_PROPERTIES=false`) 지원하지 않는 `schemaVersion`을 거부합니다. 이는 Consumer 쪽에서 BACKWARD에 가까운 태도를 코드로 구현한 것입니다. 다만 검사가 Consumer가 레코드를 받은 뒤에야 일어나므로, 잘못된 레코드는 이미 토픽에 들어간 상태입니다. Schema Registry는 같은 검사를 **Producer가 쓰기 전**, 등록 시점으로 앞당겨 줍니다.
""",
        ),
        dict(
            id='spring-kafka',
            kicker='A2-3 / SPRING',
            title='Spring for Apache Kafka로 옮겨 보기',
            toc='Spring Kafka',
            body=r"""
Spring Boot 애플리케이션에서는 Spring for Apache Kafka(이하 Spring Kafka)가 Producer와 Consumer를 감싸 줍니다. 처음 보면 코드가 짧아져 원리가 가려지는데, 이 과정에서 직접 쓴 코드와 대응시켜 보면 무엇을 대신하는지 보입니다.
""",
            tables=[dict(
                title='이 과정의 코드와 Spring Kafka 구성 요소',
                columns=['이 과정에서 직접 구현한 것', 'Spring Kafka의 대응 요소', '관련 단원'],
                rows=[
                    ['`KafkaProducer` 생성과 `send().get()`', '`KafkaTemplate`의 `send()` (결과는 CompletableFuture로 확인)', '04'],
                    ['poll 루프와 레코드 반복', '`@KafkaListener` 메서드와 리스너 컨테이너', '03, 04'],
                    ['`commitOne()`으로 레코드별 커밋', '컨테이너가 커밋을 관리하며 AckMode로 시점을 고름', '05'],
                    ['Consumer 프로세스 여러 개 실행', '`ConcurrentKafkaListenerContainerFactory`의 concurrency', '03'],
                    ['`RetryConsumer`의 최대 3회 재시도', '`DefaultErrorHandler`와 BackOff', '07'],
                    ['`orders.dlt`로 실패 기록 발행', '`DeadLetterPublishingRecoverer`', '07'],
                    ['`OrderEvent.parse` 실패 처리', '`ErrorHandlingDeserializer`와 재시도 제외 예외', '07, 12'],
                ],
            )],
            after=r"""
```java
@KafkaListener(topics = "orders", groupId = "notification")
public void onOrder(ConsumerRecord<String, String> record) throws Exception {
    OrderEvent event = OrderEvent.parse(record.value());
    // 업무 처리. 예외를 던지면 컨테이너의 오류 처리기가 재시도와 DLT 발행을 맡습니다.
}

@Bean
public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template) {
    var recoverer = new DeadLetterPublishingRecoverer(template);        // 기본 대상: <원본토픽>-dlt, 같은 파티션
    var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L)); // 1초 간격 2회 재시도 = 총 3회 시도
    handler.addNotRetryableExceptions(IllegalArgumentException.class);  // 잘못된 데이터는 바로 DLT로
    return handler;
}
```

코드에 드러나지 않는 동작을 단원 개념과 함께 짚어 두면 다음과 같습니다.

- **기본 재시도 횟수:** 오류 처리기를 따로 정하지 않으면 `DefaultErrorHandler`는 `FixedBackOff(0, 9)`, 즉 대기 없이 9번 재시도해 총 10번 시도한 뒤 로그만 남깁니다. 07단원에서 말한 '제한된 재시도'를 기본값으로 갖추고 있지만, 대기 시간 0은 외부 시스템 장애에 어울리지 않으므로 직접 정하는 편이 좋습니다.
- **재시도 방식:** 실패한 레코드 이전까지는 커밋하고, 실패한 레코드부터 다시 받도록 seek합니다. 05단원의 '처리 완료 구간까지만 커밋한다'는 원칙과 같습니다.
- **재시도하지 않는 예외:** `DeserializationException`, `ClassCastException` 등은 기본으로 재시도 대상에서 빠지고 첫 실패에 바로 복구기(recoverer)로 넘어갑니다. 업무상 영구 오류인 예외는 `addNotRetryableExceptions`로 추가합니다.
- **DLT 이름과 파티션:** `DeadLetterPublishingRecoverer`는 기본으로 `<원본토픽>-dlt` 토픽의 **원본과 같은 번호 파티션**에 보냅니다. 따라서 DLT 토픽의 파티션 수가 원본 이상이어야 합니다. 원본 토픽·파티션·offset·예외 정보는 헤더로 붙습니다.
- **역직렬화 실패:** 값 자체를 읽을 수 없는 레코드는 리스너에 도달하기 전에 Consumer를 멈추게 할 수 있습니다. `ErrorHandlingDeserializer`로 실제 Deserializer를 감싸면 예외를 헤더에 담아 넘기고, DLT에는 원본 바이트를 그대로 보냅니다.
- **병렬성:** concurrency를 토픽 파티션 수보다 크게 잡으면 남는 Consumer는 파티션을 받지 못하고 놀게 됩니다(03단원).

재시도 동안 같은 파티션의 다음 레코드가 기다리는 것이 부담스럽다면 `@RetryableTopic`으로 **비차단(non-blocking) 재시도**를 구성할 수 있습니다. 실패한 레코드를 재시도용 토픽으로 넘기고 원래 Consumer는 다음 레코드로 진행합니다. 기본값은 1000ms 간격 최대 3회 시도이며, 필요한 재시도 토픽과 DLT를 자동으로 만듭니다. 대신 실패한 레코드가 뒤의 레코드보다 늦게 처리되므로 key별 순서가 바뀔 수 있습니다(07단원).
""",
        ),
        dict(
            id='log-tools',
            kicker='A2-4 / LOG PIPELINE',
            title='로그 파이프라인에서 만나는 도구',
            toc='로그 파이프라인 도구',
            body=r"""
장비 로그 수집처럼 데이터가 많고 형식이 단순한 파이프라인에서는 Kafka 앞뒤에 전용 도구를 붙이는 경우가 많습니다. 각 단원의 '로그 수집 파이프라인에 적용하면' 항목과 함께 읽어 보세요.

**수집 에이전트의 Kafka 출력.** 로그 수집 에이전트인 Fluent Bit에는 Kafka로 바로 보내는 출력 플러그인이 있습니다. 에이전트가 직접 Producer 역할을 하는 셈이라 중간 서버를 줄일 수 있습니다. 대신 브로커 주소, 인증 정보, 토픽 구조가 모든 장비의 에이전트 설정에 퍼지고, 입력 검증이나 공통 형식 변환을 한곳에서 하기 어렵습니다. 그래서 에이전트는 HTTP로 수집 서버에 보내고, 수집 서버가 검증한 뒤 Kafka에 쓰는 구성도 흔합니다.

**저장소가 Kafka를 직접 구독하는 방식.** ClickHouse는 Kafka 테이블 엔진으로 토픽을 직접 읽을 수 있습니다. Kafka 엔진 테이블은 데이터를 보관하지 않는 대기열 역할만 하고, 구체화 뷰(materialized view)가 레코드를 읽어 실제 저장 테이블(MergeTree 계열)에 넣습니다.

```sql
CREATE TABLE device_logs (ts DateTime64(3), device_id String, level String, message String)
ENGINE = MergeTree ORDER BY (device_id, ts);

CREATE TABLE device_logs_queue (ts DateTime64(3), device_id String, level String, message String)
ENGINE = Kafka('kafka:9092', 'device.logs', 'clickhouse-loader', 'JSONEachRow');

CREATE MATERIALIZED VIEW device_logs_mv TO device_logs AS SELECT * FROM device_logs_queue;
```

테이블 구조는 설명을 위한 예시이며, 실제 컬럼 타입과 시각 형식 설정은 ClickHouse 문서를 확인해 맞춥니다. 엔진의 인자는 차례로 브로커, 토픽, Consumer Group, 데이터 형식입니다. ClickHouse 문서는 이 엔진이 **at-least-once**라서 드물게 중복이 생길 수 있다고 밝힙니다. 연결이 끊기거나 리밸런싱이 일어나면 같은 묶음이 다시 들어올 수 있으므로, 05·06단원의 중복 대책을 저장소 쪽에서도 고민해야 합니다. 그룹 하나의 Consumer 수는 토픽 파티션 수를 넘을 수 없다는 03단원의 원칙도 그대로 적용됩니다.
""",
            tables=[dict(
                title='Kafka에 넣는 방법과 꺼내는 방법',
                columns=['방향', '방법', '장점', '주의할 점'],
                rows=[
                    ['넣기', '직접 만든 Producer 애플리케이션(수집 서버)', '검증, 형식 변환, 인증, 응답 시점을 마음대로 설계', '코드와 운영을 직접 책임짐'],
                    ['넣기', 'Kafka Connect Source 커넥터', '설정만으로 DB·파일 등에서 가져옴. 진행 위치를 Connect가 관리', '맞는 커넥터가 있어야 하고 Connect 클러스터 운영이 필요'],
                    ['넣기', '수집 에이전트의 Kafka 출력 플러그인', '중간 서버 없이 바로 전송', '모든 에이전트에 브로커 정보가 퍼지고 중앙 검증이 어려움'],
                    ['꺼내기', '직접 만든 Consumer 애플리케이션', '커밋 시점, 재시도, DLT, 멱등 처리를 정밀하게 제어', '이 과정의 05~07단원 내용을 직접 구현해야 함'],
                    ['꺼내기', 'Kafka Connect Sink 커넥터', '설정만으로 적재, DLQ와 재시도 설정 제공', '변환 로직이 복잡하면 SMT만으로 부족할 수 있음'],
                    ['꺼내기', '저장소의 Kafka 연동 기능(예: ClickHouse Kafka 엔진)', '별도 Consumer 서버 없이 저장소가 직접 읽음', '중복 가능성, 오류 레코드 처리 방식을 저장소 기능에 맞춰야 함'],
                ],
            )],
        ),
        dict(
            id='stream-processing',
            kicker='A2-5 / PROCESSING',
            title='Kafka Streams와 다른 처리 방식, Share Group',
            toc='처리 방식과 Share Group',
            body=r"""
11단원의 Kafka Streams는 **별도 서버 없이 애플리케이션 안에서 실행되는 Java 라이브러리**입니다. 입력과 출력이 모두 Kafka 토픽이고, 상태 복구도 Kafka의 changelog 토픽에 기대므로 Kafka 중심의 서비스에 잘 맞습니다. 반면 Apache Flink 같은 스트림 처리 엔진은 보통 별도 클러스터로 실행하며 Kafka 외의 다양한 입력과 출력을 다룹니다. 어느 쪽이든 이벤트 시간, 윈도우, 상태 저장, 처리 보장이라는 개념은 같으므로 11단원의 내용이 그대로 바탕이 됩니다.

**Share Group (Kafka Queues).** 일반 Consumer Group에서는 한 파티션을 그룹 안의 Consumer 하나만 맡기 때문에, Consumer를 파티션 수보다 늘려도 처리량이 늘지 않습니다(03단원). Kafka 4.2에서 운영 환경에 쓸 수 있는 상태가 된 Share Group(KIP-932)은 레코드를 하나씩 획득하고 처리 결과를 확인(acknowledge)하는 **작업 대기열 방식**을 제공합니다. 같은 파티션의 레코드를 여러 Consumer가 나눠 처리할 수 있어 파티션 수에 묶이지 않지만, 그만큼 파티션 안의 처리 순서는 보장하지 않습니다. 순서가 중요하면 일반 Consumer Group을, 순서와 무관한 작업을 많은 작업자에게 나누는 것이 목적이면 Share Group을 검토합니다.
""",
        ),
    ],
    sources=['connect', 'schema-evolution', 'spring-kafka', 'clickhouse-kafka', 'fluentbit-kafka', 'streams', 'release42'],
)


def term(id, term_name, en, body, refs):
    return dict(id=id, term=term_name, en=en, body=body, refs=refs)


GLOSSARY = dict(
    slug='glossary',
    no='A3',
    title='용어집',
    short='핵심 용어 70개 · 검색',
    desc='단원과 부록에 나오는 Kafka 용어를 주제별로 모아 짧게 정의하고, 자세히 다루는 단원으로 연결합니다.',
    intro=r"""
처음 Kafka를 공부할 때 가장 큰 벽은 비슷해 보이는 용어들입니다. offset과 committed offset, Replicas와 ISR, 트랜잭션 커밋과 offset 커밋처럼 이름은 닮았지만 가리키는 대상이 다른 경우가 많습니다. 각 용어는 2~4문장으로 짧게 정의하고, 아래의 단원 표시를 누르면 자세한 설명으로 이동합니다.

검색창에 한글이나 영문 일부를 입력하면 해당 용어만 남습니다.
""",
    sections=[],
    glossary=[
        dict(title='기본 구조', terms=[
            term('record', '레코드 (이벤트, 메시지)', 'record / event', r"""
Kafka에 저장되는 데이터 한 건입니다. key, value, timestamp, 선택적인 headers로 이루어지며, 이 과정에서는 주문 생성 사실 하나가 레코드 하나입니다. 한 번 기록되면 수정되지 않습니다.
""", ['01']),
            term('topic', '토픽', 'topic', r"""
같은 종류의 레코드를 모아 두는 이름 붙은 로그입니다. 여러 Producer가 쓰고 여러 Consumer Group이 읽을 수 있으며, 읽어도 레코드가 지워지지 않습니다. 이 과정은 `orders`, `orders.dlt` 등 5개 토픽을 씁니다.
""", ['01', 'cli']),
            term('partition', '파티션', 'partition', r"""
토픽을 나눈 독립된 로그입니다. 순서는 파티션 안에서만 보장되고, 그룹 안의 병렬 처리 단위도 파티션입니다. 파티션 수는 늘릴 수 있지만 줄일 수 없습니다.
""", ['02', '03']),
            term('offset', 'Offset', 'offset', r"""
파티션 안에서 레코드의 위치를 나타내는 번호입니다. 파티션마다 0부터 따로 증가하므로 P0의 offset 7과 P1의 offset 7은 다른 레코드입니다. Consumer의 진행 위치를 표현할 때도 이 번호를 씁니다.
""", ['02', '05']),
            term('segment', '세그먼트', 'log segment', r"""
파티션 로그를 디스크에 나눠 저장한 파일 단위입니다. 새 레코드는 활성 세그먼트의 끝에 덧붙고, 일정 크기(`segment.bytes`, 기본 1GiB)나 시간(`segment.ms`, 기본 7일)이 지나면 새 세그먼트로 넘어갑니다. 보관 정책에 따른 삭제도 세그먼트 단위로 일어납니다.
""", ['02']),
            term('retention', '보관 정책', 'retention', r"""
레코드를 얼마나 오래 또는 얼마나 많이 보관할지 정하는 설정입니다. `retention.ms`(기본 7일)와 `retention.bytes`(기본 제한 없음)로 정하며, 읽었는지 여부와 관계없이 기준을 넘은 오래된 세그먼트가 정리됩니다. 재처리할 수 있는 범위가 이 기간으로 정해집니다.
""", ['01', '02', 'cli']),
            term('compaction', 'Log Compaction', 'log compaction', r"""
`cleanup.policy=compact`인 토픽에서 key별로 최신 값만 남기고 이전 값을 정리하는 방식입니다. 정리는 백그라운드에서 비동기로 일어나므로 같은 key의 옛 레코드가 즉시 사라지지는 않습니다. key별 최신 상태를 보관하는 용도로 씁니다.
""", ['02']),
            term('tombstone', '톰스톤', 'tombstone', r"""
compaction 토픽에서 key는 있고 value가 null인 레코드입니다. 해당 key의 이전 값을 지우라는 표시로 쓰이며, 톰스톤 자체도 `delete.retention.ms`(기본 1일)가 지나면 정리됩니다.
""", ['02']),
            term('broker', '브로커', 'broker', r"""
레코드를 받아 디스크에 저장하고 Consumer에게 내주는 Kafka 서버 프로세스입니다. 여러 브로커가 모여 클러스터를 이루며, 각 파티션의 리더와 복제본을 나눠 맡습니다.
""", ['01', '08']),
            term('cluster', '클러스터', 'cluster', r"""
함께 동작하는 브로커와 컨트롤러의 묶음입니다. 이 과정의 단일 환경은 노드 1개, `-Cluster` 환경은 노드 3개로 된 서로 다른 클러스터이며 데이터를 공유하지 않습니다.
""", ['01', '08']),
            term('kraft', 'KRaft 컨트롤러', 'KRaft controller', r"""
토픽, 파티션 리더, 브로커 상태 같은 클러스터 메타데이터를 관리하는 Kafka 내부 구성 요소입니다. 여러 컨트롤러가 Raft 합의로 정보를 맞추며, Kafka 4.0부터는 ZooKeeper 없이 KRaft만 지원합니다. 과반이 살아 있어야 메타데이터를 바꿀 수 있습니다.
""", ['01', '08']),
            term('key', 'Key', 'record key', r"""
레코드를 어느 파티션에 보낼지 정하는 데 쓰이는 값입니다. 파티션 수가 그대로라면 같은 key는 같은 파티션으로 가므로 key별 순서를 지킬 수 있습니다. 이 과정은 `orderId`를 key로 씁니다.
""", ['02']),
            term('header', '헤더', 'record header', r"""
레코드 본문과 분리해 붙이는 key-value 형태의 부가 정보입니다. 추적 ID, 원본 위치, 오류 정보 등을 담으며, 07단원의 DLT 레코드는 `source-position` 헤더를 붙입니다.
""", ['01', '07']),
            term('timestamp', 'Timestamp', 'record timestamp', r"""
레코드의 시각입니다. 기본값 `CreateTime`은 Producer가 레코드를 만든 시각, `LogAppendTime`은 브로커가 저장한 시각입니다. Kafka 4.0부터는 브로커 시각보다 1시간 넘게 미래인 `CreateTime` 레코드를 기본적으로 거부합니다.
""", ['01']),
        ]),
        dict(title='Producer', terms=[
            term('producer', 'Producer', 'producer', r"""
레코드를 만들어 토픽에 보내는 클라이언트입니다. key로 파티션을 고르고, 레코드를 배치로 묶어 보내며, 브로커의 확인(ack)을 받아 성공 여부를 알립니다.
""", ['01', '04']),
            term('serializer', 'Serializer / Deserializer', 'serializer', r"""
Java 객체와 네트워크로 오가는 바이트를 서로 바꾸는 구성 요소입니다. 이 과정은 JSON 문자열을 `StringSerializer`로 바이트로 바꾸고, Consumer는 `StringDeserializer`로 되돌립니다. Kafka는 바이트의 형식을 해석하지 않습니다.
""", ['04', 'ecosystem']),
            term('acks', 'acks', 'acks', r"""
Producer가 전송 성공으로 판단하기 전에 기다리는 확인 수준입니다. `0`은 기다리지 않고, `1`은 리더의 기록만, `all`은 현재 ISR 전체의 기록을 기다립니다. 현재 기본값은 `all`입니다.
""", ['04', '08']),
            term('idempotent-producer', '멱등 Producer', 'idempotent producer', r"""
Producer 내부 재시도 때문에 같은 레코드가 중복 기록되는 것을 막는 기능입니다. 현재 기본으로 켜져 있으며 `acks=all`, 재시도 허용, `max.in.flight.requests.per.connection` 5 이하가 필요합니다. 애플리케이션이 `send()`를 두 번 호출한 중복은 막지 못합니다.
""", ['04', '06']),
            term('batch-linger', '배치와 linger.ms', 'batching / linger.ms', r"""
Producer는 같은 파티션으로 갈 레코드를 배치로 묶어 보냅니다. `linger.ms`는 배치를 채우려고 잠시 기다리는 시간으로, Kafka 4.0부터 기본값이 0에서 5ms로 바뀌었습니다. `batch.size`(기본 16384바이트)가 차면 기다리지 않고 바로 보냅니다.
""", ['04']),
            term('compression', '압축', 'compression.type', r"""
Producer가 배치 단위로 데이터를 압축하는 설정입니다. `gzip`, `snappy`, `lz4`, `zstd` 중에서 고르며 기본값은 압축 없음입니다. 배치가 클수록 압축 효율이 좋아지고 네트워크와 디스크 사용량이 줄지만 CPU를 더 씁니다.
""", ['04']),
            term('partitioner', '파티셔너', 'partitioner', r"""
레코드를 보낼 파티션을 고르는 Producer 구성 요소입니다. 기본 동작에서는 key가 있으면 key의 해시로 파티션을 정합니다. 직접 만든 파티셔너를 지정할 수도 있습니다.
""", ['02']),
            term('sticky-partitioning', 'Sticky 파티셔닝', 'sticky partitioning', r"""
key가 없는 레코드를 한 파티션에 몰아 배치를 채운 뒤 다음 파티션으로 넘어가는 기본 동작입니다. 레코드마다 파티션을 바꾸는 것보다 배치가 커져 효율이 좋고, 길게 보면 파티션 사이에 고르게 퍼집니다.
""", ['02']),
        ]),
        dict(title='Consumer와 그룹', terms=[
            term('consumer', 'Consumer', 'consumer', r"""
토픽에서 레코드를 가져와 처리하는 클라이언트입니다. 브로커가 밀어 주기를 기다리지 않고 `poll()`로 스스로 가져가는 pull 방식이라, 느린 Consumer는 뒤처질 뿐 데이터에 깔리지 않습니다.
""", ['01', '04']),
            term('consumer-group', 'Consumer Group', 'consumer group', r"""
`group.id`가 같은 Consumer들의 묶음으로, 하나의 논리적 구독자처럼 동작합니다. 그룹 안에서는 파티션을 나눠 맡고, 서로 다른 그룹은 같은 토픽을 독립적으로 읽으며 각자 위치를 커밋합니다.
""", ['03']),
            term('group-coordinator', '그룹 코디네이터', 'group coordinator', r"""
특정 Consumer Group의 구성원 관리와 offset 커밋 저장을 맡는 브로커입니다. 멤버의 가입과 이탈을 감지해 리밸런싱을 이끕니다. `kafka-consumer-groups.sh --describe --state`로 어느 브로커인지 확인할 수 있습니다.
""", ['03', 'cli']),
            term('rebalance', '리밸런싱', 'rebalance', r"""
그룹의 구성원이 바뀔 때 파티션 배정을 다시 정하는 과정입니다. Consumer의 가입, 종료, 장애, 처리 지연으로 일어나며, 이때 미커밋 처리는 다른 Consumer가 다시 실행할 수 있습니다.
""", ['03', '05']),
            term('assignor', 'Assignor', 'partition assignor', r"""
리밸런싱 때 파티션을 구성원에게 어떻게 나눌지 정하는 전략입니다. classic 프로토콜의 기본값은 `RangeAssignor`와 `CooperativeStickyAssignor` 순서이고, 새 consumer 프로토콜은 브로커 쪽의 `uniform`(기본), `range`를 씁니다.
""", ['03']),
            term('cooperative-rebalance', '협력적 리밸런싱', 'cooperative rebalancing', r"""
모든 파티션을 한꺼번에 회수하지 않고 옮겨야 할 파티션만 단계적으로 넘기는 리밸런싱 방식입니다. 나머지 파티션은 처리를 멈추지 않아 리밸런싱 중의 정지 시간이 줄어듭니다.
""", ['03']),
            term('consumer-protocol', '새 consumer 그룹 프로토콜', 'KIP-848 consumer rebalance protocol', r"""
Kafka 4.0에서 정식 기능이 된 차세대 리밸런싱 프로토콜입니다. 배정을 브로커가 계산하고 전체 동기화 없이 점진적으로 진행해 리밸런싱이 빨라집니다. 클라이언트에서 `group.protocol=consumer`로 켜며, 이 과정의 실습은 반복 관찰을 위해 classic을 명시합니다.
""", ['03', '12']),
            term('static-membership', '정적 멤버십', 'static membership', r"""
`group.instance.id`를 지정해 Consumer를 고정된 구성원으로 등록하는 방식입니다. 같은 ID로 짧게 재시작하면 새 멤버로 취급되지 않아 불필요한 리밸런싱을 줄일 수 있습니다. 같은 ID를 가진 인스턴스는 그룹에 하나만 있을 수 있습니다.
""", ['03']),
            term('max-poll-interval', 'max.poll.interval.ms', 'max.poll.interval.ms', r"""
`poll()` 호출 사이에 허용되는 최대 간격입니다(기본 5분). 한 배치의 처리가 이보다 오래 걸리면 Consumer가 그룹에서 빠지고 리밸런싱이 일어납니다. `max.poll.records`를 줄이거나 처리 시간을 제한해 대응합니다.
""", ['03', '07']),
            term('position', 'Position', 'consumer position', r"""
Consumer가 다음 `poll()`에서 가져올 offset입니다. 레코드를 받는 즉시 메모리 안에서 앞으로 움직이며, 커밋하지 않으면 재시작 후에는 사라집니다.
""", ['05']),
            term('committed-offset', 'Committed Offset', 'committed offset', r"""
그룹이 저장해 둔 복구 위치로, 다음에 읽을 offset입니다. offset 12까지 처리했다면 13을 커밋합니다. 재시작이나 리밸런싱 뒤에는 이 위치부터 다시 읽습니다.
""", ['05', 'cli']),
            term('auto-offset-reset', 'auto.offset.reset', 'auto.offset.reset', r"""
그룹에 커밋 기록이 없을 때 어디서 읽기 시작할지 정하는 설정입니다. `latest`(기본), `earliest`, `none`, 지정한 기간만큼 거슬러 올라가는 `by_duration:<기간>` 중에서 고릅니다. 이미 커밋 기록이 있는 그룹에는 적용되지 않습니다.
""", ['01', '03', '05']),
            term('consumer-offsets', '__consumer_offsets', '__consumer_offsets', r"""
Consumer Group의 커밋 위치를 저장하는 Kafka 내부 토픽입니다. 그룹 코디네이터가 이 토픽에 기록하고 읽습니다. 클러스터 환경에서는 이 토픽도 복제해 두어야 커밋 정보를 잃지 않습니다.
""", ['03', '05', '08', '09']),
            term('share-group', 'Share Group', 'share group (Kafka Queues)', r"""
레코드를 하나씩 획득하고 처리 결과를 확인하는 작업 대기열 방식의 그룹입니다(KIP-932). Kafka 4.2에서 운영 환경에 쓸 수 있는 상태가 되었으며, 같은 파티션을 여러 Consumer가 나눠 처리할 수 있는 대신 순서를 보장하지 않습니다.
""", ['01', '03', 'ecosystem']),
        ]),
        dict(title='복제와 클러스터', terms=[
            term('leader', '리더', 'partition leader', r"""
파티션의 읽기와 쓰기를 맡은 복제본입니다. Producer와 Consumer는 메타데이터로 리더를 찾아 통신하며, 리더가 멈추면 컨트롤러가 다른 복제본을 새 리더로 선출합니다.
""", ['08', 'cli']),
            term('follower', '팔로워', 'follower replica', r"""
리더의 로그를 복제해 따라가는 복제본입니다. 리더에게 데이터를 가져와 자기 로그에 덧붙이며, 충분히 따라잡은 상태라면 ISR에 포함되어 리더 후보가 됩니다.
""", ['08']),
            term('replica', '복제본과 복제 수', 'replica / replication factor', r"""
같은 파티션 데이터를 여러 브로커에 나눠 둔 사본입니다. 복제 수 3은 사본이 3개라는 뜻이며, 같은 그룹이 레코드를 세 번 처리한다는 뜻이 아닙니다. 운영 환경에서는 3을 흔히 씁니다.
""", ['08']),
            term('isr', 'ISR', 'in-sync replicas', r"""
배정된 복제본 중 현재 리더와 동기화된 복제본의 집합입니다. `replica.lag.time.max.ms` 안에 리더를 따라잡지 못한 팔로워는 ISR에서 빠집니다. `acks=all`은 이 집합 전체의 기록을 기다립니다.
""", ['08', 'cli']),
            term('min-insync-replicas', 'min.insync.replicas', 'min.insync.replicas', r"""
`acks=all` 쓰기가 성공하려면 필요한 최소 ISR 수입니다(리더 포함, 토픽 기본값 1). 복제 수 3에 이 값을 2로 두면 브로커 하나가 멈춰도 쓰기가 계속되고, 둘이 멈추면 쓰기를 거부해 유실 위험을 막습니다.
""", ['08']),
            term('high-watermark', 'High Watermark', 'high watermark', r"""
ISR 전체에 복제가 끝나 '커밋된' 레코드의 경계입니다. Consumer는 이 경계 앞의 레코드만 읽을 수 있으므로, 리더가 바뀌어도 사라질 수 있는 레코드를 읽지 않습니다.
""", ['08']),
            term('leo', 'LEO', 'log end offset', r"""
복제본마다 다음 레코드가 기록될 위치입니다. 리더의 LEO가 앞서 있고 팔로워가 따라오는 동안 High Watermark는 ISR 가운데 가장 뒤처진 위치를 따라갑니다. CLI의 LOG-END-OFFSET도 파티션 끝을 가리키는 같은 계열의 값입니다.
""", ['08', '12']),
            term('elr', 'ELR', 'Eligible Leader Replicas', r"""
ISR에서 빠졌지만 데이터가 안전해 리더가 될 수 있는 복제본 목록입니다(KIP-966). Kafka 4.1부터 새 클러스터에서 기본으로 켜지며, `kafka-topics --describe` 출력의 `Elr`, `LastKnownElr` 열에 나타납니다.
""", ['08', 'cli']),
            term('unclean-election', 'Unclean 리더 선출', 'unclean leader election', r"""
ISR의 복제본이 모두 사라졌을 때 동기화되지 않은 복제본을 리더로 세우는 마지막 수단입니다. 가용성은 되찾지만 커밋된 레코드를 잃을 수 있어 기본값은 꺼짐(`unclean.leader.election.enable=false`)입니다.
""", ['08']),
        ]),
        dict(title='전달 보장과 트랜잭션', terms=[
            term('at-most-once', 'At-most-once', 'at-most-once', r"""
레코드를 잃을 수는 있지만 두 번 처리하지는 않는 전달 보장입니다. 처리 전에 커밋하면 이렇게 되며, 커밋 직후 장애가 나면 그 레코드를 건너뜁니다.
""", ['05']),
            term('at-least-once', 'At-least-once', 'at-least-once', r"""
레코드를 잃지 않지만 두 번 이상 처리할 수 있는 전달 보장으로, Kafka의 기본적인 성격입니다. 처리 후에 커밋하면 이렇게 되며, 중복에 대비한 멱등 처리가 함께 필요합니다.
""", ['05', '06']),
            term('exactly-once', 'Exactly-once', 'exactly-once semantics', r"""
각 레코드의 처리 결과가 한 번만 반영되는 보장입니다. Kafka에서는 트랜잭션 Producer와 `read_committed` Consumer, Kafka Streams로 Kafka 안의 읽기·처리·쓰기에 대해 제공합니다. 외부 DB나 API 호출까지 자동으로 포함하지는 않습니다.
""", ['09', '11']),
            term('transaction', '트랜잭션', 'Kafka transaction', r"""
여러 토픽·파티션에 대한 쓰기와 Consumer offset 커밋을 하나로 묶어 모두 반영하거나 모두 취소하는 기능입니다. `beginTransaction`, `commitTransaction`, `abortTransaction`으로 경계를 정합니다.
""", ['09']),
            term('transactional-id', 'transactional.id', 'transactional.id', r"""
트랜잭션 Producer를 식별하는 고정 ID입니다. 재시작해도 같은 ID를 쓰면 이전 세션의 미완료 트랜잭션을 정리하고 옛 인스턴스를 차단할 수 있습니다. 동시에 실행되는 인스턴스는 서로 다른 ID를 가져야 합니다.
""", ['09']),
            term('fencing', 'Fencing', 'producer fencing', r"""
같은 `transactional.id`로 새 Producer가 시작되면 이전 Producer를 더 이상 쓰지 못하게 막는 동작입니다. 멈춘 줄 알았던 옛 인스턴스가 늦게 깨어나 결과를 중복으로 쓰는 일을 막습니다.
""", ['09']),
            term('control-record', '제어 레코드', 'control record / transaction marker', r"""
트랜잭션의 commit 또는 abort를 표시하려고 파티션에 기록되는 특수 레코드입니다. Consumer는 이를 업무 레코드로 돌려주지 않지만 offset은 차지하므로, offset 차이가 업무 레코드 수와 다를 수 있습니다.
""", ['09', '12']),
            term('lso', 'LSO', 'last stable offset', r"""
아직 끝나지 않은 트랜잭션이 시작되기 직전까지의 위치입니다. `read_committed` Consumer는 이 위치까지만 읽으므로, 오래 열린 트랜잭션이 있으면 그 뒤의 커밋된 레코드도 보이지 않고 기다립니다.
""", ['09']),
            term('read-committed', 'read_committed', 'isolation.level', r"""
Consumer의 격리 수준 설정 값으로, 커밋된 트랜잭션의 레코드만 돌려줍니다. 기본값 `read_uncommitted`는 abort된 트랜잭션의 레코드도 돌려줍니다.
""", ['09', 'cli']),
            term('outbox', 'Transactional Outbox', 'transactional outbox', r"""
업무 데이터와 발행할 이벤트를 같은 DB 트랜잭션으로 저장하고, 별도 Relay가 나중에 Kafka로 보내는 패턴입니다. DB와 Kafka 사이의 '한쪽만 성공' 문제를 없애지만, Relay는 같은 이벤트를 다시 보낼 수 있습니다.
""", ['10']),
            term('idempotent-consumer', '멱등 Consumer', 'idempotent consumer', r"""
같은 이벤트를 여러 번 받아도 업무 결과가 한 번만 반영되도록 만든 Consumer입니다. 이 과정은 eventId 처리 표식과 합계 갱신을 같은 DB 트랜잭션에 넣고 유일 제약으로 중복을 걸러 냅니다.
""", ['06', '10']),
            term('dlt', 'DLT', 'dead letter topic', r"""
처리에 끝내 실패한 레코드를 원본 위치와 오류 정보와 함께 모아 두는 토픽입니다. 실패한 레코드 때문에 뒤의 처리가 막히지 않게 하며, 원인 수정 후 재처리할 근거가 됩니다. DLT 발행이 실패하면 원본 offset을 커밋하지 않습니다.
""", ['07', 'ecosystem']),
        ]),
        dict(title='Streams', terms=[
            term('kstream', 'KStream', 'KStream', r"""
Kafka Streams에서 레코드의 흐름을 나타내는 추상화입니다. 각 레코드를 독립된 사건으로 다루며, 변환하거나 다른 토픽으로 보낼 수 있습니다.
""", ['11']),
            term('ktable', 'KTable', 'KTable', r"""
key별 최신 값을 나타내는 추상화입니다. 같은 key의 새 레코드가 오면 이전 값을 갱신하며, 이 과정에서는 주문별 이벤트 건수를 담습니다.
""", ['11']),
            term('state-store', '상태 저장소', 'state store', r"""
집계나 조인 결과처럼 처리 중에 유지해야 하는 상태를 담는 Streams의 로컬 저장소입니다. 이 과정의 `order-counts`가 그 예이며, 변경 내용은 changelog 토픽에 함께 기록됩니다.
""", ['11']),
            term('changelog', 'Changelog 토픽', 'changelog topic', r"""
상태 저장소의 변경을 기록하는 Kafka 내부 토픽입니다. 애플리케이션이 재시작하거나 다른 인스턴스로 작업이 옮겨질 때 이 토픽을 읽어 상태를 복구합니다.
""", ['11']),
            term('repartition', 'Repartition 토픽', 'repartition topic', r"""
key를 바꾼 뒤 그 key로 집계하려 할 때 Streams가 자동으로 만드는 중간 토픽입니다. 같은 새 key의 레코드를 같은 파티션으로 다시 모으는 역할을 합니다.
""", ['11']),
            term('application-id', 'application.id', 'application.id', r"""
Kafka Streams 애플리케이션의 이름입니다. 내부 Consumer Group 이름과 내부 토픽 이름의 접두사로 쓰이므로, 이 값을 바꾸면 처음부터 다시 처리하는 새 애플리케이션이 됩니다.
""", ['11']),
            term('window', '윈도우', 'window', r"""
시간을 일정 구간으로 나눠 그 안의 레코드끼리 집계하는 방식입니다. 예를 들어 1분 단위로 장비별 오류 건수를 셀 때 씁니다. 늦게 도착한 레코드를 얼마나 기다릴지도 함께 정합니다.
""", ['11']),
        ]),
        dict(title='운영과 생태계', terms=[
            term('lag', 'Consumer Lag', 'consumer lag', r"""
파티션의 끝 위치와 그룹이 커밋한 위치의 차이로, 아직 커밋하지 않은 진행량입니다. 몇 초 늦었는지를 나타내는 시간 지표가 아니므로 처리율, 이벤트 시각과 함께 해석합니다.
""", ['12', 'cli']),
            term('security', 'TLS · SASL · ACL', 'security', r"""
Kafka 보안의 세 축입니다. TLS는 통신을 암호화하고, SASL은 클라이언트가 누구인지 인증하며, ACL은 인증된 주체가 어떤 토픽이나 그룹에 무엇을 할 수 있는지 정합니다. 이 과정의 실습 환경은 로컬 전용이라 셋 다 쓰지 않습니다.
""", ['12']),
            term('tiered-storage', 'Tiered Storage', 'tiered storage', r"""
오래된 세그먼트를 브로커 디스크 대신 원격 저장소로 옮겨 보관하는 기능입니다. 토픽의 `remote.storage.enable`로 켜고, `local.retention.ms`로 브로커에 남길 기간을 정합니다. 긴 보관 기간이 필요할 때 디스크 비용을 줄이는 수단입니다.
""", ['12']),
            term('kafka-connect', 'Kafka Connect', 'Kafka Connect', r"""
Kafka와 외부 시스템 사이의 데이터 이동을 커넥터 설정만으로 실행하는 틀입니다. Source 커넥터는 외부에서 Kafka로, Sink 커넥터는 Kafka에서 외부로 데이터를 옮깁니다.
""", ['07', '10', 'ecosystem']),
            term('schema-registry', 'Schema Registry', 'Schema Registry', r"""
이벤트 스키마를 버전별로 등록하고 새 버전의 호환성을 검사하는 별도 서비스입니다. Producer가 쓰기 전에 형식 오류를 막아 주며, 기본 호환성 규칙은 BACKWARD입니다.
""", ['12', 'ecosystem']),
            term('kafka-ui', 'Kafka UI', 'web UI', r"""
토픽, 메시지, Consumer Group의 Lag, 브로커 상태를 웹 화면으로 보여 주는 도구입니다. 부록 A1에서는 오픈소스 Kafbat UI를 실습 네트워크에 붙여 실행하는 방법을 다룹니다.
""", ['cli']),
        ]),
    ],
    sources=['intro', 'design', 'pconfig', 'cconfig', 'tconfig', 'elr', 'rebalance'],
)

APPENDICES = [CLI, ECOSYSTEM, GLOSSARY]
