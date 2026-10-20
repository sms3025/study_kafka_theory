"""LESSON 05 · 수동 커밋과 전달 보장: 이론 페이지와 실습 안내의 원본입니다."""

LESSON = dict(
    id='05',
    level='중급',
    title='처리했는데 왜 또 들어올까?',
    short='수동 커밋과 전달 보장',
    minutes=60,
    desc='커밋 전후에 프로세스를 멈춰 중복과 누락의 원인을 재현하고, 커밋 위치로 전달 보장이 정해지는 원리를 이해합니다.',
    goals=[
        'position과 committed offset을 구분한다',
        '처리 전·후 커밋의 실패 구간을 찾는다',
        '부분 처리 배치의 안전한 커밋 위치를 계산한다',
        'at-most-once · at-least-once · exactly-once를 커밋 위치로 설명한다',
    ],
    why=r"""
초급 단원에서는 Consumer가 레코드를 읽고 나면 다음에 이어서 읽는다는 사실만 확인했습니다. 그런데 운영에서는 이런 질문이 곧바로 따라옵니다. "배포하려고 Consumer를 껐다 켰더니 문자가 두 번 갔어요." "장애 뒤에 재시작했는데 주문 몇 건이 통계에서 빠졌어요." 두 문제의 원인은 대부분 같은 곳, 즉 **커밋을 언제 하느냐**에 있습니다.

Kafka는 Consumer가 업무를 끝냈는지 알지 못합니다. Consumer가 알려 주는 '다음에 읽을 위치(committed offset)'만 기억하고, 재시작하면 그 위치부터 다시 내줍니다. 그래서 커밋을 업무 처리 앞에 두면 누락이, 뒤에 두면 중복이 생길 수 있습니다. 이 단원에서는 두 실패 구간을 일부러 만들어 보고, 그 결과로 전달 보장(delivery semantics)이 어떻게 정해지는지 정리합니다. 06단원의 멱등 처리와 09단원의 트랜잭션은 모두 여기서 출발합니다.
""",
    analogy=('책갈피를 언제 옮기느냐의 문제', r"""
두꺼운 업무 지시서 묶음을 한 장씩 처리하는 담당자를 떠올려 보세요. 담당자는 처리한 장 다음에 책갈피를 끼워 두고, 퇴근하거나 자리를 비웠다가 돌아오면 책갈피 위치부터 다시 일을 시작합니다.

책갈피를 **일을 끝낸 뒤에** 옮기는 습관이라면, 일을 끝내고 책갈피를 옮기기 직전에 호출을 받아 자리를 비울 때 문제가 생깁니다. 돌아와서는 책갈피가 아직 이전 장에 있으니 같은 장을 한 번 더 처리합니다. 이것이 **중복**입니다.

반대로 책갈피를 **일을 시작하기 전에** 옮기는 습관이라면, 책갈피만 옮기고 일을 하기 전에 자리를 비운 경우 돌아와서 그 장을 건너뜁니다. 이것이 **누락**입니다.

어느 쪽이든 책갈피를 옮기는 순간과 일을 끝내는 순간이 동시에 일어나지 않기 때문에 생기는 틈입니다. Kafka의 커밋도 같습니다. 이 틈을 없앨 수는 없으므로, 어느 쪽 실패를 허용하고 어떻게 보완할지 선택해야 합니다.
"""),
    concept=[
        ('읽은 위치와 복구 위치는 다릅니다', r"""
Consumer는 파티션마다 두 개의 위치를 다룹니다.

- **position:** 다음 `poll()`이 가져올 레코드의 offset입니다. Consumer 프로세스의 메모리에만 있는 값이며, `poll()`이 레코드를 돌려줄 때마다 앞으로 이동합니다.
- **committed offset:** Consumer Group이 Kafka에 저장해 둔 '다음에 읽을 위치'입니다. Kafka 내부 토픽 `__consumer_offsets`에 기록되며, 재시작하거나 리밸런싱으로 파티션이 다른 Consumer에게 넘어갈 때 이 값부터 읽기 시작합니다.

`poll()`로 offset 5~9를 받았다면 position은 곧바로 10이 됩니다. 하지만 커밋하지 않았다면 committed offset은 여전히 5입니다. 이때 프로세스가 죽으면 position 10이라는 정보는 메모리와 함께 사라지고, 새로 시작한 Consumer는 5부터 다시 읽습니다. 레코드를 **읽었다는 것**, 업무를 **처리했다는 것**, 복구 위치를 **저장했다는 것**은 모두 다른 사건입니다.

committed offset은 '마지막으로 처리한 offset'이 아니라 **'다음에 읽을 offset'**이라는 점을 꼭 기억하세요. offset 12를 처리했다면 13을 커밋합니다. 이 과정의 `Settings.commitOne`이 `r.offset() + 1`을 쓰는 이유입니다.
"""),
        ('처리 후 커밋에는 재처리 구간이 있습니다', r"""
가장 흔한 순서는 '처리 → 커밋'입니다. 업무를 끝낸 뒤에만 다음 위치를 저장하므로, 처리하지 못한 레코드를 건너뛰는 일은 없습니다. 대신 처리와 커밋 사이에 장애가 나면 문제가 생깁니다.

```text
committed=5 → poll(offset 5) → 업무 처리 성공 → [여기서 장애] → commit(6) 실행 못 함
재시작 → committed=5부터 읽음 → offset 5를 한 번 더 처리
```

이렇게 **놓치지는 않지만 중복될 수 있는** 방식을 at-least-once라고 부릅니다. Kafka 설계 문서도 at-least-once를 기본 동작으로 설명합니다. 중복을 견디려면 같은 레코드를 두 번 처리해도 결과가 달라지지 않는 **멱등(idempotent) 처리**가 필요하고, 이것이 06단원의 주제입니다.

재처리 범위는 커밋 간격에 비례합니다. 레코드마다 커밋하면 최대 1건이 다시 처리되고, 100건마다 커밋하면 최대 100건이 다시 처리될 수 있습니다. 커밋은 브로커와 통신하는 비용이 있으므로, 운영에서는 처리량과 허용 가능한 재처리 범위를 함께 놓고 간격을 정합니다.
"""),
        ('처리 전 커밋에는 건너뛰는 구간이 있습니다', r"""
순서를 뒤집어 '커밋 → 처리'로 하면 중복은 사라지지만 반대 문제가 생깁니다.

```text
committed=5 → poll(offset 5) → commit(6) 성공 → [여기서 장애] → 업무 처리 못 함
재시작 → committed=6부터 읽음 → offset 5는 영영 처리되지 않음
```

이 방식은 **중복은 없지만 잃어버릴 수 있는** at-most-once입니다. 데이터는 Kafka에 그대로 남아 있지만, 이 그룹의 책갈피가 이미 지나갔기 때문에 다시 읽히지 않습니다. 누락은 오류 로그도 남기지 않아 발견하기가 훨씬 어렵습니다.

이 단원의 `crash-before` 모드는 이 상태를 일부러 만듭니다. 커밋을 먼저 한 뒤 처리 전에 `Runtime.halt(17)`로 프로세스를 강제 종료합니다. 같은 그룹으로 다시 실행하면 직전에 출력된 partition/offset이 처리 목록에 나타나지 않는 것을 확인할 수 있습니다.
"""),
        ('배치 일부만 처리했으면 일부만 커밋합니다', r"""
`poll()`은 보통 레코드를 여러 건 돌려줍니다. 이 과정은 `max.poll.records=10`이라 최대 10건이고, Kafka 기본값은 500건입니다. 이때 실수하기 쉬운 지점이 **인자 없는 `commitSync()`**입니다.

인자 없는 `commitSync()`는 '마지막 `poll()`이 돌려준 위치까지'를 커밋합니다. offset 0~9를 받아 0~3만 처리하고 4번에서 예외가 났는데 이 메서드를 호출하면, 아직 처리하지 않은 4~9까지 처리한 것으로 저장됩니다. 재시작하면 4~9는 건너뜁니다.

안전한 방법은 파티션별로 **처리를 마친 마지막 offset + 1**을 명시해서 커밋하는 것입니다.

```java
// offset 3까지 처리했다면 4를 커밋합니다.
consumer.commitSync(Map.of(
    new TopicPartition("orders", 0),
    new OffsetAndMetadata(3 + 1)));
```

여러 파티션의 레코드가 한 배치에 섞여 오므로, 파티션마다 따로 '어디까지 끝났는지'를 추적해야 합니다. 이 과정의 `commitOne`은 레코드 하나를 처리할 때마다 그 레코드의 파티션에 대해서만 다음 위치를 커밋해 이 문제를 가장 단순하게 피합니다.
"""),
        ('전달 보장은 커밋 위치로 정해집니다', r"""
지금까지의 내용을 정리하면, Consumer 쪽의 전달 보장은 **커밋과 처리의 순서**로 결정됩니다.

- **at-most-once:** 커밋을 먼저 합니다. 중복은 없지만 장애 시 누락될 수 있습니다.
- **at-least-once:** 처리를 먼저 합니다. 누락은 없지만 장애 시 중복될 수 있습니다.
- **exactly-once:** 처리 결과와 커밋이 **한꺼번에 성공하거나 한꺼번에 실패**해야 합니다.

exactly-once는 커밋 순서만 바꿔서는 얻을 수 없습니다. 처리 결과가 Kafka 토픽이라면 Kafka 트랜잭션으로 출력과 입력 offset을 함께 커밋할 수 있고(09단원), 처리 결과가 DB라면 at-least-once에 멱등 처리를 더해 결과적으로 한 번만 반영되게 만드는 방법을 많이 씁니다(06단원). 문자 발송 같은 외부 부작용은 Kafka가 보장할 수 있는 범위 밖에 있으므로, 수신 쪽 API의 멱등 키 같은 별도 장치가 필요합니다.

자동 커밋을 껐다고(`enable.auto.commit=false`) at-least-once가 저절로 완성되지도 않습니다. 처리에 실패한 레코드를 건너뛰고 다음 레코드를 커밋해 버리면 결국 누락입니다. 실패했을 때 종료할지, `seek()`로 되돌아갈지, 재시도하거나 DLT로 보낼지(07단원)까지 정해야 보장이 완성됩니다.
"""),
    ],
    example=r"""
알림 Consumer가 주문 이벤트를 읽고 고객에게 문자를 보낸다고 해 봅시다. 문자 발송 API가 성공을 돌려준 직후, 커밋하기 전에 서버가 재부팅되었습니다. 재시작한 Consumer는 커밋된 위치부터 읽으므로 같은 주문 이벤트를 다시 받고, 고객은 같은 문자를 두 번 받습니다.

순서를 바꿔 커밋을 먼저 했다면 이번에는 문자가 아예 가지 않았을 수 있습니다. Kafka는 문자가 실제로 발송되었는지 모르므로, 어느 쪽이 덜 나쁜지 업무 기준으로 정해야 합니다. 주문 알림이라면 보통 '한 번 더 가는 것'이 '안 가는 것'보다 낫기 때문에 처리 후 커밋을 택하고, 문자 API에 주문 번호를 멱등 키로 넘겨 중복 발송을 막는 식으로 보완합니다.
""",
    scenario=dict(
        title='시간순으로 따라가기: crash-after 실험에서 일어나는 일',
        intro='`commit crash-after` 실험을 같은 그룹으로 두 번 실행할 때 파티션 0의 위치 변화를 정리합니다. offset 값은 설명을 위한 예시입니다.',
        columns=['시점', '일어나는 일', 'position', 'committed offset'],
        rows=[
            ['T1', 'Consumer가 그룹에 참여해 파티션을 배정받습니다. 커밋 기록이 없어 `earliest` 규칙으로 가장 오래된 위치부터 읽습니다.', '0', '없음'],
            ['T2', '`poll()`이 offset 0~9 레코드를 돌려줍니다.', '10', '없음'],
            ['T3', 'offset 0을 처리하고 `PROCESSED ... offset=0`을 출력합니다.', '10', '없음'],
            ['T4', '커밋 직전에 `Runtime.halt(17)`로 프로세스가 강제 종료됩니다. position은 메모리와 함께 사라집니다.', '(사라짐)', '없음'],
            ['T5', '같은 그룹으로 재시작합니다. 여전히 커밋 기록이 없으므로 다시 offset 0부터 읽습니다.', '0', '없음'],
            ['T6', 'offset 0을 **두 번째로** 처리합니다. 이번에는 정상 모드라 처리 후 커밋합니다.', '10', '1'],
            ['T7', '`max=1`에 도달해 종료합니다. 다음 실행은 offset 1부터 이어 읽습니다.', '(종료)', '1'],
        ],
        note='이미 커밋 기록이 있는 그룹이라면 T1과 T5의 시작 위치는 0이 아니라 마지막 committed offset입니다.',
    ),
    tables=[
        dict(
            title='세 가지 전달 보장 비교',
            columns=['구분', '커밋 위치', '장애 시 결과', '어울리는 경우'],
            rows=[
                ['at-most-once', '처리 **전**에 커밋', '누락 가능, 중복 없음', '일부 유실을 허용하는 샘플링 지표, 최신 값만 의미 있는 상태 알림'],
                ['at-least-once', '처리 **후**에 커밋', '중복 가능, 누락 없음', '대부분의 업무 처리. 멱등 처리와 함께 씁니다.'],
                ['exactly-once', '처리 결과와 offset을 **원자적으로** 커밋', '결과가 한 번만 반영', 'Kafka→Kafka 처리(트랜잭션), DB 결과 + 멱등 처리 또는 offset 동시 저장'],
            ],
            note='exactly-once는 보장 범위를 함께 말해야 합니다. Kafka 트랜잭션은 외부 API 호출이나 별도 DB 커밋까지 자동으로 포함하지 않습니다(09단원).',
        ),
        dict(
            title='커밋 방식 비교',
            intro='자동 커밋과 수동 커밋은 동작이 다르고, 수동 커밋도 호출 방법에 따라 결과가 달라집니다.',
            columns=['방식', '동작', '장점', '주의할 점'],
            rows=[
                ['자동 커밋 (`enable.auto.commit=true`)', '`auto.commit.interval.ms` 간격으로 이전 `poll()`이 돌려준 위치까지 커밋', '코드가 단순합니다.', '처리를 다른 스레드에 넘기면 처리 전에 커밋될 수 있습니다.'],
                ['`commitSync()` (인자 없음)', '마지막 `poll()`이 돌려준 위치까지 동기 커밋', '결과를 바로 알 수 있습니다.', '배치를 일부만 처리했을 때 쓰면 누락이 생깁니다.'],
                ['`commitSync(Map)`', '지정한 파티션별 offset을 동기 커밋', '처리한 범위만 정확히 저장합니다.', '호출마다 브로커 응답을 기다립니다.'],
                ['`commitAsync()`', '응답을 기다리지 않고 커밋 요청, 결과는 callback으로', '처리 흐름을 막지 않습니다.', '실패를 callback에서 다뤄야 하며, 무작정 재시도하면 오래된 값으로 덮어쓸 수 있습니다.'],
            ],
            note='Kafka 기본값은 `enable.auto.commit=true`, `auto.commit.interval.ms=5000`입니다. 이 실습은 실패 구간을 드러내기 위해 자동 커밋을 끄고 레코드마다 `commitSync(Map)`을 호출합니다.',
        ),
    ],
    deep=[
        ('자동 커밋은 어떻게 동작하고, 언제 위험한가요?', r"""
Kafka Consumer의 기본값은 `enable.auto.commit=true`, `auto.commit.interval.ms=5000`(5초)입니다. 자동 커밋은 정해진 간격마다 **이전 `poll()`들이 돌려준 위치까지**를 커밋합니다. 이 실습이 쓰는 classic Consumer는 이 일을 별도 타이머가 아니라 `poll()`과 `close()` 흐름 안에서 처리합니다.

그래서 자동 커밋도 조건을 지키면 at-least-once가 됩니다. 조건은 '다음 `poll()`을 부르기 전에 이전 `poll()`이 돌려준 레코드를 모두 처리한다'는 것입니다. KafkaConsumer 문서도 같은 조건을 설명합니다.

문제는 이 조건이 깨질 때입니다. 처리 속도를 높이려고 레코드를 다른 스레드나 메모리 큐에 넘기고 곧바로 다음 `poll()`을 부르면, 자동 커밋은 아직 처리되지 않은 레코드의 위치까지 저장합니다. 그 상태에서 프로세스가 죽으면 큐에 남아 있던 레코드는 누락됩니다. 처리를 비동기로 넘기는 구조라면 자동 커밋을 끄고, 처리가 끝난 위치만 직접 커밋해야 합니다.

이 실습은 `enable.auto.commit=false`로 자동 커밋을 끄고 커밋 시점을 코드에 드러냅니다.
"""),
        ('commitSync와 commitAsync, 그리고 종료·리밸런싱 시점의 커밋', r"""
`commitSync`는 브로커가 커밋을 확인할 때까지 기다리고, 실패하면 예외를 던집니다. 확실하지만 그동안 처리가 멈춥니다. `commitAsync`는 요청만 보내고 바로 다음 일을 하며, 결과는 callback으로 받습니다.

`commitAsync`를 쓸 때 주의할 점은 **실패한 커밋을 callback에서 그대로 재시도하는 것**입니다. offset 100 커밋이 일시적으로 실패해 재시도하는 사이에 offset 200 커밋이 먼저 성공했다면, 뒤늦게 성공한 재시도가 committed offset을 100으로 되돌려 놓을 수 있습니다. 그러면 재시작 시 100~199를 다시 처리합니다. 그래서 흔히 쓰는 조합은 다음과 같습니다.

- **처리 루프 안에서는 `commitAsync`:** 흐름을 막지 않고, 실패해도 다음 커밋이 더 최신 값으로 덮어쓰므로 재시도하지 않습니다.
- **종료 직전에는 `commitSync`:** 마지막 위치만큼은 확실히 저장하고 끝냅니다.

리밸런싱도 커밋 시점입니다. 파티션을 다른 Consumer에게 넘겨주기 직전에 호출되는 `ConsumerRebalanceListener.onPartitionsRevoked()`에서 처리 완료 위치를 커밋하면, 새 담당자가 불필요하게 많은 레코드를 다시 처리하지 않습니다. 이 과정의 `Settings.subscribe`는 이 콜백에서 `REVOKED` 로그만 출력하며, 레코드마다 이미 커밋하므로 추가 커밋은 하지 않습니다.
"""),
        ('커밋 위치를 되돌리거나 건너뛰기: seek와 --reset-offsets', r"""
버그를 고친 뒤 특정 시점부터 다시 처리하거나, 독이 든 레코드 하나를 건너뛰어야 할 때가 있습니다. 방법은 두 가지입니다.

- **코드에서 `consumer.seek(partition, offset)`:** 다음 `poll()`이 읽을 position을 바꿉니다. position만 바뀌므로, 커밋해야 committed offset에도 반영됩니다.
- **CLI의 `kafka-consumer-groups --reset-offsets`:** 그룹의 committed offset 자체를 바꿉니다. 그룹에 실행 중인 Consumer가 없어야(inactive) 적용됩니다.

```powershell
# 실제로 바꾸지 않고 결과만 미리 봅니다(--dry-run).
.\실습\study.ps1 groups --group commit-after-demo --topic orders --reset-offsets --to-earliest --dry-run
# 확인했다면 --execute로 적용합니다.
.\실습\study.ps1 groups --group commit-after-demo --topic orders --reset-offsets --to-earliest --execute
```

`--to-earliest`, `--to-latest` 외에 `--to-datetime`(특정 시각), `--shift-by -10`(10건 뒤로), `--to-offset` 같은 기준을 쓸 수 있습니다. 운영에서는 반드시 `--dry-run`으로 바뀔 위치를 먼저 확인합니다. 명령 사용법은 부록 A1에 더 자세히 정리했습니다.
"""),
        ('커밋 기록에도 보관 기한이 있습니다', r"""
committed offset은 영원히 남지 않습니다. 그룹에 Consumer가 하나도 없는 상태(empty)가 되면, 브로커 설정 `offsets.retention.minutes`가 지난 뒤 그 그룹의 커밋 기록이 삭제됩니다. 이 설정의 기본값은 10080분, 즉 7일입니다.

기록이 지워진 그룹이 다시 시작하면 '커밋 기록이 없는 그룹'이 되므로 `auto.offset.reset` 규칙이 다시 적용됩니다. Kafka 기본값 `latest`라면 그동안 쌓인 레코드를 모두 건너뛰고 최신 위치부터 읽습니다. 반대로 `earliest`라면 보관 중인 가장 오래된 레코드부터 전부 다시 처리합니다. 2주 동안 멈춰 있던 배치 Consumer를 다시 켰더니 데이터가 통째로 빠지거나 중복되는 사고는 대개 이 동작 때문입니다.

`OffsetAndMetadata`에 함께 넣는 leader epoch는 커밋 시점에 그 파티션의 리더가 몇 번째 리더였는지를 나타내는 값입니다. 리더가 바뀌는 과정에서 로그 끝부분이 잘려 나간 경우, Consumer가 자신의 위치가 여전히 유효한지 확인하는 데 쓰입니다. 애플리케이션이 직접 다룰 일은 드물지만, `commitOne`이 `r.leaderEpoch()`를 함께 넘기는 이유입니다.
"""),
    ],
    summary=[
        'position은 다음에 가져올 위치(메모리), committed offset은 재시작 시 복구할 위치(Kafka 저장)입니다.',
        'committed offset은 처리한 offset이 아니라 다음에 읽을 offset이므로 offset 12를 처리했다면 13을 커밋합니다.',
        '처리 후 커밋은 at-least-once(중복 가능), 처리 전 커밋은 at-most-once(누락 가능)입니다.',
        '배치 일부만 처리했다면 인자 없는 `commitSync()` 대신 파티션별 처리 완료 위치를 명시해 커밋합니다.',
        'exactly-once는 커밋 순서가 아니라 결과와 offset의 원자적 저장 또는 멱등 처리로 얻습니다.',
    ],
    nodes=[('poll', '로컬 position 이동', 30, 140), ('업무 처리', 'DB / 외부 호출', 330, 140), ('offset commit', '다음 읽을 위치 저장', 650, 140), ('실패 구간 A', '처리 전 커밋 → 누락 가능', 30, 285), ('실패 구간 B', '처리 후 미커밋 → 재처리', 650, 285)],
    edges=[(0, 1, '처리'), (1, 2, '성공 후 커밋'), (0, 3, '잘못된 선커밋'), (1, 4, '이때 장애')],
    steps=[
        '현재 committed offset=5, 다음 처리 대상은 offset 5입니다.',
        'poll이 레코드를 반환하면 position은 앞으로 가지만 committed offset은 아직 5입니다.',
        '업무 처리를 완료합니다. 아직 커밋 전입니다.',
        '이 시점에 장애가 나면 재시작한 Consumer는 offset 5를 다시 읽습니다.',
        '커밋으로 6을 저장하면 재시작 시 offset 6부터 이어 읽습니다.',
    ],
    snippet=('공통/src/main/java/study/kafka/Settings.java', 'commit-one'),
    explain=[
        '커밋 값은 처리한 offset 자체가 아니라 `offset + 1`입니다. leader epoch와 메타데이터 문자열(`processed`)도 함께 전달합니다.',
        '`Map.of(partition, ...)`로 이 레코드가 속한 파티션만 커밋합니다. 같은 배치에 섞인 다른 파티션의 미처리 레코드 위치는 건드리지 않습니다.',
        '레코드마다 `commitSync`를 호출해 실패 구간을 명확히 보여 줍니다. 운영에서는 안전한 완료 구간을 추적하면서 묶어서 커밋해 호출 비용을 줄일 수 있습니다.',
        '`Runtime.halt(17)`은 shutdown 처리를 거치지 않는 의도된 장애입니다. exit 17과 PowerShell 오류는 이 실험의 예상 결과입니다.',
    ],
    pitfall='자동 커밋을 끄기만 해서 at-least-once가 완성되지는 않습니다. 실패한 레코드를 건너뛰고 다음 위치를 커밋하면 결국 누락입니다. 실패 시 종료할지, `seek()`로 되돌아갈지, 재시도나 DLT로 보낼지까지 정해야 합니다.',
    commands=[
        ('미커밋 장애', r'.\실습\study.ps1 run commit crash-after commit-after-demo 1', 'PROCESSED 뒤 CRASH와 exit 17이 출력됩니다.', r"""
```text
ASSIGNED [orders-0, orders-1, orders-2]
PROCESSED key=order-2 p=0 offset=0 value={"schemaVersion":1,"eventId":"day01-2",...}
CRASH after processing, before commit (will replay)
Docker command failed (exit 17). See the output above.
```

`PROCESSED`는 업무 처리의 대역인 로그 출력까지 끝났다는 뜻이고, 바로 다음 줄의 `CRASH`는 커밋 코드에 도달하기 전에 프로세스를 강제 종료했다는 표시입니다. 마지막 PowerShell 오류(`exit 17`)는 실패가 아니라 **이 실험이 의도한 결과**입니다.

이 시점에 `commit-after-demo` 그룹의 committed offset은 아무것도 저장되지 않았습니다. 출력된 `p=0 offset=0`(여러분의 값은 다를 수 있습니다)을 메모해 두세요. 다음 명령에서 같은 위치가 다시 나타나야 합니다.
"""),
        ('동일 그룹 복구', r'.\실습\study.ps1 run commit after commit-after-demo 1', '직전 PROCESSED와 같은 partition/offset이 다시 처리됩니다.', r"""
```text
ASSIGNED [orders-0, orders-1, orders-2]
PROCESSED key=order-2 p=0 offset=0 value={"schemaVersion":1,"eventId":"day01-2",...}
REVOKED [orders-0, orders-1, orders-2]
PROCESSED_COUNT 1
```

강제 종료 직후에 실행하면 `ASSIGNED`가 몇 초 늦게 나올 수 있습니다. 죽은 이전 멤버는 그룹을 떠난다는 신호를 보내지 못했으므로, 코디네이터가 세션 타임아웃(이 실습은 10초)이 지나 그 멤버를 제외한 뒤에야 재배정을 마치기 때문입니다.

직전 실행과 **같은 p/offset이 다시 처리**되었다면 at-least-once의 재처리 구간을 직접 본 것입니다. 이번에는 `after` 모드라 처리 뒤에 커밋까지 했으므로, 같은 명령을 한 번 더 실행하면 다음 offset으로 넘어갑니다.

같은 레코드가 두 번 처리되었다는 것은 실제 서비스라면 문자가 두 번 발송되거나 합계가 두 번 더해졌을 수 있다는 뜻입니다. 이 중복을 업무 결과에서 없애는 방법은 06단원에서 다룹니다.
"""),
        ('선커밋 장애', r'.\실습\study.ps1 run commit crash-before commit-before-demo 1', '처리할 partition/offset을 출력한 뒤, 업무 처리 전 exit 17로 종료합니다.', r"""
```text
ASSIGNED [orders-0, orders-1, orders-2]
CRASH before processing: p=0 offset=0 (already committed)
Docker command failed (exit 17). See the output above.
```

`(already committed)`는 커밋을 먼저 끝냈다는 표시입니다. 이 레코드에 대해서는 `PROCESSED` 줄이 출력되지 않았습니다. 즉 업무 처리는 하지 않았는데 이 그룹의 책갈피는 이미 다음 위치(이 예시에서는 offset 1)로 옮겨졌습니다. 이번에도 `p`와 `offset` 값을 메모해 두세요.
"""),
        ('누락 관찰', r'.\실습\study.ps1 run commit after commit-before-demo 0', '직전 CRASH 로그의 partition/offset이 처리되지 않는지 확인합니다.', r"""
```text
ASSIGNED [orders-0, orders-1, orders-2]
PROCESSED key=order-2 p=0 offset=1 value={...}
PROCESSED key=order-0 p=1 offset=0 value={...}
...
REVOKED [orders-0, orders-1, orders-2]
PROCESSED_COUNT 92
```

`max=0`이라 입력이 끊긴 뒤 25초까지 모든 레코드를 읽습니다. 출력에서 직전에 메모한 위치(예시의 `p=0 offset=0`)를 찾아보세요. 파티션 0은 offset 1부터 시작하고 0은 끝내 나타나지 않습니다. 레코드는 Kafka에 그대로 남아 있지만 이 그룹은 다시 읽지 않습니다. 이것이 at-most-once의 누락입니다.

개수로도 확인할 수 있습니다. 01~04단원 명령만 실행했다면 `orders`에는 93건(01단원 3건, 02단원 12건, 03단원 60건, 04단원 18건)이 있으므로, `PROCESSED_COUNT`는 그보다 1건 적은 92가 됩니다. 모자란 1건이 바로 선커밋 뒤 처리하지 못한 레코드입니다.

누락은 오류 없이 조용히 일어난다는 점을 눈여겨보세요. 같은 레코드를 다시 처리하고 싶다면 그룹을 멈춘 상태에서 `--reset-offsets`로 위치를 되돌리거나 새 그룹으로 읽어야 합니다.
"""),
    ],
    experiment=[
        '실험용 새 group 이름으로 각 시나리오를 다시 실행하고, 매번 CRASH 줄의 p/offset과 다음 실행의 첫 PROCESSED 줄을 비교하세요.',
        r'누락 실험 뒤 `.\실습\study.ps1 groups --group commit-before-demo --topic orders --reset-offsets --to-earliest --dry-run`으로 되돌릴 위치를 미리 보고, `--execute`로 적용한 뒤 누락된 레코드가 다시 처리되는지 확인하세요.',
        r'`.\실습\study.ps1 groups --describe --group commit-after-demo`로 파티션별 CURRENT-OFFSET이 마지막 처리 offset + 1인지 확인하세요.',
        'CommitLab의 로그는 업무 처리의 대역입니다. 실제 DB 쓰기와 커밋 사이의 실패는 다음 실습에서 관찰합니다.',
    ],
    logcase=dict(
        title='로그 저장 Consumer는 저장소 적재가 끝난 뒤에 커밋합니다',
        body=r"""
로그 수집 파이프라인의 저장 Consumer는 보통 레코드를 한 건씩 저장하지 않고, `poll()`로 받은 수백~수천 건을 모아 로그 저장소에 **한 번에 적재(batch insert)**합니다. 저장소 입장에서는 작은 INSERT 여러 번보다 큰 INSERT 한 번이 훨씬 효율적이기 때문입니다. 이때 커밋 위치는 이 단원의 원칙을 그대로 따릅니다.

- **적재 성공 후 커밋:** 적재가 끝난 뒤 그 배치의 파티션별 마지막 offset + 1을 커밋합니다. 적재와 커밋 사이에 장애가 나면 그 배치 전체가 한 번 더 적재되므로, 최악의 중복 범위는 배치 크기만큼입니다.
- **커밋 후 적재는 금지:** 적재 전에 커밋하면 저장소가 일시적으로 거절했을 때 그 배치를 영영 잃습니다. 로그는 나중에 장애 분석의 증거가 되므로 조용한 누락이 가장 나쁩니다.
- **자동 커밋과 내부 버퍼 조합 주의:** '메모리에 쌓아 두었다가 5초마다 적재'하는 구조에서 자동 커밋을 켜 두면, 아직 버퍼에 있는 로그의 위치까지 커밋될 수 있습니다. 이 구조라면 자동 커밋을 끄고 적재 완료 위치만 커밋합니다.
- **느린 적재와 리밸런싱:** 적재가 오래 걸려 `max.poll.interval.ms`를 넘기면 Consumer가 그룹에서 빠지고, 커밋도 실패합니다. 배치 크기를 저장소가 감당할 수 있는 시간 안으로 조절합니다.

결국 로그 저장은 at-least-once로 설계하고, 생기는 중복은 06단원의 방법(로그 ID 기반 중복 제거)으로 저장소에서 정리하는 구성이 가장 흔합니다.
""",
    ),
    faq=[
        ('레코드마다 커밋하면 느리지 않나요?', r"""
느릴 수 있습니다. 커밋할 때마다 브로커와 통신하기 때문입니다. 이 실습은 실패 구간을 눈으로 보기 위해 레코드마다 `commitSync`를 호출합니다. 운영에서는 배치를 처리한 뒤 한 번 커밋하거나, 루프 안에서는 `commitAsync`를 쓰고 종료할 때만 `commitSync`를 쓰는 식으로 횟수를 줄입니다. 다만 커밋 간격을 늘릴수록 장애 시 다시 처리되는 레코드도 늘어난다는 점을 함께 고려합니다.
"""),
        ('커밋이 실패하면 어떻게 되나요?', r"""
`commitSync`는 예외를 던집니다. 대표적인 경우가 `CommitFailedException`으로, 처리가 너무 오래 걸려 `max.poll.interval.ms`를 넘기는 바람에 그룹에서 제외되었고 그 사이 파티션이 다른 Consumer에게 넘어간 상황입니다. 이 경우 커밋하지 못한 레코드는 새 담당자가 다시 처리하므로, 결과적으로 중복이 생길 수 있습니다. 처리 시간을 줄이거나 `max.poll.records`를 낮추는 것이 근본적인 대응입니다.
"""),
        ('처리 전 커밋(at-most-once)은 언제 쓰나요?', r"""
일부 유실보다 중복이 더 해로운 경우에 고려합니다. 예를 들어 1분마다 최신 센서 값을 화면에 띄우는 기능이라면 한두 건을 놓쳐도 다음 값이 곧 오지만, 같은 경고 알림이 반복되면 사용자가 혼란스러워합니다. 주문·결제·로그 보관처럼 누락이 사고가 되는 업무에서는 쓰지 않습니다.
"""),
        ('새 그룹인데 왜 처음부터 읽나요? 기본값은 latest라고 들었는데요.', r"""
Kafka의 `auto.offset.reset` 기본값은 `latest`가 맞습니다. 이 과정의 `Settings.consumerProperties`가 학습 편의를 위해 `earliest`로 바꿔 두었기 때문에 새 그룹이 가장 오래된 레코드부터 읽습니다. 이 설정은 커밋 기록이 없는 그룹에만 적용되며, 기록이 있는 그룹은 항상 committed offset부터 읽습니다.
"""),
    ],
    quiz=('파티션의 offset 12를 성공적으로 처리했다면 커밋할 값은?', ['11', '12', '13'], 2, 'committed offset은 마지막으로 처리한 위치가 아니라 다음에 읽을 위치이므로 13입니다.'),
    sources=['consumer', 'design', 'cconfig'],
)
