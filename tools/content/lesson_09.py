"""LESSON 09 · 트랜잭션 · Exactly Once의 범위: 이론 페이지와 실습 안내의 원본입니다."""

LESSON = dict(
    id='09',
    level='고급',
    title='Kafka 안에서 함께 성공시키기',
    short='트랜잭션 · Exactly Once의 범위',
    minutes=80,
    desc='출력 이벤트와 입력 offset을 하나의 Kafka 트랜잭션으로 묶고, Exactly Once가 어디까지 성립하는지 경계를 긋습니다.',
    goals=[
        'commit/abort와 read_committed를 비교한다',
        '입력 offset과 출력의 원자성을 구현한다',
        'Exactly Once를 외부 DB까지 일반화하지 않는다',
        '트랜잭션 코디네이터 · 제어 레코드 · LSO가 가시성을 정하는 방식을 설명한다',
    ],
    why=r"""
05~06단원에서는 '처리 후 커밋'의 재처리 구간을 DB 멱등성으로 막았습니다. 그런데 처리 결과가 DB가 아니라 **또 다른 Kafka 토픽**이라면 어떨까요? `orders`를 읽어 가공한 결과를 `orders.tx`에 쓰는 파이프라인에서, 결과를 보낸 직후 입력 offset을 커밋하기 전에 프로세스가 죽으면 재시작 후 같은 입력을 다시 처리해 **결과 토픽에 중복 레코드**가 생깁니다. 순서를 바꿔 offset을 먼저 커밋하면 이번에는 결과가 빠질 수 있습니다.

Kafka 트랜잭션은 '출력 레코드 기록'과 '입력 offset 커밋'을 한 번에 확정하거나 한 번에 취소해 이 틈을 Kafka 안에서 닫습니다. 다만 'Exactly Once'라는 말은 쉽게 과장됩니다. 이 단원의 목표는 트랜잭션을 쓰는 방법만큼이나, 이 보장이 **어디서 끝나는지**를 정확히 말할 수 있게 되는 것입니다.
""",
    analogy=('커튼 뒤 선반과 확정 도장', r"""
택배 분류 센터의 작업자를 떠올려 보세요. 작업자는 입고 선반에서 상자를 꺼내 송장을 새로 붙인 뒤 출고 선반에 올립니다. 이 센터에는 규칙이 하나 있습니다. 출고 선반 앞에는 **커튼**이 쳐져 있어서, 작업자가 올린 상자는 관리자가 '확정' 도장을 찍기 전까지 배송 기사에게 보이지 않습니다.

작업자는 한 묶음을 다 옮긴 뒤 관리자에게 "이 묶음 확정해 주세요"라고 요청합니다. 관리자는 출고 상자들과 '입고 선반은 여기까지 처리함'이라는 책갈피를 **한꺼번에** 확정합니다. 도중에 문제가 생기면 '취소' 도장을 찍습니다. 취소된 상자는 선반에서 치우지 않고 그대로 두되 무효 표시가 붙어, 규칙을 지키는 기사(read_committed)는 집어 가지 않습니다. 다만 표시를 무시하는 기사(read_uncommitted)는 무효 상자까지 들고 갈 수 있습니다.

이 비유에서 커튼은 LSO(Last Stable Offset), 도장은 트랜잭션 마커, 관리자는 트랜잭션 코디네이터에 해당합니다. 실제 Kafka에서는 커튼이 파티션 단위로 동작하므로, 확정되지 않은 묶음 하나가 같은 파티션의 뒤쪽 레코드까지 가린다는 점이 비유와 다릅니다.
"""),
    concept=[
        ('여러 쓰기의 원자성을 제공합니다', r"""
트랜잭션 Producer는 여러 토픽·파티션에 대한 쓰기를 하나로 묶어 **함께 commit하거나 함께 abort**합니다. 기본 흐름은 짧습니다.

```java
p.beginTransaction();
p.send(new ProducerRecord<>("orders.tx", "order-tx", json)).get();
if (mode.equals("abort")) p.abortTransaction();
else p.commitTransaction();
```

여기서 놓치기 쉬운 사실이 있습니다. `send(...).get()`이 성공했다면 레코드는 **이미 브로커의 로그에 기록된 상태**입니다. 트랜잭션은 레코드를 숨겨 두었다가 나중에 쓰는 장치가 아니라, 이미 쓰인 레코드에 '확정' 또는 '취소' 표식을 남기는 장치입니다. abort된 레코드도 로그에서 지워지지 않고 보관 기간이 지날 때까지 남습니다.

그래서 읽는 쪽의 설정이 결과를 좌우합니다. Consumer의 `isolation.level`이 `read_committed`이면 abort된 레코드와 아직 확정되지 않은 레코드를 업무 데이터로 받지 않습니다. 반면 **Kafka 기본값은 `read_uncommitted`**이라서, 별도로 설정하지 않은 Consumer는 abort된 레코드까지 그대로 읽습니다. 트랜잭션으로 쓴 토픽을 읽는 모든 Consumer에 `read_committed`를 명시해야 보장이 완성됩니다.
"""),
        ('읽기 → 처리 → 쓰기를 한 트랜잭션으로 묶습니다', r"""
Kafka에서 읽어 Kafka로 쓰는 처리를 **consume-transform-produce** 패턴이라고 부릅니다. 핵심은 결과 레코드와 함께 **입력 Consumer Group의 다음 offset도 같은 트랜잭션에 넣는 것**입니다.

```java
producer.sendOffsetsToTransaction(records.nextOffsets(), consumer.groupMetadata());
producer.commitTransaction();
```

`sendOffsetsToTransaction`은 offset 커밋을 Consumer가 직접 하지 않고 Producer의 트랜잭션에 맡깁니다. commit되는 순간 결과 레코드의 확정과 `__consumer_offsets`의 offset 갱신이 함께 일어나고, abort되면 둘 다 없던 일이 됩니다. 결과는 기록되었는데 입력 위치는 그대로인 상황(중복 출력), 또는 입력 위치는 넘어갔는데 결과가 없는 상황(유실)이 Kafka 안에서는 생기지 않습니다.

이 패턴을 쓸 때는 두 가지를 지킵니다.

- **Consumer가 따로 커밋하지 않습니다:** `enable.auto.commit=false`로 두고 `commitSync()`를 섞지 않습니다. offset은 오직 트랜잭션으로만 움직여야 합니다.
- **`groupMetadata()`를 함께 넘깁니다:** 그룹의 세대(generation) 정보가 포함되어, 리밸런싱으로 파티션을 빼앗긴 옛 Consumer가 뒤늦게 offset을 커밋하려 하면 거부됩니다.
"""),
        ('transactional.id와 epoch가 좀비를 막습니다', r"""
트랜잭션 Producer는 `transactional.id`로 자신을 식별합니다. `initTransactions()`를 호출하면 코디네이터가 이 ID에 대한 **epoch(세대 번호)**를 하나 올리고, 같은 ID로 열려 있던 이전 트랜잭션을 정리합니다. 그 뒤 옛 epoch를 가진 Producer가 쓰기를 시도하면 `ProducerFencedException`으로 거부됩니다. 네트워크가 끊겨 죽은 줄 알았던 인스턴스(좀비)가 되살아나 중복 결과를 쓰는 사고를 이렇게 막습니다.

ID를 정하는 규칙은 두 가지입니다.

- **동시에 실행되는 인스턴스끼리는 ID가 달라야 합니다:** 같은 ID로 두 프로세스를 띄우면 나중에 뜬 쪽의 `initTransactions()`가 먼저 뜬 쪽을 밀어내(fencing), 먼저 뜬 쪽은 더 이상 쓸 수 없게 됩니다. 이 실습은 기본 ID `course-tx-processor-1`을 쓰므로, 여러 개를 동시에 돌리려면 `TX_ID` 환경 변수로 인스턴스별 ID를 줘야 합니다.
- **같은 논리적 인스턴스의 재시작에는 같은 ID를 씁니다:** 그래야 재시작한 인스턴스가 이전 세션의 미완료 트랜잭션을 정리하고 이어받습니다. 실행할 때마다 무작위 ID를 쓰면 fencing의 의미가 사라집니다.

`commitTransaction()`이 타임아웃으로 끝나면 커밋이 실제로 되었는지 알 수 없습니다. 이때 무조건 abort하면 이미 확정된 결과를 부정하는 꼴이 될 수 있으므로, 이 예제는 abort 처리 블록 밖에서 commit을 호출하고 예외를 상위로 올려 종료합니다. `ProducerFencedException` 같은 치명적 오류가 나면 그 Producer는 다시 쓰지 않고 닫습니다.
"""),
        ('Exactly Once의 경계를 말할 수 있어야 합니다', r"""
Kafka 설계 문서는 Exactly Once를 Kafka Streams와, 트랜잭션 Producer와 `read_committed` Consumer의 조합으로 달성한다고 설명하며, 외부 시스템은 그 시스템의 협력이 필요하다고 덧붙입니다. 즉 이 보장은 **'Kafka 입력 offset'과 'Kafka 출력'을 묶는 것**입니다.

트랜잭션 안에서 실행되더라도 다음 동작은 보장 범위 밖입니다.

- **외부 DB 쓰기:** Kafka 트랜잭션이 abort되어도 이미 커밋된 DB 변경은 되돌아가지 않습니다. DB 쪽은 06단원의 멱등 처리나 10단원의 Outbox로 다룹니다.
- **HTTP 호출과 문자 발송:** 재처리 때 다시 호출될 수 있습니다. 외부 API가 멱등 키를 지원해야 안전합니다.
- **애플리케이션의 중복 발행:** 같은 업무 이벤트를 앱이 send로 두 번 보내면 트랜잭션은 둘 다 정상 레코드로 확정합니다. 업무 중복은 eventId로 따로 걸러야 합니다.

'정확히 한 번 실행된다'가 아니라 '재시도가 일어나도 Kafka 안의 결과는 한 번 처리한 것과 같아진다'가 정확한 표현입니다. 처리 코드 자체는 재시작 때 여러 번 실행될 수 있습니다.
"""),
        ('Abort 뒤의 위치와 열린 트랜잭션의 비용', r"""
`abortTransaction()`은 Consumer의 **로컬 position을 되돌리지 않습니다.** 방금 poll한 배치를 abort했는데 그대로 다음 poll을 호출하면, Consumer는 abort된 입력을 건너뛰고 다음 레코드부터 읽습니다. 계속 실행하려면 마지막으로 커밋된 offset으로 `seek`하거나 Consumer를 다시 만들어야 합니다. 이 실습 코드는 처리 실패 시 abort하고 **프로세스를 종료**하는 가장 단순한 방법을 택했습니다. 재시작하면 커밋된 위치부터 다시 읽습니다.

열린 트랜잭션은 다른 Consumer에게도 영향을 줍니다. `read_committed` Consumer는 파티션의 **LSO(Last Stable Offset)**, 즉 아직 결론이 나지 않은 가장 앞쪽 트랜잭션의 시작 위치까지만 읽습니다. 트랜잭션 하나가 오래 열려 있으면 그 뒤에 쌓인 레코드는 다른 Producer가 쓴 확정 레코드라도 기다려야 합니다. 그래서 트랜잭션은 짧게 유지하고, 오래 걸리는 외부 호출을 트랜잭션 안에 넣지 않습니다.
"""),
    ],
    example=r"""
`orders`에서 주문 이벤트를 읽어 검증한 뒤 `orders.tx`로 전달하는 처리기가 있다고 해 봅시다. 처리기는 10건을 poll해 결과 10건을 보내고, 입력 offset 50을 트랜잭션에 넣은 다음 commit합니다.

결과 7건을 보낸 시점에 프로세스가 강제 종료되면 어떻게 될까요? 이미 기록된 7건은 확정되지 않은 채 로그에 남고, 입력 offset은 40 그대로입니다. 재시작한 처리기가 같은 `transactional.id`로 `initTransactions()`를 호출하면 이전 트랜잭션이 abort되고, 처리기는 offset 40부터 다시 10건을 처리해 새 결과를 확정합니다. `read_committed`로 읽는 하류 서비스는 abort된 7건을 보지 않으므로 결과를 정확히 10건으로 받습니다.

반면 이 처리기 안에 외부 결제 API 호출을 넣었다면, 재처리 과정에서 결제 API는 두 번 호출됩니다. Kafka 트랜잭션은 그 호출을 되돌려 주지 않습니다.
""",
    scenario=dict(
        title='시간순으로 따라가기: 트랜잭션 한 번이 확정되기까지',
        intro='`tx-process`가 배치 하나를 처리하는 과정입니다. offset과 시각은 설명을 위한 예시이며, 아래쪽 두 줄은 commit 대신 장애가 난 경우입니다.',
        columns=['시점', '일어나는 일', 'read_committed Consumer가 보는 것', '입력 그룹의 committed offset'],
        rows=[
            ['T0', '`orders`에서 10건을 poll합니다(offset 40~49).', '변화 없음', '40'],
            ['T1', '`beginTransaction()` 후 첫 send에서 코디네이터가 `orders.tx` 파티션을 트랜잭션에 등록합니다.', '변화 없음', '40'],
            ['T2', '결과 10건이 `orders.tx` 로그에 기록됩니다.', '보이지 않습니다. 이 레코드들은 LSO 뒤에 있습니다.', '40'],
            ['T3', '`sendOffsetsToTransaction`으로 다음 offset 50을 트랜잭션에 넣습니다.', '변화 없음', '40 (50은 아직 대기 중)'],
            ['T4', '`commitTransaction()`: 코디네이터가 결정을 `__transaction_state`에 남기고 각 파티션에 COMMIT 마커를 씁니다.', '결과 10건이 보이기 시작합니다.', '50'],
            ['T4′', '(장애) T2와 T3 사이에 프로세스가 종료됩니다.', '여전히 보이지 않습니다.', '40'],
            ['T5′', '재시작한 인스턴스가 같은 ID로 `initTransactions()`를 호출하면 이전 트랜잭션이 abort됩니다. 재시작이 없으면 `transaction.timeout.ms`가 지난 뒤 코디네이터가 abort합니다.', 'ABORT 마커가 쓰여 LSO가 앞으로 이동합니다. abort된 결과는 끝까지 보이지 않습니다.', '40 → offset 40부터 다시 처리'],
        ],
        note='T4가 끝나기 전에는 결과와 입력 위치 중 어느 것도 확정되지 않습니다. 이 성질이 Kafka 안에서의 원자성입니다.',
    ),
    tables=[
        dict(
            title='격리 수준 비교: read_uncommitted와 read_committed',
            columns=['항목', 'read_uncommitted', 'read_committed'],
            rows=[
                ['Kafka 기본값', '예 (`isolation.level`의 기본값)', '아니요. 명시적으로 설정해야 합니다.'],
                ['확정 전 트랜잭션 레코드', '기록되는 즉시 읽습니다.', 'commit될 때까지 기다립니다.'],
                ['abort된 레코드', '읽습니다.', '건너뜁니다.'],
                ['트랜잭션을 쓰지 않은 일반 레코드', '읽습니다.', '읽습니다. 단, 앞에 열린 트랜잭션이 있으면 LSO에 막혀 늦게 보일 수 있습니다.'],
                ['읽을 수 있는 끝 위치', 'High Watermark(복제가 확인된 위치)', 'LSO(가장 앞쪽 미결 트랜잭션의 시작 위치)'],
                ['제어 레코드(트랜잭션 마커)', '반환하지 않습니다.', '반환하지 않습니다.'],
                ['이 실습에서 쓰는 곳', '격리 수준 비교 A', '격리 수준 비교 B, `tx-process`, `commit`, `dedup`, `retry`'],
            ],
            note='High Watermark는 08단원에서 다룹니다. 트랜잭션을 쓰지 않는 토픽에서는 두 설정의 결과가 같습니다.',
        ),
        dict(
            title='Exactly Once가 보장하는 것과 보장하지 않는 것',
            columns=['상황', 'Kafka 트랜잭션으로 보장되나요?', '보장되지 않으면 무엇으로 다루나요?'],
            rows=[
                ['Kafka 입력 offset과 Kafka 출력의 동시 확정', '예', '추가 장치가 필요 없습니다.'],
                ['여러 토픽·파티션에 대한 출력을 함께 확정', '예', '추가 장치가 필요 없습니다.'],
                ['Kafka Streams의 `exactly_once_v2` 처리', '예 (Kafka 안의 입력·상태·출력)', '추가 장치가 필요 없습니다.'],
                ['외부 DB에 쓰기', '아니요', '06단원의 멱등 처리, 10단원의 Outbox'],
                ['HTTP 호출, 문자·메일 발송', '아니요', '외부 API의 멱등 키, 발송 이력 저장'],
                ['같은 업무 이벤트를 앱이 두 번 send', '아니요 (둘 다 정상 확정)', 'eventId 기반 중복 제거'],
                ['`read_uncommitted`로 읽는 하류 Consumer', '아니요 (abort된 레코드도 보임)', '하류 Consumer에 `read_committed` 설정'],
            ],
        ),
    ],
    deep=[
        ('트랜잭션 코디네이터와 __transaction_state', r"""
트랜잭션의 상태를 관리하는 쪽은 브로커 안의 **트랜잭션 코디네이터**입니다. `transactional.id`마다 담당 코디네이터가 정해지고, 코디네이터는 트랜잭션의 진행 상태(시작, 참여 파티션, 커밋 준비, 완료)를 내부 토픽 `__transaction_state`에 기록합니다. 코디네이터를 맡은 브로커가 죽어도 다른 브로커가 이 토픽을 읽어 상태를 이어받습니다.

이 내부 토픽이 안전하려면 복제가 필요합니다. Producer 설정 문서는 트랜잭션이 기본적으로 **브로커 3대 이상**을 요구하며, 개발 환경에서는 `transaction.state.log.replication.factor`를 낮출 수 있다고 안내합니다. 브로커 기본값도 `transaction.state.log.replication.factor=3`, `transaction.state.log.min.isr=2`입니다. 이 실습의 단일 환경이 `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1`과 `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1`로 낮추는 이유가 여기에 있고, 3노드 환경은 기본값과 같은 3과 2를 씁니다.

`transactional.id`를 설정하면 멱등 Producer(`enable.idempotence`)도 자동으로 켜집니다. 멱등성은 한 세션 안에서 재시도로 생기는 중복 기록을 막고, 트랜잭션은 그 위에 여러 쓰기의 원자성과 세션을 넘는 fencing을 더한다고 정리할 수 있습니다.
"""),
        ('제어 레코드 때문에 offset에 빈 번호가 생깁니다', r"""
commit이나 abort가 결정되면 코디네이터는 트랜잭션에 참여한 모든 파티션에 **트랜잭션 마커**를 씁니다. 마커는 업무 데이터가 아닌 **제어 레코드(control record)**이며, Consumer API는 이를 애플리케이션에 돌려주지 않습니다.

하지만 제어 레코드도 로그에 기록되므로 **offset 하나를 차지합니다.** 그래서 트랜잭션으로 쓴 토픽을 읽으면 offset이 0, 2, 4처럼 건너뛰어 보일 수 있습니다. 이는 유실이 아니라 정상입니다. 이 성질은 12단원의 Lag 계산에도 영향을 줍니다. 'log end offset − committed offset'으로 구한 Lag 숫자에는 마커와 abort된 레코드가 섞여 있어, 실제로 처리할 업무 메시지 건수와 다를 수 있습니다.
"""),
        ('transaction.timeout.ms: 열린 채 방치된 트랜잭션의 정리', r"""
Producer가 트랜잭션을 시작한 뒤 commit도 abort도 하지 못하고 죽으면, 그 트랜잭션은 열린 채 남아 LSO를 붙잡습니다. 코디네이터는 Producer 설정 `transaction.timeout.ms`(Kafka 기본값 60000ms, 1분)보다 오래 열려 있는 트랜잭션을 스스로 abort해 이 상태를 풉니다. 이 타이머는 트랜잭션에 첫 파티션이 추가될 때 시작됩니다.

값이 브로커 설정 `transaction.max.timeout.ms`(Kafka 기본값 900000ms, 15분)보다 크면 `InvalidTxnTimeoutException`으로 요청이 거부됩니다. 이 값을 너무 크게 잡으면 장애 때 `read_committed` Consumer가 오래 멈추고, 너무 작게 잡으면 정상적인 긴 배치가 중간에 abort됩니다. 같은 `transactional.id`로 재시작하면 타임아웃을 기다리지 않고 즉시 정리되므로, 안정된 ID를 쓰는 것이 복구 시간을 줄이는 방법이기도 합니다.
"""),
        ('트랜잭션의 비용과 커밋 주기', r"""
트랜잭션은 공짜가 아닙니다. 트랜잭션마다 코디네이터와의 왕복, `__transaction_state` 기록, 참여 파티션마다의 마커 쓰기가 따릅니다. 레코드 1건마다 트랜잭션을 열고 닫으면 이 고정 비용이 처리량을 크게 깎습니다. 그래서 보통은 poll한 배치 단위로 트랜잭션을 묶습니다. 이 실습의 `tx-process`도 poll 한 번(최대 `max.poll.records`=10건, Kafka 기본값은 500건)을 트랜잭션 하나로 처리합니다.

배치를 키우면 처리량은 오르지만, 하류의 `read_committed` Consumer가 결과를 보기까지의 지연도 함께 늘어납니다. 결과는 commit 시점에 한꺼번에 보이기 때문입니다.

Kafka Streams는 `processing.guarantee=exactly_once_v2`로 이 모든 과정을 대신 처리합니다. 커밋 주기(`commit.interval.ms`)마다 입력 offset, 상태 저장소의 changelog, 출력 레코드를 하나의 트랜잭션으로 확정합니다. 11단원의 `OrderStreams`가 이 설정을 씁니다.
"""),
    ],
    summary=[
        '트랜잭션은 레코드를 숨겼다가 쓰는 장치가 아니라, 이미 기록된 레코드에 확정 또는 취소 마커를 남기는 장치입니다.',
        '`sendOffsetsToTransaction`으로 출력과 입력 offset을 함께 확정하면 Kafka 안에서 중복 출력과 유실이 사라집니다.',
        '하류 Consumer는 `isolation.level=read_committed`를 명시해야 합니다. Kafka 기본값은 `read_uncommitted`입니다.',
        '`transactional.id`는 동시 실행 인스턴스끼리 다르게, 같은 인스턴스의 재시작에는 같게 둡니다. epoch가 좀비 Producer를 막습니다.',
        'Exactly Once는 Kafka 입력과 Kafka 출력 사이의 보장이며 외부 DB, HTTP, 앱의 중복 발행은 포함하지 않습니다.',
        '열린 트랜잭션은 LSO를 붙잡아 다른 레코드의 가시성까지 늦추므로 짧게 유지합니다.',
    ],
    nodes=[('orders', '입력 레코드', 30, 140), ('Transaction', 'beginTransaction', 330, 35), ('orders.tx', '아직 미커밋인 출력', 650, 35), ('입력 offset', 'sendOffsetsToTransaction', 330, 245), ('read_committed', 'commit된 출력만 읽기', 650, 245)],
    edges=[(0, 1, 'poll'), (1, 2, 'send'), (1, 3, '같은 트랜잭션'), (3, 4, 'commit'), (2, 4, 'commit 후 가시화')],
    steps=[
        '입력 주문을 poll하고 Kafka 트랜잭션을 시작합니다.',
        'orders.tx에 결과 레코드를 씁니다. 로그에는 기록되었지만 아직 read_committed에 보이지 않습니다.',
        '입력 그룹의 다음 offset을 같은 트랜잭션에 포함합니다.',
        'commit하면 코디네이터가 마커를 써서 출력과 입력 offset이 함께 확정됩니다.',
        '처리 도중 실패하면 abort 후 종료합니다. 재시작하면 커밋된 위치부터 입력을 다시 읽습니다.',
    ],
    snippet=('고급/src/main/java/study/kafka/TransactionLab.java', 'transaction-pipeline'),
    explain=[
        '`records.nextOffsets()`는 poll 배치의 각 파티션별 다음 offset입니다. 이 코드는 배치 전체를 한 트랜잭션에 넣으므로 배치가 끝난 위치가 곧 안전한 완료 위치입니다.',
        '`ProducerFencedException`, `InvalidProducerEpochException`, `OutOfOrderSequenceException`, `AuthorizationException`은 치명적 오류로 보고 Producer를 재사용하지 않고 종료합니다. 그 밖의 처리 오류만 abort합니다.',
        '`commitTransaction()`은 abort를 하는 catch 블록 밖에 둡니다. commit 타임아웃은 결과가 불확실하므로 함부로 abort하지 않고 예외를 올려 종료합니다.',
        '`tx-process`의 max는 배치 경계에서만 확인하므로 지정한 수보다 많이 처리할 수 있습니다. 하류 Consumer에는 `read_committed`를 지정합니다.',
    ],
    pitfall='single 환경은 학습 편의를 위해 transaction state의 복제 수와 최소 ISR을 1로 둡니다. 내구성과 장애 실험은 `-Cluster` 환경을 사용하세요. 트랜잭션이 업무의 모든 부작용을 원자적으로 묶는다고 표현하지 않습니다. Kafka 밖의 부작용은 재처리 때 다시 일어날 수 있습니다.',
    commands=[
        ('취소된 레코드', r'.\실습\study.ps1 run tx-produce abort tx-aborted-demo', 'TRANSACTION ABORT가 출력됩니다.', r"""
```text
TRANSACTION ABORT event=tx-aborted-demo
```

`tx-produce`는 `transactional.id=course-tx-writer`(환경 변수 `TX_ID`가 있으면 그 값)로 `initTransactions()`를 호출한 뒤 트랜잭션을 시작하고, key `order-tx`로 레코드 한 건을 `send(...).get()`합니다. 이 시점에 레코드는 **이미 `orders.tx` 로그에 기록되고 ack까지 받았습니다.** 그다음 `abortTransaction()`이 호출되어 코디네이터가 ABORT 마커를 씁니다.

ack를 받았는데도 이 레코드는 업무 데이터가 아닙니다. 'ack = 기록 확인'과 '확정되어 보임'이 다른 개념이라는 점을 다음 두 명령에서 직접 확인합니다.
"""),
        ('확정된 레코드', r'.\실습\study.ps1 run tx-produce commit tx-committed-demo', 'TRANSACTION COMMIT이 출력됩니다.', r"""
```text
TRANSACTION COMMIT event=tx-committed-demo
```

같은 `transactional.id`로 다시 실행했으므로 `initTransactions()`가 epoch를 하나 올립니다. key가 같은 `order-tx`이므로 이 레코드는 방금 abort된 레코드와 **같은 파티션**의 뒤쪽에 기록되고, 이어서 COMMIT 마커가 붙습니다.

이 파티션의 로그를 순서대로 적으면 'abort된 레코드 → ABORT 마커 → 확정된 레코드 → COMMIT 마커'가 됩니다. 마커도 offset을 하나씩 차지한다는 점을 기억하고 다음 출력을 보세요.
"""),
        ('격리 수준 비교 A', r'.\실습\study.ps1 run consumer tx-view-all 0 orders.tx 0 read_uncommitted', '두 종류의 트랜잭션 레코드가 보입니다. Consumer는 제어 레코드는 반환하지 않습니다.', r"""
```text
ASSIGNED [orders.tx-0, orders.tx-1, orders.tx-2]
RECEIVED group=tx-view-all key=order-tx partition=2 offset=0 value={"schemaVersion":1,"eventId":"tx-aborted-demo",...}
RECEIVED group=tx-view-all key=order-tx partition=2 offset=2 value={"schemaVersion":1,"eventId":"tx-committed-demo",...}
REVOKED [orders.tx-0, orders.tx-1, orders.tx-2]
CONSUMED 2
```

`read_uncommitted`는 abort된 `tx-aborted-demo`까지 읽습니다. offset이 0 다음에 2로 건너뛰는 것을 보세요. **빠진 offset 1은 ABORT 마커**이고, 확정 레코드 뒤의 offset 3에는 COMMIT 마커가 있습니다. Consumer는 제어 레코드를 반환하지 않으므로 마커는 출력에 나오지 않습니다.

partition 번호는 예시입니다. 이전에 `tx-process`를 실행했다면 `orders.tx`에 다른 레코드도 있어 줄 수와 offset이 달라집니다.
"""),
        ('격리 수준 비교 B', r'.\실습\study.ps1 run consumer tx-view-committed 0 orders.tx 0 read_committed', 'abort된 tx-aborted-demo는 보이지 않습니다.', r"""
```text
ASSIGNED [orders.tx-0, orders.tx-1, orders.tx-2]
RECEIVED group=tx-view-committed key=order-tx partition=2 offset=2 value={"schemaVersion":1,"eventId":"tx-committed-demo",...}
REVOKED [orders.tx-0, orders.tx-1, orders.tx-2]
CONSUMED 1
```

같은 데이터를 `read_committed`로 읽으면 확정된 한 건만 나옵니다. Consumer는 abort된 offset 0과 마커를 조용히 건너뛰고 position을 앞으로 옮깁니다. 두 명령의 차이는 **쓰는 쪽이 아니라 읽는 쪽의 설정**에서 나왔습니다. 트랜잭션을 써도 하류 Consumer가 기본값(`read_uncommitted`)을 쓰면 보장이 깨진다는 뜻입니다.
"""),
        ('Kafka → Kafka 처리', r'.\실습\study.ps1 run tx-process tx-pipeline-day09 12', 'TX_COMMITTED와 배치 처리 수가 출력됩니다.', r"""
```text
ASSIGNED [orders-0, orders-1, orders-2]
TX_COMMITTED batch=10 total=10
TX_COMMITTED batch=10 total=20
REVOKED [orders-0, orders-1, orders-2]
```

`tx-pipeline-day09`는 새 그룹이므로 `orders`의 가장 오래된 레코드부터 `read_committed`로 읽습니다. 한 줄은 '배치 하나를 트랜잭션 하나로 확정했다'는 뜻입니다. `batch`는 poll 한 번에 받은 건수로, 이 실습은 `max.poll.records=10`이라 최대 10입니다.

max를 12로 줬는데 total이 20인 이유는 max를 배치 경계에서만 확인하기 때문입니다. 첫 배치 뒤 total=10은 12보다 작으므로 한 번 더 poll하고, 두 번째 배치 전체를 처리합니다. 배치 중간에서 멈추면 트랜잭션을 나눠야 하므로 이 편이 안전합니다. 결과는 `orders.tx`에 쓰였으니 새 그룹으로 `read_committed` 소비를 실행해 확인해 보세요. 숫자는 토픽에 쌓인 데이터에 따라 달라집니다.
"""),
    ],
    experiment=[
        '두 `tx-process`를 같은 기본 `transactional.id`로 동시에 실행하면 한쪽에서 fencing 오류가 납니다. 왜 그런지 epoch로 설명해 보세요. 실제 다중 실행은 `TX_ID`를 인스턴스별로 다르게 설정해야 합니다.',
        '`tx-process` 실행 뒤 새 그룹으로 `orders.tx`를 `read_committed`와 `read_uncommitted`로 각각 읽어 건수를 비교해 보세요.',
        '처리 코드에 일반 DB 쓰기를 추가하면 보장 범위가 왜 달라지는지, 그림에 Kafka 트랜잭션 경계와 DB 트랜잭션 경계를 따로 그려 보세요.',
    ],
    logcase=dict(
        title='원본 로그 적재에는 트랜잭션이 거의 필요 없습니다',
        body=r"""
장비 로그를 수집 서버가 Kafka에 쓰고, 저장 Consumer가 로그 저장소(ClickHouse 등)에 적재하는 경로를 생각해 보세요. 이 경로의 끝은 Kafka가 아니라 **외부 저장소**입니다. 이 단원에서 본 대로 Kafka 트랜잭션은 Kafka 출력까지만 묶으므로, 저장소 적재까지 Exactly Once로 만들어 주지 않습니다. ClickHouse의 Kafka 테이블 엔진도 공식 문서에서 at-least-once라고 밝히고 있습니다. 로그 적재의 중복은 결국 저장소 쪽에서 로그 ID 기반 중복 정리나 06단원의 멱등 설계로 다뤄야 합니다.

트랜잭션을 검토할 만한 곳은 **Kafka에서 Kafka로 가는 가공 단계**입니다.

- **정규화 단계:** 장비마다 형식이 다른 원본 로그 토픽을 읽어 공통 스키마로 바꾼 뒤 정규화 토픽에 쓰는 처리기는 consume-transform-produce 그 자체입니다. 재시작 때 정규화 토픽에 중복이 생기면 곤란하다면 트랜잭션을 쓰고, 하류 Consumer를 모두 `read_committed`로 맞춥니다.
- **분기 단계:** 한 로그를 '저장용'과 '알람용' 두 토픽에 나눠 쓸 때, 한쪽에만 쓰이는 상황을 막고 싶다면 두 출력을 한 트랜잭션에 넣습니다.

비용도 따져야 합니다. 로그는 양이 많고, 중복 몇 건보다 처리량과 지연이 더 중요한 경우가 흔합니다. 트랜잭션을 쓰면 결과가 commit 단위로 묶여 보이므로 실시간 대시보드의 지연이 커질 수 있습니다. '중복을 저장소에서 정리할 수 있는가'를 먼저 확인하고, 그럴 수 없을 때 트랜잭션을 고르는 순서가 현실적입니다.
""",
    ),
    faq=[
        ('Kafka 트랜잭션을 abort하면 같이 실행한 DB 쓰기도 롤백되나요?', r"""
아닙니다. Kafka 트랜잭션은 Kafka의 파티션 쓰기와 Consumer Group offset만 묶습니다. DB 커밋은 DB의 트랜잭션이 결정하며 Kafka는 관여하지 않습니다. Kafka는 외부 시스템과 2단계 커밋(XA)에 참여하지 않으므로, DB와 Kafka를 함께 다뤄야 할 때는 06단원의 멱등 처리나 10단원의 Outbox 패턴을 씁니다.
"""),
        ('트랜잭션으로 썼는데 Consumer에서 abort된 데이터가 보입니다.', r"""
Consumer의 `isolation.level`을 확인하세요. Kafka 기본값은 `read_uncommitted`이므로, 설정하지 않은 Consumer는 abort된 레코드와 확정 전 레코드를 모두 읽습니다. 트랜잭션으로 쓴 토픽을 읽는 Consumer에는 `read_committed`를 명시해야 합니다. 이 실습의 `consumer` 명령은 다섯 번째 인자로 격리 수준을 받습니다.
"""),
        ('`enable.idempotence`와 트랜잭션은 무엇이 다른가요?', r"""
멱등 Producer는 한 Producer 세션 안에서 네트워크 재시도 때문에 같은 배치가 파티션에 두 번 기록되는 것을 막습니다. 트랜잭션은 여기에 여러 파티션 쓰기의 원자성, 입력 offset과의 동시 확정, 재시작을 넘는 fencing을 더합니다. `transactional.id`를 설정하면 멱등성은 자동으로 켜집니다. 어느 쪽도 애플리케이션이 같은 업무 이벤트를 send로 두 번 보내는 것은 막지 않습니다.
"""),
        ('브로커가 1대인 환경에서도 트랜잭션이 동작하나요?', r"""
동작은 하지만 기본 설정 그대로는 어렵습니다. 트랜잭션 상태를 담는 `__transaction_state` 토픽이 기본적으로 브로커 3대 이상의 복제를 요구하기 때문입니다. 이 실습의 단일 환경은 `compose.yaml`에서 transaction state의 복제 수와 최소 ISR을 1로 낮춰 학습용으로만 맞췄습니다. 브로커 1대가 죽으면 트랜잭션 상태도 함께 사용할 수 없으므로 운영 구성으로 쓰면 안 됩니다.
"""),
        ('offset 번호가 1, 3, 5처럼 건너뛰는데 데이터가 사라진 건가요?', r"""
트랜잭션으로 쓴 토픽에서는 정상입니다. commit·abort 때 기록되는 트랜잭션 마커가 offset을 차지하지만 Consumer는 이를 반환하지 않으므로 번호가 비어 보입니다. `read_committed`라면 abort된 레코드의 offset도 건너뜁니다. 그래서 'offset 차이 = 업무 메시지 수'라는 계산은 트랜잭션 토픽에서는 맞지 않습니다.
"""),
    ],
    quiz=('sendOffsetsToTransaction이 묶는 대상은?', ['외부 DB와 모든 HTTP 요청', 'Kafka 출력과 입력 그룹의 offset', '운영 로그와 알람'], 1, '같은 Kafka 트랜잭션에 참여한 출력 레코드와 입력 Consumer Group의 offset을 함께 확정하거나 함께 취소합니다. 외부 DB와 HTTP 호출은 포함되지 않습니다.'),
    sources=['producer', 'consumer', 'design', 'pconfig', 'cconfig', 'streams'],
)
