"""LESSON 08 · Replication · ISR · KRaft: 이론 페이지와 실습 안내의 원본입니다."""

LESSON = dict(
    id='08',
    level='중급',
    title='브로커 하나가 멈춘다면?',
    short='Replication · ISR · KRaft',
    minutes=75,
    desc='3개 브로커로 Leader 교체와 쓰기 가용성의 조건을 관찰하고, 복제 수 · 최소 ISR · acks가 내구성을 어떻게 정하는지 이해합니다.',
    goals=[
        '데이터 복제와 Consumer 분담을 구분한다',
        'acks=all과 min.insync.replicas를 함께 설명한다',
        '한 노드 중단과 두 노드 중단의 차이를 이해한다',
        'High Watermark와 ISR로 Consumer가 읽을 수 있는 범위를 설명한다',
    ],
    why=r"""
지금까지는 브로커 1대짜리 환경에서 실습했습니다. 그 브로커가 멈추면 모든 토픽의 읽기와 쓰기가 함께 멈추고, 디스크가 망가지면 데이터도 사라집니다. 운영 환경의 Kafka가 여러 대의 브로커로 클러스터를 이루고 파티션마다 복제본을 두는 이유가 여기에 있습니다.

하지만 '복제본이 3개니까 안전하다'는 말은 반만 맞습니다. 복제본이 실제로 최신 상태를 따라가고 있는지(ISR), 쓰기를 받아들일 때 몇 개의 복제본이 확인해야 하는지(`acks`, `min.insync.replicas`), 리더를 누가 정하는지(KRaft 컨트롤러)에 따라 같은 복제 수 3도 결과가 크게 달라집니다. 이 단원에서는 3노드 클러스터에서 브로커 하나를 직접 멈춰 보며, 리더가 바뀌고 ISR이 줄었다가 회복되는 과정을 관찰합니다.
""",
    analogy=('원본 장부와 실시간 사본을 든 세 명의 기록원', r"""
중요한 장부를 기록원 세 명이 나눠 지킨다고 해 봅시다. 그중 한 명이 **원본 담당(Leader)**이고, 나머지 두 명(Follower)은 원본 담당이 새 줄을 적을 때마다 자기 사본에 똑같이 옮겨 적습니다. 장부에 기록을 맡기는 사람(Producer)은 원본 담당에게만 말합니다.

원본 담당은 '사본까지 확실히 옮겨 적혔다'고 확인된 줄까지만 다른 사람이 읽게 해 줍니다. 원본에만 있고 사본에는 아직 없는 줄을 보여 줬다가 원본 담당이 쓰러지면, 그 줄은 사라질 수 있기 때문입니다. 이 '읽어도 되는 마지막 줄'이 **High Watermark**입니다.

사본 담당 중 한 명이 너무 느려 한참 뒤처지면, 원본 담당은 그 사람을 '최신 사본을 가진 사람 명단(ISR)'에서 잠시 뺍니다. 원본 담당이 쓰러지면 관리자(Controller)가 명단에 남은 사람 중 한 명을 새 원본 담당으로 지명합니다. 그리고 관리자도 혼자가 아니라 세 명이 다수결로 결정하기 때문에, 두 명이 동시에 자리를 비우면 새 지명조차 할 수 없게 됩니다.
"""),
    concept=[
        ('파티션마다 Leader와 복제본이 있습니다', r"""
복제는 **파티션 단위**로 이루어집니다. 복제 수(replication factor) 3인 토픽이라면 파티션마다 서로 다른 브로커 3대에 같은 로그가 한 벌씩 있습니다. 그중 하나가 **Leader**로서 Producer의 쓰기와 Consumer의 읽기를 담당하고, 나머지 **Follower**는 Leader에게서 로그를 가져와(fetch) 자기 복사본에 덧붙입니다.

Producer와 Consumer는 클러스터에서 받은 메타데이터로 '이 파티션의 Leader가 어느 브로커인지' 알아내 그 브로커와 직접 통신합니다. 이 실습의 3노드 환경에서 `orders` 토픽을 조회하면 파티션 0은 브로커 2, 파티션 1은 브로커 3, 파티션 2는 브로커 1이 Leader인 식으로 **리더가 여러 브로커에 흩어져** 있습니다. 한 브로커에 부하가 몰리지 않게 하려는 배치입니다.

복제 수 3은 데이터 사본이 3벌이라는 뜻일 뿐, 같은 이벤트를 3번 처리한다는 뜻이 아닙니다. Consumer는 Leader 한 곳에서만 읽으며, 같은 그룹이 같은 레코드를 세 번 받는 일은 없습니다. 복제(가용성과 내구성)와 Consumer Group의 분담(처리 병렬성)은 서로 다른 개념입니다.
"""),
        ('ISR은 현재 동기화된 복제본 집합입니다', r"""
모든 복제본이 늘 최신인 것은 아닙니다. 네트워크가 느리거나 브로커가 바쁘면 Follower가 뒤처질 수 있습니다. 그래서 Kafka는 **ISR(In-Sync Replicas)**, 즉 '지금 Leader를 충분히 따라잡고 있는 복제본 목록'을 따로 관리합니다. Leader 자신도 ISR에 포함됩니다.

`acks=all`은 '모든 브로커'가 아니라 **'현재 ISR에 속한 모든 복제본'**이 기록을 확인해야 ack를 돌려준다는 뜻입니다. 여기서 함정이 생깁니다. Follower 두 대가 모두 뒤처져 ISR에서 빠지면 ISR에는 Leader 하나만 남고, `acks=all`이어도 Leader 혼자 기록하면 ack가 나갑니다. 이때 Leader가 죽으면 그 데이터는 사라집니다.

이를 막는 설정이 **`min.insync.replicas`**입니다. 값이 2이면 ISR이 2개 미만일 때 `acks=all` 쓰기를 거절합니다(`NotEnoughReplicasException`). Producer는 이 오류를 재시도하다가 `delivery.timeout.ms`(이 실습은 15초)가 지나면 실패로 보고합니다. 즉 '쓰기를 조금 못 받더라도, 받은 쓰기는 최소 두 곳에 있다'를 보장합니다. 복제 수 3, `min.insync.replicas=2`, `acks=all`이 흔히 쓰는 조합인 이유입니다. 이 실습의 `-Cluster` 환경도 이 조합으로 토픽을 만듭니다.
"""),
        ('Leader가 멈추면 ISR 안에서 새 Leader를 고릅니다', r"""
Leader 브로커가 멈추거나 종료 절차에 들어가면 KRaft 컨트롤러가 이를 알아차리고, 그 브로커가 Leader였던 파티션마다 **ISR에 남은 복제본 중 하나**를 새 Leader로 정합니다. 새 Leader 정보가 메타데이터로 퍼지면 Producer와 Consumer는 새 Leader로 연결을 옮깁니다. 그 짧은 사이에 Producer는 `NOT_LEADER_OR_FOLLOWER` 같은 오류를 받고 자동으로 재시도하므로, 애플리케이션 입장에서는 잠깐 느려졌다가 이어지는 것으로 보입니다.

실습 로그를 보면 이 과정이 그대로 드러납니다. kafka1(브로커 1)을 멈추기 전 파티션 2는 `Leader: 1, Isr: 1,2,3`이었고, 멈춘 뒤에는 `Leader: 2, Isr: 2,3`으로 바뀌었습니다. 파티션 0과 1은 Leader가 원래 브로커 2·3이었으므로 Leader는 그대로이고 ISR에서 1만 빠졌습니다.

브로커 1을 다시 켜면 그 복제본은 멈춰 있던 동안의 로그를 Leader에게서 따라잡은 뒤 ISR에 다시 들어갑니다. ISR이 `1,2,3`으로 돌아오면 복구가 끝난 것입니다.
"""),
        ('KRaft의 메타데이터 합의도 필요합니다', r"""
'누가 어떤 파티션의 Leader인가', 'ISR은 누구인가', '어떤 토픽이 있는가' 같은 클러스터 메타데이터는 **KRaft 컨트롤러 quorum**이 관리합니다. 컨트롤러들은 Raft 합의 방식으로 동작하며, 결정을 내리려면 **과반수**가 살아 있어야 합니다.

이 실습의 3노드는 모두 broker와 controller 역할을 겸합니다(`KAFKA_PROCESS_ROLES: broker,controller`, `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka1:9093,2@kafka2:9093,3@kafka3:9093`). 그래서 노드를 멈추는 실험은 데이터 복제본과 컨트롤러 투표권을 **동시에** 줄입니다.

- **한 노드 중단:** 컨트롤러 3개 중 2개가 남아 과반을 유지합니다. 리더 교체가 일어나고, ISR 2개가 `min.insync.replicas=2`를 만족하므로 쓰기도 계속됩니다.
- **두 노드 중단:** 컨트롤러가 1개만 남아 과반을 잃습니다. 리더 교체나 ISR 변경 같은 메타데이터 변경이 불가능해지고, 살아 있는 복제본도 1개뿐이라 `min.insync.replicas=2`를 만족할 수 없습니다. 쓰기가 정상적으로 이루어진다고 기대할 수 없습니다.

두 노드 중단 결과를 'ISR 조건 때문'이라고만 해석하면 절반만 본 것입니다. 이 실습 구성에서는 컨트롤러 과반 상실이 함께 일어난다는 점을 기억하세요.
"""),
        ('학습용 combined 구성의 한계', r"""
이 실습은 한 PC 안의 컨테이너 3개로 클러스터를 흉내 냅니다. 동작 원리를 관찰하기에는 충분하지만, 운영 환경과는 큰 차이가 있습니다.

- **장애 격리가 없습니다:** 세 브로커가 같은 PC, 같은 디스크, 같은 전원을 씁니다. PC가 꺼지면 복제본 3벌이 동시에 사라집니다. 운영에서는 브로커를 서로 다른 서버, 가능하면 다른 랙이나 가용 영역에 배치합니다.
- **역할이 합쳐져 있습니다:** 공식 KRaft 운영 문서는 broker와 controller를 한 프로세스에 둔 combined 구성을 개발 환경 같은 작은 용도에 적합하다고 설명하며, 중요한 운영 환경에는 권장하지 않습니다. 운영에서는 컨트롤러 전용 노드 3대(또는 5대)를 따로 둡니다.
- **장애 판정 시간이 다릅니다:** 컨테이너를 `stop`하는 것은 깔끔한 종료에 가깝습니다. 실제 서버 장애는 네트워크가 반쯤 끊기거나 디스크가 느려지는 식으로 더 애매하게 나타나며, 감지에도 시간이 걸립니다.

그 밖에 디스크 용량, 네트워크 대역폭, 복구 시간, 백업, 보안까지 함께 설계해야 운영 수준의 가용성을 말할 수 있습니다.
"""),
    ],
    example=r"""
3노드 클러스터에서 `orders` 파티션 2의 Leader가 kafka1이라고 해 봅시다. kafka1을 멈추면 컨트롤러가 ISR에 남은 kafka2나 kafka3 중 하나를 새 Leader로 정하고, ISR은 2개로 줄어듭니다. `min.insync.replicas=2`를 만족하므로 Producer의 `acks=all` 쓰기는 잠깐의 재시도 뒤 계속 성공합니다.

모든 파티션이 kafka1을 Leader로 쓰는 것은 아니므로, 실습 전에 `topics -Cluster`로 실제 배치를 확인하고 중단할 노드를 고릅니다. 여기서 kafka2까지 멈추면 살아 있는 복제본이 1개뿐이라 `acks=all` 쓰기가 성공할 수 없고, 컨트롤러도 과반을 잃습니다. 데이터가 지워지는 것은 아니지만, 노드가 돌아올 때까지 쓰기를 받을 수 없습니다.
""",
    scenario=dict(
        title='시간순으로 따라가기: kafka1 중단과 복구',
        intro='실제 검증 실행에서 기록한 `orders` 토픽 상태입니다. 여러분의 환경에서는 Leader 배치가 다를 수 있습니다.',
        columns=['시점', '일어나는 일', '파티션 2 상태', '쓰기 가능 여부'],
        rows=[
            ['T1', '3노드 정상 상태에서 토픽을 조회합니다.', 'Leader 1, ISR 1,2,3', '가능 (ISR 3 ≥ 2)'],
            ['T2', '`stop kafka1`로 브로커 1을 멈춥니다.', 'Leader 1이 종료되는 중', '파티션 2로 가는 전송은 잠시 실패 후 재시도'],
            ['T3', '컨트롤러가 브로커 1의 중단을 감지하고 파티션 2의 새 Leader를 정합니다.', 'Leader 2, ISR 2,3', '가능 (ISR 2 ≥ 2)'],
            ['T4', 'Producer가 메타데이터를 갱신해 새 Leader로 전송합니다.', 'Leader 2, ISR 2,3', '`SENT` 성공'],
            ['T5', '`start kafka1`로 브로커 1을 다시 켭니다. 멈춘 동안의 로그를 따라잡습니다.', 'Leader 2, ISR 2,3', '가능'],
            ['T6', '따라잡기가 끝나 브로커 1이 ISR에 다시 들어옵니다.', 'Leader 2, ISR 2,3,1', '가능 (ISR 3)'],
            ['T7', '선호 리더 재조정이 켜져 있다면 시간이 지나 브로커 1이 다시 Leader가 됩니다.', 'Leader 1, ISR 1,2,3', '가능'],
        ],
        note='T7은 브로커의 자동 리더 재조정(`auto.leader.rebalance.enable`, Kafka 기본값 `true`)이 일으킵니다. 컨트롤러가 리더 불균형을 점검하는 간격의 기본값은 300초(`leader.imbalance.check.interval.seconds`)이므로 복구 직후 Leader가 2로 남아 있어도 정상입니다.',
    ),
    tables=[
        dict(
            title='복제 수 · min.insync.replicas · acks 조합 비교',
            intro='`acks=all` 쓰기를 기준으로, 브로커가 몇 대 멈춰도 쓰기를 계속 받을 수 있는지와 ack를 받은 데이터가 최소 몇 곳에 있는지를 비교합니다.',
            columns=['복제 수', 'min.insync.replicas', '쓰기가 버티는 브로커 장애 수', 'ack 받은 데이터의 최소 사본 수', '평가'],
            rows=[
                ['1', '1', '0대', '1', '복제 없음. 학습용 single 환경이 이 구성입니다.'],
                ['3', '1', '2대', '1', '가용성은 높지만 ISR이 Leader 하나로 줄었을 때 받은 데이터는 한 곳에만 있습니다.'],
                ['3', '2', '1대', '2', '가용성과 내구성의 균형. 흔히 쓰는 조합이며 이 실습의 cluster 구성입니다.'],
                ['3', '3', '0대', '3', '한 대만 멈춰도 쓰기가 거절됩니다. 내구성은 가장 높습니다.'],
                ['2', '2', '0대', '2', '한 대만 멈춰도 쓰기가 멈춥니다. 복제 수 2는 이 균형을 잡기 어렵습니다.'],
            ],
            note='브로커 장애 수는 컨트롤러가 따로 과반을 유지한다는 전제의 값입니다. 이 실습처럼 노드 3대가 컨트롤러를 겸하면 2대 장애에서 컨트롤러 과반도 잃습니다. `acks=1`이면 Leader 기록만으로 ack가 나가므로 `min.insync.replicas`가 적용되지 않고, Leader가 복제 전에 멈추면 ack를 받은 데이터도 사라질 수 있습니다. Kafka 4.x Producer 기본값은 `acks=all`입니다.',
        ),
        dict(
            title='kafka-topics --describe 출력 읽는 법',
            columns=['항목', '예시 값', '뜻'],
            rows=[
                ['`PartitionCount` / `ReplicationFactor`', '3 / 3', '파티션 수와 파티션마다의 복제본 수입니다.'],
                ['`Configs`', '`min.insync.replicas=2,retention.ms=604800000`', '기본값과 다르게 지정한 토픽 설정입니다.'],
                ['`Leader`', '2', '이 파티션의 읽기·쓰기를 맡은 브로커 ID입니다.'],
                ['`Replicas`', '2,3,1', '복제본을 가진 브로커 목록입니다. 첫 번째가 선호 리더(preferred leader)입니다.'],
                ['`Isr`', '2,3', '현재 Leader를 따라잡고 있는 복제본입니다. 1이 빠졌다면 브로커 1이 멈췄거나 뒤처진 것입니다.'],
                ['`Elr` / `LastKnownElr`', '(비어 있음)', 'Kafka 4.x의 Eligible Leader Replicas 정보입니다. 정상 상태와 한 노드 장애에서는 보통 비어 있습니다.'],
            ],
        ),
    ],
    deep=[
        ('LEO와 High Watermark: Consumer는 어디까지 읽을 수 있나요?', r"""
각 복제본에는 **LEO(Log End Offset)**, 즉 '자기 로그의 다음에 쓸 위치'가 있습니다. Leader가 레코드를 받으면 Leader의 LEO가 먼저 늘고, Follower가 가져가 기록하면 Follower의 LEO도 따라 늡니다.

**High Watermark(HW)**는 ISR의 모든 복제본이 기록을 마친 위치입니다. Kafka 설계 문서는 ISR의 모든 복제본이 기록한 메시지를 **committed** 메시지라고 부르며, Consumer에게는 committed 메시지만 내준다고 설명합니다. Leader에만 있고 아직 복제되지 않은 레코드를 Consumer가 읽었다가 Leader가 죽으면, 새 Leader에는 그 레코드가 없어 '읽었던 데이터가 사라지는' 일이 생기기 때문입니다.

```text
Leader   LEO=105   [ ... 100 101 102 103 104 ]
Follower LEO=103   [ ... 100 101 102 ]
Follower LEO=104   [ ... 100 101 102 103 ]
High Watermark = 103  → Consumer는 offset 102까지 읽을 수 있음
```

ELR이 켜진 Kafka 4.x 클러스터는 ISR이 `min.insync.replicas`보다 작으면 HW를 올리지 않는 규칙(strict min ISR)도 적용합니다. 12단원에서 보는 Lag의 '끝 위치'도 이 HW를 기준으로 합니다.
"""),
        ('ISR에서 빠지는 조건과 unclean leader election', r"""
Follower가 ISR에 머물려면 두 가지 조건을 지켜야 합니다. 브로커가 컨트롤러와 세션을 유지해야 하고(KRaft에서는 하트비트), Leader의 로그 끝을 `replica.lag.time.max.ms`(Kafka 기본값 30000ms, 30초) 안에 따라잡아야 합니다. 이 시간 동안 따라잡지 못하면 Leader가 그 Follower를 ISR에서 뺍니다.

ISR의 모든 복제본이 동시에 사라지면 Kafka는 어려운 선택을 해야 합니다. 설계 문서는 두 가지 길을 설명합니다.

- **ISR 복제본이 돌아올 때까지 기다리기:** 일관성을 지키지만, 그 복제본이 영영 돌아오지 않으면 파티션도 계속 쓸 수 없습니다.
- **먼저 돌아온 아무 복제본이나 Leader로 세우기:** 바로 다시 쓸 수 있지만, 그 복제본에 없던 committed 메시지는 사라집니다.

두 번째 길을 허용하는 설정이 `unclean.leader.election.enable`이며, 기본값은 `false`입니다. 즉 Kafka는 기본적으로 데이터 일관성을 우선합니다. 일부 유실을 감수하더라도 가용성이 더 중요한 경우에만 신중하게 켭니다.
"""),
        ('ELR: ISR 밖에 있어도 안전하게 Leader가 될 수 있는 복제본', r"""
Kafka 4.x에는 **Eligible Leader Replicas(ELR, KIP-966)**라는 개선이 들어왔습니다. 공식 문서는 ELR을 'ISR에 속하지 않은 일부 복제본을 Leader가 되어도 안전한 상태로 만드는 개선'이라고 설명하며, 4.1부터 새로 만든 클러스터에서는 기본으로 켜집니다.

ELR이 켜져 있으면 컨트롤러는 Leader를 다음 순서로 고릅니다.

1. ISR이 비어 있지 않으면 ISR에서 고릅니다.
2. ISR이 비었다면, 차단(fenced)되지 않은 ELR 복제본에서 고릅니다.
3. 그것도 없으면, 마지막으로 알려진 Leader가 차단되지 않은 상태일 때 그 복제본을 고릅니다.

`kafka-topics --describe` 출력의 `Elr`, `LastKnownElr` 열이 이 정보입니다. 정상 상태와 한 노드 장애에서는 보통 비어 있으며, ISR이 최소 ISR보다 작아지는 심한 장애에서 의미가 생깁니다. 기존 클러스터를 4.x로 업그레이드한 경우에는 별도로 켜야 할 수 있으므로 운영 문서를 확인합니다.
"""),
        ('선호 리더, 리더 균형, 랙 인식', r"""
`Replicas` 목록의 첫 번째 브로커를 **선호 리더(preferred leader)**라고 합니다. 토픽을 만들 때 Kafka는 선호 리더가 브로커들에 고르게 흩어지도록 배치합니다. 장애로 리더가 다른 브로커로 옮겨 가면 이 균형이 깨집니다. 자동 리더 재조정(`auto.leader.rebalance.enable`)은 Kafka 기본값이 `true`이며, 컨트롤러가 `leader.imbalance.check.interval.seconds`(기본 300초) 간격으로 불균형을 점검해 원래 브로커가 돌아오면 선호 리더로 되돌립니다. 복구 직후에 리더가 바로 돌아오지 않는 것은 이 점검 주기 때문입니다.

브로커에 `broker.rack`(랙이나 가용 영역 이름)을 지정하면, 토픽을 만들 때 한 파티션의 복제본을 서로 다른 랙에 나눠 둡니다. 랙 하나가 통째로 멈춰도 다른 랙의 복제본이 살아남게 하려는 것입니다. Consumer에 `client.rack`을 지정하고 브로커가 랙 인식 복제본 선택기를 쓰도록 설정하면, Consumer가 Leader 대신 같은 랙의 Follower에서 읽게 할 수도 있습니다. 랙 사이 네트워크 비용을 줄일 때 씁니다.
"""),
        ('컨트롤러는 몇 대를 두나요?', r"""
KRaft 컨트롤러는 과반 합의로 동작하므로, 버틸 수 있는 장애 수는 과반이 남는 범위까지입니다.

- **3대:** 1대 장애까지 버팁니다(2대가 과반).
- **5대:** 2대 장애까지 버팁니다(3대가 과반).
- **4대:** 과반이 3대라 3대짜리와 똑같이 1대 장애까지만 버팁니다. 그래서 컨트롤러 수는 보통 홀수로 둡니다.

브로커는 데이터와 트래픽 양에 맞춰 늘리고, 컨트롤러는 3대나 5대로 고정해 따로 운영하는 구성이 일반적입니다. 이 실습처럼 노드 3대가 두 역할을 겸하면, 노드 장애가 데이터 복제본과 컨트롤러 투표권을 동시에 줄인다는 점을 염두에 둡니다.
"""),
    ],
    summary=[
        '복제는 파티션 단위이며, Leader가 읽기·쓰기를 맡고 Follower는 Leader의 로그를 따라 기록합니다.',
        '`acks=all`은 현재 ISR 전체의 확인이고, `min.insync.replicas`가 ISR의 최소 크기를 강제합니다.',
        '복제 수 3 · `min.insync.replicas=2` · `acks=all`은 브로커 1대 장애에서도 쓰기와 내구성을 함께 지킵니다.',
        'Consumer는 High Watermark(ISR 전체가 기록한 위치)까지만 읽으므로, 복제되지 않은 데이터를 보지 않습니다.',
        '이 실습의 combined 구성에서 두 노드가 멈추면 ISR 조건과 컨트롤러 과반을 함께 잃습니다.',
    ],
    nodes=[('Producer', 'acks = all', 30, 140), ('Leader', 'ISR 중 1개', 330, 140), ('Follower B', '동기화 복제', 650, 35), ('Follower C', '동기화 복제', 650, 245), ('Controller quorum', '메타데이터 · 과반 합의', 30, 290)],
    edges=[(0, 1, 'write / ack'), (1, 2, 'replicate'), (1, 3, 'replicate'), (4, 1, '리더 관리')],
    steps=[
        '복제 수 3, 최소 ISR 2인 토픽을 만듭니다.',
        'Leader가 레코드를 받아 Follower에 복제하고, ISR 전체가 기록하면 ack를 보냅니다.',
        '노드 하나가 중단됩니다. Controller quorum 과반은 유지됩니다.',
        '필요하면 Leader가 ISR 안의 다른 복제본으로 교체되고 ISR은 2개로 줄어듭니다.',
        '남은 ISR이 최소 조건을 만족하므로 전송이 계속됩니다. 노드를 복구하면 따라잡기 후 ISR이 3개로 회복됩니다.',
    ],
    snippet=('초급/src/main/java/study/kafka/TopicAdmin.java', 'topic-create'),
    explain=[
        '`-Cluster`로 실행하면 `REPLICATION_FACTOR=3`이 주입되어 `NewTopic(name, 3, 3)`이 되고, 사용자 토픽의 `min.insync.replicas`는 2가 됩니다.',
        'Consumer offset과 트랜잭션 상태를 저장하는 내부 토픽도 cluster 환경에서는 복제 수 3으로 구성했습니다(`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3`, `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3`).',
        """INTERNAL은 Docker 서비스 주소(`kafka1:9092`), EXTERNAL은 호스트에서 쓰는 주소(`localhost:29092` 등)입니다. `advertised.listeners`는 클라이언트가 실제로 도달할 수 있는 주소여야 합니다.""",
        '복제 수는 토픽을 만들 때 정합니다. 이미 만든 토픽의 복제 수를 바꾸려면 파티션 재배치 작업이 필요하므로, 처음부터 운영 기준에 맞게 정합니다.',
    ],
    pitfall="""초급 토픽이 들어 있는 single Kafka와 3노드 cluster는 별도 클러스터입니다. -Cluster를 빠뜨리면 다른 환경을 조회하거나 실행합니다. PostgreSQL은 공용이므로 DB 중복 표식은 환경 전환 후에도 남습니다. 복제 수 3은 사본 3벌이라는 뜻이지 Consumer가 같은 레코드를 세 번 처리한다는 뜻이 아닙니다.""",
    commands=[
        ('3노드 환경 시작', r'.\실습\study.ps1 up -Cluster', 'kafka1~3과 postgres가 healthy가 됩니다. PC 자원이 충분해야 합니다.', r"""
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
"""),
        ('클러스터 토픽 생성', r'.\실습\study.ps1 init -Cluster', '복제 수 3, ISR 3인 토픽을 확인합니다.', r"""
```text
orders [(partition=0, leader=kafka2:9092 (id: 2 rack: null isFenced: false),
         replicas=kafka2:9092 (id: 2 ...), kafka3:9092 (id: 3 ...), kafka1:9092 (id: 1 ...),
         isr=kafka2:9092 (id: 2 ...), kafka3:9092 (id: 3 ...), kafka1:9092 (id: 1 ...),
         elr=, lastKnownElr=), (partition=1, ...), (partition=2, ...)]
```

single 환경의 01단원 출력과 비교해 보세요. `replicas`와 `isr`에 브로커가 3개씩 나열됩니다. 파티션마다 `leader`가 서로 다른 브로커로 흩어져 있는지도 확인하세요. `rack: null`은 브로커에 랙 정보를 지정하지 않았다는 뜻입니다.
"""),
        ('Leader 조회', r'.\실습\study.ps1 topics -Cluster', 'orders 파티션별 Leader ID를 기록합니다.', r"""
```text
Topic: orders  TopicId: 4i58hWBNSViNeNGdm7wX1g  PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2,retention.ms=604800000
  Topic: orders  Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1  Elr:   LastKnownElr:
  Topic: orders  Partition: 1  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2  Elr:   LastKnownElr:
  Topic: orders  Partition: 2  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3  Elr:   LastKnownElr:
```

이번에는 브로커에 포함된 `kafka-topics.sh --describe`의 출력입니다. 위 예시에서는 브로커 1이 파티션 2의 Leader입니다. `Replicas`의 첫 번째 값이 선호 리더이므로, 정상 상태에서는 `Leader`와 `Replicas` 첫 값이 같습니다.

다음 단계에서 멈출 브로커를 이 출력으로 정합니다. Leader를 하나 이상 맡은 브로커를 골라야 리더 교체를 관찰할 수 있습니다.
"""),
        ('한 노드 중단', r'.\실습\study.ps1 stop kafka1', '예시로 kafka1을 중단합니다. 실제 Leader가 다른 노드라면 그 노드를 선택하세요.', r"""
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
"""),
        ('장애 중 전송', r'.\실습\study.ps1 run -Cluster producer 6 orders failover-demo', '메타데이터 갱신 후 전송 성공 여부와 일시 재시도 로그를 관찰합니다.', r"""
```text
[main] WARN org.apache.kafka.clients.ClientUtils - Couldn't resolve server kafka1:9092 from bootstrap.servers as DNS resolution failed for kafka1
SENT event=failover-demo-0 key=order-0 partition=1 offset=0
SENT event=failover-demo-1 key=order-1 partition=1 offset=1
SENT event=failover-demo-2 key=order-2 partition=0 offset=0
...
```

`WARN ... Couldn't resolve server kafka1:9092`는 `bootstrap.servers`에 적힌 세 주소 중 kafka1 컨테이너가 멈춰 이름을 찾지 못했다는 경고입니다. 나머지 주소로 클러스터에 접속하므로 전송에는 문제가 없습니다. bootstrap 주소를 여러 개 적는 이유가 이것입니다.

`SENT` 6줄이 모두 나왔다면, `acks=all`과 `min.insync.replicas=2` 조건에서 ISR 2개로 쓰기가 계속된 것입니다. 리더 교체 직후라면 Producer가 내부적으로 재시도해 첫 줄이 조금 늦게 나올 수 있습니다. 여기서 kafka2까지 멈추고 같은 명령을 실행하면, 살아 있는 복제본이 하나뿐이고 컨트롤러 과반도 사라져 전송이 성공하지 못합니다. 이 실습 설정에서는 대략 15초(`delivery.timeout.ms`, `max.block.ms`) 뒤 실패로 끝납니다. 실험 뒤에는 반드시 두 노드를 모두 다시 시작하세요.
"""),
        ('복구', r'.\실습\study.ps1 start kafka1', '잠시 후 topics -Cluster로 ISR이 회복되는지 확인합니다.', r"""
```text
 Container kafka-field-guide-kafka1-1 Starting
 Container kafka-field-guide-kafka1-1 Started
```

브로커 1은 멈춰 있던 동안 쌓인 로그(위 단계의 failover-demo 6건 등)를 각 파티션의 Leader에게서 가져와 따라잡은 뒤 ISR에 다시 들어갑니다. `topics -Cluster`에서 `Isr`가 다시 3개(예: `2,3,1`)가 되면 복구가 끝난 것입니다.

복구 직후 파티션 2의 `Leader`가 여전히 2일 수 있습니다. 선호 리더(Replicas의 첫 값인 1)로 되돌리는 자동 재조정은 기본 300초 간격의 점검에서 일어나므로, 바로 바뀌지 않아도 정상입니다.
"""),
    ],
    experiment=[
        'single과 cluster는 서로 다른 로그 볼륨을 사용합니다. cluster의 그룹과 토픽은 별도로 시작합니다.',
        '두 노드 중단은 controller 과반도 잃으므로 별도의 복구 실험으로 구분하세요. 반드시 중단한 노드를 모두 start로 복구합니다.',
        r'장애 중 전송한 failover-demo 6건을 `.\실습\study.ps1 run -Cluster consumer failover-reader 0 orders`로 읽어, 리더가 바뀐 파티션의 레코드도 빠짐없이 읽히는지 확인하세요.',
        'kafka1을 복구한 뒤 몇 분 간격으로 `topics -Cluster`를 실행해, 파티션 2의 Leader가 선호 리더로 돌아오는지 관찰해 보세요.',
    ],
    logcase=dict(
        title='로그 토픽의 복제 설정과, 쓰기가 막혔을 때 수집 서버의 선택',
        body=r"""
장비 로그는 장애 분석의 근거가 되므로, 수집된 로그가 브로커 한 대의 고장으로 사라지면 곤란합니다. 그래서 로그 토픽도 보통 **복제 수 3, `min.insync.replicas=2`, Producer `acks=all`**로 둡니다.

- **처리량 비용:** `acks=all`은 Follower의 기록까지 기다리므로 `acks=1`보다 지연이 조금 늘어납니다. 하지만 로그 수집은 배치와 압축으로 처리량을 확보하는 경우가 많아(04단원), 이 비용은 대개 감당할 만합니다. `acks=1`로 낮추면 Leader 장애 순간에 ack를 받은 로그가 사라질 수 있다는 점을 알고 선택해야 합니다.
- **한 대 장애:** 위 설정이면 브로커 한 대가 멈춰도 수집은 계속됩니다. 수집 서버는 잠깐의 재시도만 겪습니다.
- **두 대 장애:** ISR이 1개가 되어 쓰기가 거절되고, Producer는 `delivery.timeout.ms` 뒤 실패를 보고합니다. 이때 수집 서버가 할 수 있는 일은 둘 중 하나입니다. 로컬 디스크 버퍼에 잠시 쌓아 두고 Kafka가 돌아오면 다시 보내거나, 수집 에이전트에게 실패 응답(예: HTTP 503)을 돌려줘 에이전트가 자기 버퍼에 보관하고 재시도하게 하는 것입니다.
- **가장 위험한 선택:** Kafka ack를 받기 전에 에이전트에게 성공(2xx)을 돌려주는 것입니다. 에이전트는 로그를 지우고, 수집 서버는 Kafka에 쓰지 못해 로그가 어디에도 남지 않습니다. 수집 서버의 성공 응답은 Kafka ack 이후에 보냅니다.

브로커 배치도 중요합니다. 브로커 3대를 같은 서버나 같은 랙에 두면 복제 수 3의 의미가 크게 줄어듭니다.
""",
    ),
    faq=[
        ('복제 수가 3이면 Consumer가 같은 메시지를 3번 받나요?', r"""
아닙니다. 복제는 장애에 대비한 사본일 뿐이고, Consumer는 기본적으로 각 파티션의 Leader 한 곳에서만 읽습니다. 같은 그룹 안에서 한 파티션은 한 Consumer만 맡으므로(03단원), 복제 수와 관계없이 그룹은 각 레코드를 한 번 받습니다.
"""),
        ('브로커 두 대가 멈추면 데이터가 사라지나요?', r"""
데이터가 지워지는 것은 아닙니다. 멈춘 브로커의 디스크에는 로그가 그대로 있고, 남은 한 대에도 사본이 있습니다. 다만 살아 있는 복제본이 최소 ISR보다 적어 `acks=all` 쓰기가 성공할 수 없고, 이 실습 구성에서는 컨트롤러 과반도 잃어 클러스터가 정상적으로 동작하지 않습니다. 브로커가 돌아오면 다시 동기화됩니다. `unclean.leader.election.enable`이 기본값 `false`이므로, Kafka는 데이터가 덜 복제된 복제본을 억지로 Leader로 세우지 않습니다.
"""),
        ('`acks=all`인데 ISR이 Leader 하나로 줄면 어떻게 되나요?', r"""
`min.insync.replicas`가 1이면 Leader 혼자 기록해도 ack가 나가므로, 그 순간부터는 사실상 복제 없이 쓰는 셈입니다. 그래서 복제 수 3에서는 `min.insync.replicas=2`를 함께 설정해, ISR이 하나로 줄면 차라리 쓰기를 거절하게 합니다. 토픽 설정의 Kafka 기본값은 1이므로 명시적으로 지정해야 합니다.
"""),
        ('장애 중 전송에서 kafka1 DNS 경고가 나오는데 문제인가요?', r"""
문제가 아닙니다. Producer 설정의 `bootstrap.servers`에 세 브로커 주소가 모두 들어 있고, 멈춘 kafka1 컨테이너의 이름을 찾지 못했다는 경고일 뿐입니다. 클라이언트는 나머지 주소로 접속해 전체 클러스터의 메타데이터를 받아 옵니다. 운영에서도 bootstrap 주소를 여러 개 적어 두는 이유입니다.
"""),
    ],
    quiz=('acks=all에서 all이 가리키는 대상은?', ['항상 클러스터의 모든 브로커', '현재 ISR에 속한 복제본의 확인 조건', '모든 Consumer의 업무 완료'], 1, '현재 ISR 전체의 기록 확인을 뜻합니다. ISR이 줄어들 수 있으므로 min.insync.replicas와 함께 해석해야 합니다.'),
    sources=['design', 'pconfig', 'kraft', 'tconfig', 'elr'],
)
