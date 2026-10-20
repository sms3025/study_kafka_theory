"""Course authoring source. build_theory.py renders HTML and matching lab guides.

단원 본문은 tools/content/lesson_XX.py, 부록은 tools/content/appendix.py에 있습니다.
이 파일은 공식 출처 목록, 추가 코드 발췌, 단원 목록의 조립만 담당합니다.

본문 문자열에는 간단한 표기법을 쓸 수 있습니다.
- 빈 줄로 문단을 나눕니다.
- '- '로 시작하는 줄은 글머리표 목록, '1. '로 시작하는 줄은 번호 목록이 됩니다.
- `코드`는 고정폭 글꼴, **강조**는 굵은 글씨가 됩니다.
- ```text / ```java / ```powershell 로 감싼 부분은 코드 블록이 됩니다.
"""
import importlib

SOURCES = {
    'intro': ('Kafka 핵심 개념', 'https://kafka.apache.org/43/getting-started/introduction/'),
    'producer': ('Producer API / 트랜잭션', 'https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html'),
    'consumer': ('Consumer API / 오프셋', 'https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html'),
    'design': ('Kafka 설계와 전달 보장', 'https://kafka.apache.org/43/design/design/'),
    'kraft': ('KRaft 운영', 'https://kafka.apache.org/43/operations/kraft/'),
    'pconfig': ('Producer 설정', 'https://kafka.apache.org/43/configuration/producer-configs/'),
    'cconfig': ('Consumer 설정', 'https://kafka.apache.org/43/configuration/consumer-configs/'),
    'tconfig': ('토픽 설정', 'https://kafka.apache.org/43/configuration/topic-configs/'),
    'rebalance': ('Consumer Rebalance Protocol (KIP-848)', 'https://kafka.apache.org/43/operations/consumer-rebalance-protocol/'),
    'elr': ('Eligible Leader Replicas (KIP-966)', 'https://kafka.apache.org/43/operations/eligible-leader-replicas/'),
    'streams': ('Streams DSL', 'https://kafka.apache.org/43/streams/developer-guide/dsl-api/'),
    'postgres': ('PostgreSQL SELECT / SKIP LOCKED', 'https://www.postgresql.org/docs/17/sql-select.html'),
    'monitor': ('Kafka 모니터링', 'https://kafka.apache.org/43/operations/monitoring/'),
    'security': ('Kafka 보안', 'https://kafka.apache.org/43/security/'),
    'connect': ('Kafka Connect 사용자 가이드', 'https://kafka.apache.org/43/kafka-connect/user-guide/'),
    'kip1030': ('KIP-1030: 4.0의 기본값 변경 (linger.ms 등)', 'https://cwiki.apache.org/confluence/display/KAFKA/KIP-1030:+Change+constraints+and+default+values+for+various+configurations'),
    'release42': ('Apache Kafka 4.2.0 릴리스 공지', 'https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/'),
    'release43': ('Apache Kafka 4.3.0 릴리스 공지', 'https://kafka.apache.org/blog/2026/05/22/apache-kafka-4.3.0-release-announcement/'),
    'spring-kafka': ('Spring for Apache Kafka: 오류 처리와 DLT', 'https://docs.spring.io/spring-kafka/reference/4.1/kafka/annotation-error-handling.html'),
    'schema-evolution': ('Schema Registry 호환성 규칙 (Confluent)', 'https://docs.confluent.io/platform/7.4/schema-registry/fundamentals/schema-evolution.html'),
    'kafbat': ('Kafbat UI 시작하기', 'https://ui.docs.kafbat.io/overview/getting-started'),
    'clickhouse-kafka': ('ClickHouse Kafka 테이블 엔진', 'https://clickhouse.com/docs/integrations/kafka/kafka-table-engine'),
    'fluentbit-kafka': ('Fluent Bit Kafka 출력 플러그인', 'https://docs.fluentbit.io/manual/4.1/data-pipeline/outputs/kafka'),
}

ADDITIONAL_SNIPPETS = {
    '04': [('초급/src/main/java/study/kafka/AsyncOrderProducer.java','async-send','비동기 전송과 애플리케이션 대기열 제한','단건 send().get() 다음에는 callback으로 결과를 확인하는 방식을 읽어 보세요. Semaphore는 애플리케이션의 미완료 전송을 32건으로 제한합니다. Kafka 연결의 max.in.flight 설정과는 다른 계층입니다. callback에서 긴 업무를 실행하지 않습니다. 로그는 관찰용이며 운영에서는 로깅 비용도 평가합니다.')],
    '10': [('고급/src/main/java/study/kafka/OutboxLab.java','outbox-write','먼저, 주문과 발행 의도를 함께 저장합니다','Relay 이전의 쓰기 경계입니다. 주문과 outbox INSERT를 같은 Connection의 DB 트랜잭션에 넣습니다. fail 모드는 두 INSERT 뒤에 오류를 내므로 롤백 여부를 검증할 수 있습니다.')],
    '12': [('공통/src/main/java/study/kafka/OrderEvent.java','schema-validation','이벤트 계약을 코드로 검증합니다','알 수 없는 선택 필드는 ObjectMapper 설정으로 허용하고, 지원하지 않는 버전·필수 값·금액·시간 형식은 거부합니다. 이는 이 학습 프로젝트의 검증 규칙이며 모든 스키마 호환성의 일반 해법은 아닙니다.')],
}

# Commands run from the repository root in PowerShell. No installed host JDK needed.
LESSONS = [importlib.import_module(f'content.lesson_{n:02d}').LESSON for n in range(1, 13)]
LESSONS.sort(key=lambda item: item['id'])

try:
    from content.appendix import APPENDICES
except ImportError:  # 부록 작성 전에도 빌드할 수 있게 합니다.
    APPENDICES = []
