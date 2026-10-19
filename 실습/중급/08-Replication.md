# 08. Replication · ISR · KRaft

3개 브로커로 Leader 교체와 쓰기 가용성의 조건을 관찰합니다.

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

## 실행과 관찰

### 1. 3노드 환경 시작

```powershell
.\실습\study.ps1 up -Cluster
```

예상 결과: kafka1~3과 postgres가 healthy가 됩니다. PC 자원이 충분해야 합니다.

### 2. 클러스터 토픽 생성

```powershell
.\실습\study.ps1 init -Cluster
```

예상 결과: 복제 수 3, ISR 3인 토픽을 확인합니다.

### 3. Leader 조회

```powershell
.\실습\study.ps1 topics -Cluster
```

예상 결과: orders 파티션별 Leader ID를 기록합니다.

### 4. 한 노드 중단

```powershell
.\실습\study.ps1 stop kafka1
```

예상 결과: 예시로 kafka1을 중단합니다. 실제 Leader가 다른 노드라면 그 노드를 선택하세요.

### 5. 장애 중 전송

```powershell
.\실습\study.ps1 run -Cluster producer 6 orders failover-demo
```

예상 결과: 메타데이터 갱신 후 전송 성공 여부와 일시 재시도 로그를 관찰합니다.

### 6. 복구

```powershell
.\실습\study.ps1 start kafka1
```

예상 결과: 잠시 후 topics -Cluster로 ISR이 회복되는지 확인합니다.

## 더 실험하기

- single과 cluster는 서로 다른 로그 볼륨을 사용합니다. cluster의 그룹과 토픽은 별도로 시작합니다.
- 두 노드 중단은 controller 과반도 잃으므로 별도의 복구 실험으로 구분하세요. 반드시 중단한 노드를 모두 start로 복구합니다.

## 주의할 경계

초급 토픽이 들어 있는 single Kafka와 3노드 cluster는 별도 클러스터입니다. -Cluster를 빠뜨리면 다른 환경을 조회하거나 실행합니다. PostgreSQL은 공용이므로 DB 중복 표식은 환경 전환 후에도 남습니다.

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

이해도 확인: acks=all에서 all이 가리키는 대상은?

<details><summary>답 확인</summary>

현재 ISR에 속한 복제본의 확인 조건 — 현재 ISR과 min.insync.replicas를 함께 해석해야 합니다.

</details>
