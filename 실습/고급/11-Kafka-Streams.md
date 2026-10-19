# 11. Kafka Streams · 상태 저장

key별 누적 count를 만들고 상태 복구와 처리 보장을 연결합니다.

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

## 실행과 관찰

### 1. 60초 동안 집계

```powershell
.\실습\study.ps1 run streams 60 order-counts-day11
```

예상 결과: STREAMS 상태가 RUNNING으로 이동하고 지정 시간 후 종료합니다.

### 2. 다른 터미널에서 입력

```powershell
.\실습\study.ps1 run producer 12 orders day11
```

예상 결과: 주문별 이벤트가 입력됩니다. 이전 orders 기록도 같은 app의 진행 상태에 따라 집계합니다.

### 3. 결과 조회

```powershell
.\실습\study.ps1 run consumer count-reader-day11 0 orders.counts
```

예상 결과: key별 count 문자열이 출력됩니다. 중간값 전부가 출력될 필요는 없습니다.

### 4. 같은 application.id로 재시작

```powershell
.\실습\study.ps1 run streams 30 order-counts-day11
```

예상 결과: 기존 입력 offset과 상태에서 복구하여 이어 처리합니다.

## 더 실험하기

- 새 application.id로 실행한 결과와 기존 application.id로 재시작한 결과를 비교하세요.
- 같은 runId로 Producer를 두 번 실행해 EOS가 업무 eventId 중복 제거와 다른 이유를 확인하세요.

## 주의할 경계

동일 application.id를 서로 다른 Kafka 클러스터에 쓸 때 로컬 state 디렉터리를 공유하지 마세요. single/cluster를 오갈 때는 app ID에 single/cluster 접미사를 붙여 상태를 분리합니다.

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

이해도 확인: 이벤트 count를 만든 KTable은 무엇을 나타내나요?

<details><summary>답 확인</summary>

key별 최신 누적 집계 상태 — KTable은 key별 최신 상태를 나타냅니다. 입력 이벤트 로그와 같은 의미가 아닙니다.

</details>
